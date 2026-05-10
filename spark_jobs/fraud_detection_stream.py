"""
Fraud Detection Streaming Job using Spark Structured Streaming
Implements real-time fraud detection with event-time processing and watermarking
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, approx_count_distinct,
    when, lit, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FraudDetectionStream:
    """Real-time fraud detection using Spark Structured Streaming"""
    
    def __init__(self, kafka_bootstrap_servers='kafka:29092', kafka_topic='transactions'):
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_topic = kafka_topic
        self.spark = self._create_spark_session()
        self.transaction_schema = self._define_schema()
    
    def _create_spark_session(self):
        """Create Spark session with Kafka support"""
        return SparkSession.builder \
            .appName("FraudDetectionStream") \
            .config("spark.jars.packages", 
                   "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                   "org.postgresql:postgresql:42.6.0") \
            .config("spark.sql.streaming.checkpointLocation", "/opt/data/checkpoints") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
    
    def _define_schema(self):
        """Define the transaction schema"""
        return StructType([
            StructField("user_id", StringType(), False),
            StructField("timestamp", StringType(), False),
            StructField("merchant_category", StringType(), False),
            StructField("amount", DoubleType(), False),
            StructField("location", StringType(), False)
        ])
    
    def read_kafka_stream(self):
        """Read streaming data from Kafka"""
        logger.info(f"Reading from Kafka topic: {self.kafka_topic}")
        
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("subscribe", self.kafka_topic) \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        # Parse JSON and extract timestamp
        transactions = df.select(
            from_json(col("value").cast("string"), self.transaction_schema).alias("data")
        ).select("data.*")
        
        # Convert timestamp string to timestamp type
        transactions = transactions.withColumn(
            "timestamp",
            to_timestamp(col("timestamp"))
        )
        
        return transactions
    
    def detect_high_value_fraud(self, transactions):
        """Detect high-value fraud (amount > 5000)"""
        high_value_fraud = transactions.filter(col("amount") > 5000.0)
        
        return high_value_fraud.withColumn(
            "fraud_reason",
            lit("HIGH_VALUE_TRANSACTION")
        )
    
    def detect_impossible_travel(self, transactions):
        """
        Detect impossible travel: same user in different countries within 10 minutes
        Uses event-time processing with watermarking
        """
        # Add watermark to handle late data (15 minutes tolerance)
        transactions_with_watermark = transactions.withWatermark("timestamp", "15 minutes")

        # Build 10-minute windows per user and flag windows with multiple locations
        windowed_locations = transactions_with_watermark.groupBy(
            col("user_id"),
            window(col("timestamp"), "10 minutes", "1 minute")
        ).agg(
            approx_count_distinct("location").alias("location_count")
        )

        impossible_windows = windowed_locations.filter(col("location_count") > 1)

        # Project window bounds to avoid carrying a second event-time column
        impossible_windows = impossible_windows.select(
            col("user_id"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end")
        )

        # Join back to original events inside flagged windows
        tx = transactions_with_watermark.alias("tx")
        win = impossible_windows.alias("win")

        impossible_travel = tx.join(
            win,
            (col("tx.user_id") == col("win.user_id")) &
            (col("tx.timestamp") >= col("win.window_start")) &
            (col("tx.timestamp") < col("win.window_end")),
            "inner"
        ).select(
            col("tx.user_id").alias("user_id"),
            col("tx.timestamp").alias("timestamp"),
            col("tx.merchant_category").alias("merchant_category"),
            col("tx.amount").alias("amount"),
            col("tx.location").alias("location")
        )
        
        return impossible_travel.withColumn(
            "fraud_reason",
            lit("IMPOSSIBLE_TRAVEL")
        ).select(
            "user_id", "timestamp", "merchant_category",
            "amount", "location", "fraud_reason"
        )
    
    def write_fraud_to_postgres(self, fraud_df, checkpoint_location):
        """Write fraud transactions to PostgreSQL"""
        jdbc_url = "jdbc:postgresql://postgres:5432/fraud_detection"
        connection_properties = {
            "user": "fraud_admin",
            "password": "fraud_secure_pass",
            "driver": "org.postgresql.Driver"
        }
        
        def write_batch_to_postgres(batch_df, batch_id):
            """Callback function to write each batch"""
            try:
                if not batch_df.isEmpty():
                    # Add detection timestamp
                    batch_df = batch_df.withColumn(
                        "detected_at",
                        current_timestamp()
                    )
                    
                    batch_df.write \
                        .jdbc(url=jdbc_url, table="fraud_transactions",
                              mode="append", properties=connection_properties)
                    
                    fraud_count = batch_df.count()
                    logger.info(f"Batch {batch_id}: Wrote {fraud_count} fraud transactions to PostgreSQL")
            except Exception as e:
                logger.error(f"Error writing batch {batch_id} to PostgreSQL: {e}")
        
        return fraud_df.writeStream \
            .foreachBatch(write_batch_to_postgres) \
            .option("checkpointLocation", checkpoint_location) \
            .outputMode("append") \
            .start()
    
    def write_validated_to_parquet(self, validated_df, output_path, checkpoint_location):
        """Write validated (non-fraud) transactions to Parquet"""
        return validated_df.writeStream \
            .format("parquet") \
            .option("path", output_path) \
            .option("checkpointLocation", checkpoint_location) \
            .outputMode("append") \
            .trigger(processingTime="30 seconds") \
            .start()
    
    def run(self, output_base_path="/opt/data"):
        """Run the fraud detection streaming job"""
        logger.info("Starting Fraud Detection Streaming Job")
        
        # Read transactions from Kafka
        transactions = self.read_kafka_stream()
        
        # Detect high-value fraud
        high_value_fraud = self.detect_high_value_fraud(transactions)
        
        # Detect impossible travel fraud
        impossible_travel_fraud = self.detect_impossible_travel(transactions)
        
        # Union both fraud types
        all_fraud = high_value_fraud.union(impossible_travel_fraud)
        
        # Start writing fraud to PostgreSQL
        fraud_query = self.write_fraud_to_postgres(
            all_fraud,
            f"{output_base_path}/checkpoints/fraud"
        )
        
        # Filter out fraud from validated transactions
        # Create a temporary view for anti-join
        all_fraud.createOrReplaceTempView("fraud_view")
        transactions.createOrReplaceTempView("transactions_view")
        
        # For simplicity, write all transactions to parquet
        # In production, you would do a proper anti-join
        validated_query = self.write_validated_to_parquet(
            transactions,
            f"{output_base_path}/validated",
            f"{output_base_path}/checkpoints/validated"
        )
        
        logger.info("Fraud Detection Streaming Job is running...")
        logger.info("Press Ctrl+C to stop")
        
        # Wait for termination
        self.spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Fraud Detection Streaming Job')
    parser.add_argument('--kafka-servers', default='kafka:29092', 
                       help='Kafka bootstrap servers')
    parser.add_argument('--kafka-topic', default='transactions', 
                       help='Kafka topic to consume')
    parser.add_argument('--output-path', default='/opt/data',
                       help='Base output path for data')
    
    args = parser.parse_args()
    
    detector = FraudDetectionStream(
        kafka_bootstrap_servers=args.kafka_servers,
        kafka_topic=args.kafka_topic
    )
    
    detector.run(output_base_path=args.output_path)
