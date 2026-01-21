#!/bin/bash

echo "Submitting Spark Fraud Detection Job..."

# Submit Spark job to master
docker exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0 \
    --driver-memory 2g \
    --executor-memory 2g \
    /opt/spark_jobs/fraud_detection_stream.py \
    --kafka-servers kafka:29092 \
    --kafka-topic transactions \
    --output-path /opt/data

echo "Spark job submitted!"
echo "Monitor at: http://localhost:8080"
