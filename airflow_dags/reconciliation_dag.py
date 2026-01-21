"""
Airflow DAG for Fraud Detection Batch Processing
Runs reconciliation and analytics every 6 hours
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import psycopg2
import os
import glob
import logging

logger = logging.getLogger(__name__)


# Database connection parameters
DB_CONFIG = {
    'host': 'postgres',
    'port': 5432,
    'database': 'fraud_detection',
    'user': 'fraud_admin',
    'password': 'fraud_secure_pass'
}

DATA_PATH = '/opt/airflow/data/validated'
REPORTS_PATH = '/opt/airflow/reports'


def get_db_connection():
    """Create PostgreSQL database connection"""
    return psycopg2.connect(**DB_CONFIG)


def read_validated_transactions(**context):
    """
    Read validated transactions from Parquet files
    This represents the batch layer processing
    """
    logger.info(f"Reading validated transactions from {DATA_PATH}")
    
    try:
        # Find all parquet files
        parquet_files = glob.glob(f"{DATA_PATH}/**/*.parquet", recursive=True)
        
        if not parquet_files:
            logger.warning("No parquet files found")
            context['task_instance'].xcom_push(key='transaction_count', value=0)
            context['task_instance'].xcom_push(key='total_amount', value=0.0)
            return
        
        # Read all parquet files into a single DataFrame
        dfs = [pd.read_parquet(file) for file in parquet_files]
        df = pd.concat(dfs, ignore_index=True)
        
        # Calculate metrics
        transaction_count = len(df)
        total_amount = df['amount'].sum()
        
        logger.info(f"Processed {transaction_count} validated transactions")
        logger.info(f"Total amount: ${total_amount:,.2f}")
        
        # Push to XCom for next task
        context['task_instance'].xcom_push(key='transaction_count', value=transaction_count)
        context['task_instance'].xcom_push(key='total_amount', value=float(total_amount))
        
    except Exception as e:
        logger.error(f"Error reading validated transactions: {e}")
        raise


def get_fraud_statistics(**context):
    """
    Query fraud transactions from PostgreSQL
    """
    logger.info("Querying fraud statistics from database")
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get fraud count and total amount
        query = """
            SELECT 
                COUNT(*) as fraud_count,
                COALESCE(SUM(amount), 0) as fraud_amount
            FROM fraud_transactions
            WHERE timestamp >= NOW() - INTERVAL '6 hours'
        """
        
        cursor.execute(query)
        result = cursor.fetchone()
        
        fraud_count = result[0] if result else 0
        fraud_amount = float(result[1]) if result and result[1] else 0.0
        
        logger.info(f"Found {fraud_count} fraud transactions")
        logger.info(f"Fraud amount: ${fraud_amount:,.2f}")
        
        # Push to XCom
        context['task_instance'].xcom_push(key='fraud_count', value=fraud_count)
        context['task_instance'].xcom_push(key='fraud_amount', value=fraud_amount)
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error querying fraud statistics: {e}")
        raise


def generate_reconciliation_report(**context):
    """
    Generate reconciliation report combining batch and streaming data
    """
    logger.info("Generating reconciliation report")
    
    try:
        # Pull data from XCom
        ti = context['task_instance']
        transaction_count = ti.xcom_pull(task_ids='read_validated_transactions', 
                                         key='transaction_count') or 0
        total_amount = ti.xcom_pull(task_ids='read_validated_transactions', 
                                    key='total_amount') or 0.0
        fraud_count = ti.xcom_pull(task_ids='get_fraud_statistics', 
                                   key='fraud_count') or 0
        fraud_amount = ti.xcom_pull(task_ids='get_fraud_statistics', 
                                    key='fraud_amount') or 0.0
        
        # Calculate metrics
        total_transactions = transaction_count + fraud_count
        total_transaction_amount = total_amount + fraud_amount
        fraud_percentage = (fraud_count / total_transactions * 100) if total_transactions > 0 else 0
        fraud_amount_percentage = (fraud_amount / total_transaction_amount * 100) if total_transaction_amount > 0 else 0
        
        # Create report
        report_data = {
            'report_timestamp': [datetime.now()],
            'total_transactions': [total_transactions],
            'validated_transactions': [transaction_count],
            'fraud_transactions': [fraud_count],
            'fraud_percentage': [round(fraud_percentage, 2)],
            'total_amount': [round(total_amount, 2)],
            'fraud_amount': [round(fraud_amount, 2)],
            'fraud_amount_percentage': [round(fraud_amount_percentage, 2)]
        }
        
        df = pd.DataFrame(report_data)
        
        # Save to CSV
        os.makedirs(REPORTS_PATH, exist_ok=True)
        report_filename = f"{REPORTS_PATH}/reconciliation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(report_filename, index=False)
        
        logger.info(f"Reconciliation report saved to {report_filename}")
        logger.info(f"Total Transactions: {total_transactions}")
        logger.info(f"Fraud Rate: {fraud_percentage:.2f}%")
        
        # Store report in database
        conn = get_db_connection()
        cursor = conn.cursor()
        
        insert_query = """
            INSERT INTO reconciliation_reports 
            (report_date, total_transactions, total_amount, fraud_count, fraud_amount)
            VALUES (%s, %s, %s, %s, %s)
        """
        
        cursor.execute(insert_query, (
            datetime.now(),
            total_transactions,
            total_transaction_amount,
            fraud_count,
            fraud_amount
        ))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info("Reconciliation data stored in database")
        
    except Exception as e:
        logger.error(f"Error generating reconciliation report: {e}")
        raise


def analyze_fraud_patterns(**context):
    """
    Analyze fraud patterns by merchant category
    """
    logger.info("Analyzing fraud patterns")
    
    try:
        conn = get_db_connection()
        
        # Query fraud by merchant category
        query = """
            SELECT 
                merchant_category,
                COUNT(*) as fraud_count,
                SUM(amount) as total_fraud_amount,
                AVG(amount) as avg_fraud_amount,
                fraud_reason
            FROM fraud_transactions
            WHERE timestamp >= NOW() - INTERVAL '6 hours'
            GROUP BY merchant_category, fraud_reason
            ORDER BY fraud_count DESC
        """
        
        df = pd.read_sql_query(query, conn)
        
        if not df.empty:
            # Save analysis
            analysis_filename = f"{REPORTS_PATH}/fraud_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            df.to_csv(analysis_filename, index=False)
            
            logger.info(f"Fraud analysis saved to {analysis_filename}")
            logger.info(f"Top fraud category: {df.iloc[0]['merchant_category']}")
        else:
            logger.info("No fraud transactions to analyze in this period")
        
        conn.close()
        
    except Exception as e:
        logger.error(f"Error analyzing fraud patterns: {e}")
        raise


# Define default arguments
default_args = {
    'owner': 'fraud_detection_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}


# Define the DAG
dag = DAG(
    'fraud_detection_reconciliation',
    default_args=default_args,
    description='Reconciliation and analytics for fraud detection pipeline',
    schedule_interval='0 */6 * * *',  # Run every 6 hours
    catchup=False,
    tags=['fraud-detection', 'reconciliation', 'batch']
)


# Define tasks
task_read_validated = PythonOperator(
    task_id='read_validated_transactions',
    python_callable=read_validated_transactions,
    provide_context=True,
    dag=dag
)

task_get_fraud_stats = PythonOperator(
    task_id='get_fraud_statistics',
    python_callable=get_fraud_statistics,
    provide_context=True,
    dag=dag
)

task_reconciliation = PythonOperator(
    task_id='generate_reconciliation_report',
    python_callable=generate_reconciliation_report,
    provide_context=True,
    dag=dag
)

task_fraud_analysis = PythonOperator(
    task_id='analyze_fraud_patterns',
    python_callable=analyze_fraud_patterns,
    provide_context=True,
    dag=dag
)


# Define task dependencies
[task_read_validated, task_get_fraud_stats] >> task_reconciliation >> task_fraud_analysis
