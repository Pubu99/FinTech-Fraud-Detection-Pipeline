# FinTech Fraud Detection Pipeline

A production-ready Lambda architecture implementation for real-time credit card fraud detection using Apache Kafka, Spark Structured Streaming, and Apache Airflow.

## Architecture Overview

This project implements an end-to-end data pipeline that demonstrates:

- **Speed Layer**: Real-time fraud detection using Spark Structured Streaming
- **Batch Layer**: Periodic reconciliation and analytics using Apache Airflow
- **Event-time processing** with watermarking to handle late-arriving data
- **Dual-sink architecture**: Fraud events to PostgreSQL, validated transactions to Parquet

### System Architecture

```
Transaction Generator (Python)
        |
        v
   Kafka Topic (3 partitions)
        |
        v
Spark Structured Streaming
 (Event-time + Watermarking)
        |
        +-- Fraud Detection Logic
        |
   +----+----+
   |         |
   v         v
Fraud DB   Parquet
(PostgreSQL) (Validated Tx)
   |         |
   +----+----+
        |
        v
  Airflow (Every 6h)
        |
        v
Reconciliation Reports
```

## Technology Stack

| Component         | Technology                     | Purpose                         |
| ----------------- | ------------------------------ | ------------------------------- |
| Message Queue     | Apache Kafka 3.5               | Real-time event streaming       |
| Stream Processing | Spark Structured Streaming 3.5 | Fraud detection with event-time |
| Orchestration     | Apache Airflow 2.7             | Batch ETL and reconciliation    |
| Database          | PostgreSQL 14                  | Fraud transaction storage       |
| Batch Storage     | Parquet Files                  | Historical transaction storage  |
| Analytics         | Pandas + Matplotlib            | Report generation               |
| Containerization  | Docker Compose                 | Environment management          |
| Language          | Python 3.10                    | End-to-end implementation       |

## Features

### Real-Time Fraud Detection

1. **High-Value Fraud**: Transactions exceeding $5,000
2. **Impossible Travel**: Same user in different countries within 10 minutes

### Event-Time Processing

- Uses transaction timestamp (not processing time)
- 15-minute watermark for late event tolerance
- Window-based aggregations for impossible travel detection

### Batch Reconciliation

- Runs every 6 hours via Airflow
- Compares streaming and batch totals
- Generates fraud pattern analytics

## Project Structure

```
FinTech-Fraud-Detection-Pipeline/
├── producers/
│   └── transaction_producer.py       # Kafka producer with fraud injection
├── spark_jobs/
│   └── fraud_detection_stream.py     # Streaming fraud detection
├── airflow_dags/
│   └── reconciliation_dag.py         # Batch reconciliation DAG
├── reports/
│   └── fraud_analytics.py            # Analytics report generator
├── config/
│   └── init.sql                      # PostgreSQL initialization
├── data/                             # Parquet storage directory
├── docker-compose.yml                # Service orchestration
├── requirements.txt                  # Python dependencies
├── start_services.bat/.sh            # Service startup scripts
├── run_producer.bat/.sh              # Producer execution scripts
└── run_spark_job.bat/.sh             # Spark job submission scripts
```

## Prerequisites

- Docker Desktop (Windows/Mac) or Docker Engine (Linux)
- Docker Compose
- Python 3.10+
- 8GB RAM minimum
- 10GB free disk space

## Quick Start

### 1. Start All Services

**Windows:**

```bash
start_services.bat
```

**Linux/Mac:**

```bash
chmod +x start_services.sh
./start_services.sh
```

This will start:

- Zookeeper and Kafka
- Spark Master and Worker
- PostgreSQL
- Airflow Webserver and Scheduler

### 2. Verify Services

Access the following URLs:

- **Airflow UI**: http://localhost:8081 (use the Airflow admin credentials you configured)
- **Spark Master**: http://localhost:8080
- **PostgreSQL**: localhost:5432 (use the PostgreSQL credentials you configured)

### 3. Run Transaction Producer

**Windows:**

```bash
run_producer.bat
```

**Linux/Mac:**

```bash
chmod +x run_producer.sh
./run_producer.sh
```

This generates 500 transactions with 15% fraud rate.

### 4. Submit Spark Streaming Job

**Windows:**

```bash
run_spark_job.bat
```

**Linux/Mac:**

```bash
chmod +x run_spark_job.sh
./run_spark_job.sh
```

### 5. Enable Airflow DAG

1. Open Airflow UI at http://localhost:8081
2. Login with your configured Airflow admin credentials
3. Find `fraud_detection_reconciliation` DAG
4. Toggle it to "ON"
5. Trigger manually or wait for scheduled run

### 6. Generate Analytics Report

```bash
python reports/fraud_analytics.py --hours 24
```

Reports are saved to the `reports/` directory.

## Detailed Usage

### Transaction Producer Options

```bash
python producers/transaction_producer.py \
    --bootstrap-servers localhost:9092 \
    --topic transactions \
    --num-transactions 1000 \
    --fraud-ratio 0.15 \
    --delay 1.0
```

**Parameters:**

- `--bootstrap-servers`: Kafka broker address
- `--topic`: Kafka topic name
- `--num-transactions`: Total transactions to generate
- `--fraud-ratio`: Percentage of fraudulent transactions (0-1)
- `--delay`: Delay between transactions in seconds

### Fraud Detection Patterns

**High-Value Fraud:**

- Amount > $5,000
- Logged with reason: `HIGH_VALUE_TRANSACTION`

**Impossible Travel:**

- Same user ID
- Different countries
- Time difference <= 10 minutes
- Logged with reason: `IMPOSSIBLE_TRAVEL`

### Checking Fraud Transactions

Connect to PostgreSQL:

```bash
docker exec -it postgres psql -U <POSTGRES_USER> -d <POSTGRES_DB>
```

Query fraud data:

```sql
SELECT user_id, timestamp, amount, location, fraud_reason
FROM fraud_transactions
ORDER BY timestamp DESC
LIMIT 10;
```

### Viewing Validated Transactions

Parquet files are stored in `data/validated/`:

```python
import pandas as pd
df = pd.read_parquet('data/validated/')
print(df.head())
```

## Architecture Decisions

### Why Lambda Architecture?

- **Speed Layer**: Handles real-time fraud detection with low latency
- **Batch Layer**: Ensures data consistency and historical analytics
- **Complements**: Stream processing for speed, batch for accuracy

### Event Time vs Processing Time

**Event Time**: Transaction timestamp from producer (used in this project)

- Handles out-of-order events correctly
- Enables accurate time-based fraud detection
- Watermarking compensates for network delays

**Processing Time**: Time when Spark processes the event

- Can miss fraud patterns if events arrive late
- Not suitable for financial compliance

### Watermarking Strategy

- **15-minute watermark**: Tolerates reasonable network delays
- Events arriving >15 minutes late are dropped
- Balance between accuracy and memory usage

## Monitoring and Troubleshooting

### Check Service Status

```bash
docker-compose ps
```

### View Logs

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f kafka
docker-compose logs -f spark-master
docker-compose logs -f airflow-webserver
```

### Restart Services

```bash
docker-compose restart [service_name]
```

### Stop All Services

```bash
docker-compose down
```

### Clean All Data

```bash
docker-compose down -v
rm -rf data/*
```

## Ethical Considerations

### Data Privacy

- Uses anonymized `user_id` (no PII)
- Fraud database access is password-protected
- No personally identifiable information stored

### False Positive Impact

- High-value threshold ($5,000) minimizes false positives
- Impossible travel requires geographic evidence
- Manual review recommended for production deployment

### Compliance Alignment

- Architecture supports PCI-DSS requirements
- GDPR-compliant data minimization
- Audit trail via PostgreSQL timestamps

## Performance Optimization

### Kafka Configuration

- 3 partitions for parallelism
- Replication factor: 1 (single-node setup)

### Spark Configuration

- 2GB driver memory
- 2GB executor memory
- Adaptive query execution enabled

### Database Indexing

```sql
CREATE INDEX idx_fraud_user_id ON fraud_transactions(user_id);
CREATE INDEX idx_fraud_timestamp ON fraud_transactions(timestamp);
```

## Testing

### Unit Test Producer

```bash
python -m pytest tests/test_producer.py
```

### Validate Kafka Topic

```bash
docker exec kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic transactions \
    --from-beginning \
    --max-messages 10
```

### Test Spark Locally

```bash
spark-submit \
    --master local[*] \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    spark_jobs/fraud_detection_stream.py
```

## Common Issues

### Kafka Connection Refused

**Solution**: Wait 30 seconds after `docker-compose up` for Kafka initialization.

### Spark Job Fails

**Solution**: Check memory allocation. Reduce executor memory if system has <8GB RAM.

### Airflow DAG Not Appearing

**Solution**:

1. Check DAG syntax: `python airflow_dags/reconciliation_dag.py`
2. Restart scheduler: `docker-compose restart airflow-scheduler`

### No Parquet Files Generated

**Solution**: Ensure Spark job is running and producer has sent transactions.

## Development Roadmap

- [ ] Add machine learning fraud model
- [ ] Implement real-time dashboard
- [ ] Add anomaly detection algorithms
- [ ] Integrate with cloud services (AWS/Azure)
- [ ] Add authentication and authorization
- [ ] Implement CDC for database changes

## Contributing

This is an academic project for Big Data Engineering coursework. External contributions are not currently accepted.

## License

This project is created for educational purposes as part of a university mini-project.

## Authors

- Student Name
- University: FOE-UOR
- Course: SEM 8 - Big Data Analysis
- Academic Year: 2025-2026

## Acknowledgments

- Apache Software Foundation for open-source tools
- Docker community for containerization support
- Academic supervisor for project guidance

## References

1. Lambda Architecture - Nathan Marz
2. Apache Kafka Documentation
3. Spark Structured Streaming Guide
4. Airflow Best Practices
5. Financial Fraud Detection Patterns
