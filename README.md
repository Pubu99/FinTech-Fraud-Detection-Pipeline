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

### Setup Guide (Windows PowerShell)

Open **3 PowerShell terminals** in the project folder:

- **Terminal A**: Docker services (one-time setup)
- **Terminal B**: Spark streaming job (long-running)
- **Terminal C**: Producer + verification commands

> **Note:** This project runs **Airflow inside Docker**. You do **not** need to install Apache Airflow locally on Windows.

---

### Step 1: Create Virtual Environment (Terminal A)

```powershell
# Create virtual environment
python -m venv venv

# Activate virtual environment
.\venv\Scripts\Activate.ps1

# Upgrade pip
pip install --upgrade pip

# Install all Python dependencies
pip install -r requirements.txt
```

> **Important:** You should see `(venv)` prefix in your terminal after activation. Always activate the venv before running Python scripts.

---

### Step 2: Start Docker Services (Terminal A)

**Make sure Docker Desktop is running first!**

```powershell
# Start all Docker containers
docker-compose up -d

# Verify containers are running
docker-compose ps

# Wait 30 seconds for services to initialize
timeout /t 30 /nobreak
```

Expected containers:

- `zookeeper` - Kafka coordinator
- `kafka` - Message broker
- `spark-master` - Spark cluster master
- `spark-worker` - Spark cluster worker
- `postgres` - Database for fraud transactions
- `airflow-webserver` - Airflow UI
- `airflow-scheduler` - Airflow task scheduler

---

### Step 3: Create Kafka Topic (Terminal A)

```powershell
# Create the transactions topic with 3 partitions
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic transactions --partitions 3 --replication-factor 1

# Verify topic creation
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

---

### Step 4: Verify Services

Access the following URLs in your browser:

- **Airflow UI**: http://localhost:8081 (login: `admin` / `admin`)
- **Spark Master UI**: http://localhost:8080
- **PostgreSQL**: localhost:5432 (user: `fraud_admin` / password: `fraud_secure_pass`)

---

### Step 5: Submit Spark Streaming Job (Terminal B)

**Open a NEW terminal (Terminal B) - this job runs continuously:**

```powershell
# Submit Spark job to process streaming data
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 `
  --conf spark.jars.ivy=/tmp/ivy `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0 `
  --driver-memory 2g --executor-memory 2g `
  /opt/spark_jobs/fraud_detection_stream.py --kafka-servers kafka:29092 --kafka-topic transactions --output-path /opt/data
```

> **Note:** Keep this terminal running! The Spark job will continuously process transactions from Kafka. Monitor it at http://localhost:8080

---

### Step 6: Run Transaction Producer (Terminal C)

**Open a NEW terminal (Terminal C) and activate venv:**

```powershell
# Activate virtual environment
.\venv\Scripts\Activate.ps1

# Generate 500 transactions with 15% fraud rate
python producers/transaction_producer.py --bootstrap-servers localhost:9092 --topic transactions --num-transactions 500 --fraud-ratio 0.15 --delay 0.5
```

This generates transactions and sends them to Kafka. Spark will process them in real-time.

---

### Step 7: Verify Fraud Detection (Terminal C)

```powershell
# Check fraud transactions in PostgreSQL
docker exec -it postgres psql -U fraud_admin -d fraud_detection -c "SELECT user_id, timestamp, amount, location, fraud_reason, detected_at FROM fraud_transactions ORDER BY detected_at DESC LIMIT 10;"
```

---

### Step 8: Enable Airflow DAG (Optional)

1. Open Airflow UI at http://localhost:8081
2. Login with `admin` / `admin`
3. Find `fraud_detection_reconciliation` DAG
4. Toggle it to **ON**
5. Trigger manually or wait for scheduled run (every 6 hours)

---

### Step 9: Generate Analytics Report (Terminal C)

```powershell
# Make sure venv is activated
python reports/fraud_analytics.py --hours 24
```

Reports are saved to the `reports/` directory.

---

## Alternative: Using Batch Scripts (Quick Method)

If you prefer automation, use the provided batch scripts:

**Windows:**

```powershell
.\start_services.bat    # Start all Docker services
.\run_spark_job.bat     # Submit Spark job
.\run_producer.bat      # Run transaction producer
```

**Linux/Mac:**

```bash
chmod +x *.sh
./start_services.sh     # Start all Docker services
./run_spark_job.sh      # Submit Spark job
./run_producer.sh       # Run transaction producer
```

## Manual Command Reference

### Start/Stop Docker Stack

```powershell
# Start all containers
docker-compose up -d

# View running containers
docker-compose ps

# Stop all containers
docker-compose down

# Stop and remove volumes (clean slate)
docker-compose down -v

# View logs for all services
docker-compose logs -f

# View logs for specific service
docker-compose logs -f kafka
docker-compose logs -f spark-master
docker-compose logs -f postgres
```

### Kafka Operations

```powershell
# List all topics
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Create transactions topic
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic transactions --partitions 3 --replication-factor 1

# Describe topic details
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic transactions

# Delete topic (if needed)
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic transactions

# Consume messages from topic (for debugging)
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic transactions --from-beginning
```

### Spark Job Submission

```powershell
# Submit streaming job (long-running)
docker exec spark-master /opt/spark/bin/spark-submit `
    --master spark://spark-master:7077 `
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0 `
    --driver-memory 2g --executor-memory 2g `
    /opt/spark_jobs/fraud_detection_stream.py `
    --kafka-servers kafka:29092 --kafka-topic transactions --output-path /opt/data

# View Spark Master UI
# Open: http://localhost:8080
```

### Run Transaction Producer

```powershell
# Activate venv first
.\venv\Scripts\Activate.ps1

# Basic run (500 transactions, 15% fraud rate)
python producers/transaction_producer.py --bootstrap-servers localhost:9092 --topic transactions --num-transactions 500 --fraud-ratio 0.15 --delay 0.5

# High volume test (1000 transactions, faster delay)
python producers/transaction_producer.py --bootstrap-servers localhost:9092 --topic transactions --num-transactions 1000 --fraud-ratio 0.20 --delay 0.2

# Low volume test (100 transactions, slower)
python producers/transaction_producer.py --bootstrap-servers localhost:9092 --topic transactions --num-transactions 100 --fraud-ratio 0.10 --delay 1.0
```

### PostgreSQL Queries

```powershell
# Connect to PostgreSQL
docker exec -it postgres psql -U fraud_admin -d fraud_detection

# Or run queries directly:

# View recent fraud transactions
docker exec -it postgres psql -U fraud_admin -d fraud_detection -c "SELECT user_id, timestamp, amount, location, fraud_reason, detected_at FROM fraud_transactions ORDER BY detected_at DESC LIMIT 10;"

# Count total fraud transactions
docker exec -it postgres psql -U fraud_admin -d fraud_detection -c "SELECT COUNT(*) FROM fraud_transactions;"

# Count by fraud reason
docker exec -it postgres psql -U fraud_admin -d fraud_detection -c "SELECT fraud_reason, COUNT(*) FROM fraud_transactions GROUP BY fraud_reason;"

# High-value frauds only
docker exec -it postgres psql -U fraud_admin -d fraud_detection -c "SELECT * FROM fraud_transactions WHERE fraud_reason = 'HIGH_VALUE_TRANSACTION' ORDER BY amount DESC LIMIT 10;"

# Impossible travel frauds only
docker exec -it postgres psql -U fraud_admin -d fraud_detection -c "SELECT * FROM fraud_transactions WHERE fraud_reason = 'IMPOSSIBLE_TRAVEL' ORDER BY detected_at DESC LIMIT 10;"
```

### Airflow Operations

```powershell
# Trigger DAG from CLI
docker exec airflow-webserver airflow dags trigger fraud_detection_reconciliation

# List all DAGs
docker exec airflow-webserver airflow dags list

# Check DAG status
docker exec airflow-webserver airflow dags state fraud_detection_reconciliation

# View Airflow UI
# Open: http://localhost:8081
# Login: admin / admin
```

### Generate Reports

```powershell
# Activate venv first
.\venv\Scripts\Activate.ps1

# Generate 24-hour report
python reports/fraud_analytics.py --hours 24

# Generate 7-day report
python reports/fraud_analytics.py --hours 168

# Report will be saved in reports/ directory
```

### Troubleshooting Commands

```powershell
# Check if Docker is running
docker --version
docker-compose --version

# Restart specific container
docker-compose restart kafka
docker-compose restart spark-master
docker-compose restart postgres

# View container resource usage
docker stats

# Check disk space used by Docker
docker system df

# Clean up unused Docker resources
docker system prune -a

# Check Python version
python --version

# Check installed packages
pip list

# Reinstall requirements
pip install -r requirements.txt --force-reinstall
```

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
docker compose ps
```

### View Logs

```bash
# All services
docker compose logs -f

# Specific service
docker compose logs -f kafka
docker compose logs -f spark-master
docker compose logs -f airflow-webserver
```

### Restart Services

```bash
docker compose restart [service_name]
```

### Stop All Services

```bash
docker compose down
```

### Clean All Data

```bash
docker compose down -v
```

**Windows (PowerShell):**

```powershell
Remove-Item -Recurse -Force .\data\* -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force .\reports\*.csv -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force .\reports\*.png -ErrorAction SilentlyContinue
```

**Linux/Mac:**

```bash
rm -rf data/*
rm -rf reports/*.csv reports/*.png
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

### System Test (recommended)

This repo includes a simple end-to-end sanity check:

```bash
python test_system.py
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
