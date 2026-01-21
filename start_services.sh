#!/bin/bash

echo "Starting FinTech Fraud Detection Pipeline..."

# Start Docker Compose services
echo "Starting Docker services..."
docker-compose up -d

# Wait for services to be ready
echo "Waiting for services to initialize..."
sleep 30

# Check Kafka availability
echo "Checking Kafka availability..."
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Create Kafka topic if not exists
echo "Creating Kafka topic 'transactions'..."
docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
    --create --if-not-exists \
    --topic transactions \
    --partitions 3 \
    --replication-factor 1

echo "All services are ready!"
echo ""
echo "Service URLs:"
echo "  - Airflow Web UI: http://localhost:8081 (admin/admin)"
echo "  - Spark Master UI: http://localhost:8080"
echo "  - PostgreSQL: localhost:5432 (fraud_admin/fraud_secure_pass)"
echo ""
echo "To check logs:"
echo "  docker-compose logs -f [service_name]"
