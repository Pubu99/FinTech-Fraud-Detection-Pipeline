"""
Simple test script to verify basic functionality
"""

import sys
import time

def test_kafka_connection():
    """Test Kafka connectivity"""
    print("Testing Kafka connection...")
    try:
        from kafka import KafkaProducer
        from kafka.errors import NoBrokersAvailable
        
        producer = KafkaProducer(bootstrap_servers='localhost:9092')
        producer.close()
        print("  [PASS] Kafka connection successful")
        return True
    except NoBrokersAvailable:
        print("  [FAIL] Kafka not available. Is it running?")
        return False
    except Exception as e:
        print(f"  [FAIL] Kafka test failed: {e}")
        return False


def test_postgres_connection():
    """Test PostgreSQL connectivity"""
    print("Testing PostgreSQL connection...")
    try:
        import psycopg2
        
        conn = psycopg2.connect(
            host='localhost',
            port=5432,
            database='fraud_detection',
            user='fraud_admin',
            password='fraud_secure_pass'
        )
        conn.close()
        print("  [PASS] PostgreSQL connection successful")
        return True
    except Exception as e:
        print(f"  [FAIL] PostgreSQL test failed: {e}")
        return False


def test_data_directories():
    """Test data directory structure"""
    print("Testing data directories...")
    import os
    
    required_dirs = [
        'producers',
        'spark_jobs',
        'airflow_dags',
        'data',
        'reports',
        'config'
    ]
    
    all_exist = True
    for dir_name in required_dirs:
        if os.path.exists(dir_name):
            print(f"  [PASS] {dir_name}/ exists")
        else:
            print(f"  [FAIL] {dir_name}/ not found")
            all_exist = False
    
    return all_exist


def test_required_files():
    """Test required files exist"""
    print("Testing required files...")
    import os
    
    required_files = [
        'producers/transaction_producer.py',
        'spark_jobs/fraud_detection_stream.py',
        'airflow_dags/reconciliation_dag.py',
        'reports/fraud_analytics.py',
        'config/init.sql',
        'docker-compose.yml',
        'requirements.txt'
    ]
    
    all_exist = True
    for file_name in required_files:
        if os.path.exists(file_name):
            print(f"  [PASS] {file_name} exists")
        else:
            print(f"  [FAIL] {file_name} not found")
            all_exist = False
    
    return all_exist


def test_python_dependencies():
    """Test Python dependencies are available"""
    print("Testing Python dependencies...")
    
    dependencies = [
        ('kafka', 'kafka-python'),
        ('pyspark', 'pyspark'),
        ('pandas', 'pandas'),
        ('psycopg2', 'psycopg2-binary'),
        ('matplotlib', 'matplotlib')
    ]
    
    all_installed = True
    for module, package in dependencies:
        try:
            __import__(module)
            print(f"  [PASS] {package} installed")
        except ImportError:
            print(f"  [FAIL] {package} not installed")
            all_installed = False
    
    return all_installed


def test_docker_services():
    """Test Docker services are running"""
    print("Testing Docker services...")
    import subprocess
    
    try:
        result = subprocess.run(
            ['docker-compose', 'ps', '--services', '--filter', 'status=running'],
            capture_output=True,
            text=True
        )
        
        running_services = result.stdout.strip().split('\n')
        expected_services = [
            'zookeeper', 'kafka', 'spark-master', 'spark-worker',
            'postgres', 'airflow-webserver', 'airflow-scheduler'
        ]
        
        all_running = True
        for service in expected_services:
            if service in running_services:
                print(f"  [PASS] {service} is running")
            else:
                print(f"  [FAIL] {service} is not running")
                all_running = False
        
        return all_running
    except Exception as e:
        print(f"  [FAIL] Docker test failed: {e}")
        return False


def main():
    """Run all tests"""
    print("=" * 60)
    print("FinTech Fraud Detection Pipeline - System Test")
    print("=" * 60)
    print()
    
    results = []
    
    # Test directory structure
    results.append(("Directory Structure", test_data_directories()))
    print()
    
    # Test required files
    results.append(("Required Files", test_required_files()))
    print()
    
    # Test Python dependencies
    results.append(("Python Dependencies", test_python_dependencies()))
    print()
    
    # Test Docker services (optional)
    print("Note: The following tests require Docker services to be running")
    print("Run 'docker-compose up -d' first if not already running")
    print()
    
    results.append(("Docker Services", test_docker_services()))
    print()
    
    results.append(("Kafka Connection", test_kafka_connection()))
    print()
    
    results.append(("PostgreSQL Connection", test_postgres_connection()))
    print()
    
    # Summary
    print("=" * 60)
    print("Test Summary")
    print("=" * 60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "PASS" if result else "FAIL"
        print(f"{test_name}: [{status}]")
    
    print()
    print(f"Total: {passed}/{total} tests passed")
    
    if passed == total:
        print()
        print("All tests passed! System is ready.")
        sys.exit(0)
    else:
        print()
        print("Some tests failed. Please check the output above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
