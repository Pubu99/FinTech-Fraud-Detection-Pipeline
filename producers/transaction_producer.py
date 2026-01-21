"""
Transaction Producer for FinTech Fraud Detection Pipeline
Generates synthetic credit card transactions with controlled fraud injection
"""

import json
import random
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TransactionGenerator:
    """Generates synthetic transaction data with fraud patterns"""
    
    MERCHANT_CATEGORIES = [
        'GROCERY', 'RESTAURANT', 'GAS_STATION', 'ONLINE_RETAIL',
        'ELECTRONICS', 'TRAVEL', 'ENTERTAINMENT', 'HEALTHCARE',
        'UTILITIES', 'INSURANCE'
    ]
    
    COUNTRIES = [
        'US', 'UK', 'CA', 'DE', 'FR', 'JP', 'AU', 'IN', 'BR', 'MX'
    ]
    
    def __init__(self, num_users=100):
        self.num_users = num_users
        self.user_ids = [f"USER_{str(i).zfill(5)}" for i in range(1, num_users + 1)]
        self.user_last_transaction = {}
    
    def generate_normal_transaction(self):
        """Generate a normal transaction"""
        user_id = random.choice(self.user_ids)
        timestamp = datetime.now().isoformat()
        merchant_category = random.choice(self.MERCHANT_CATEGORIES)
        
        # Normal transaction amounts (realistic distribution)
        amount = round(random.lognormvariate(3.5, 1.2), 2)
        amount = min(amount, 3000.0)  # Cap at 3000 for normal transactions
        
        location = random.choice(self.COUNTRIES)
        
        # Track user's last transaction for fraud detection
        self.user_last_transaction[user_id] = {
            'timestamp': datetime.fromisoformat(timestamp),
            'location': location
        }
        
        return {
            'user_id': user_id,
            'timestamp': timestamp,
            'merchant_category': merchant_category,
            'amount': amount,
            'location': location
        }
    
    def generate_high_value_fraud(self):
        """Generate a high-value fraud transaction (>5000)"""
        user_id = random.choice(self.user_ids)
        timestamp = datetime.now().isoformat()
        merchant_category = random.choice(['ELECTRONICS', 'TRAVEL', 'ONLINE_RETAIL'])
        
        # High value amount
        amount = round(random.uniform(5001, 15000), 2)
        location = random.choice(self.COUNTRIES)
        
        self.user_last_transaction[user_id] = {
            'timestamp': datetime.fromisoformat(timestamp),
            'location': location
        }
        
        transaction = {
            'user_id': user_id,
            'timestamp': timestamp,
            'merchant_category': merchant_category,
            'amount': amount,
            'location': location
        }
        
        logger.info(f"FRAUD INJECTED - High Value: {user_id} - ${amount} - {location}")
        return transaction
    
    def generate_impossible_travel_fraud(self):
        """Generate impossible travel fraud (same user, different countries within 10 min)"""
        # Find a user with recent transaction
        eligible_users = [
            uid for uid, last_tx in self.user_last_transaction.items()
            if (datetime.now() - last_tx['timestamp']).seconds < 600
        ]
        
        if not eligible_users:
            # Fallback to normal transaction
            return self.generate_normal_transaction()
        
        user_id = random.choice(eligible_users)
        last_location = self.user_last_transaction[user_id]['location']
        
        # Pick a different country
        new_location = random.choice([loc for loc in self.COUNTRIES if loc != last_location])
        
        timestamp = datetime.now().isoformat()
        merchant_category = random.choice(self.MERCHANT_CATEGORIES)
        amount = round(random.uniform(100, 2000), 2)
        
        self.user_last_transaction[user_id] = {
            'timestamp': datetime.fromisoformat(timestamp),
            'location': new_location
        }
        
        transaction = {
            'user_id': user_id,
            'timestamp': timestamp,
            'merchant_category': merchant_category,
            'amount': amount,
            'location': new_location
        }
        
        logger.info(f"FRAUD INJECTED - Impossible Travel: {user_id} - {last_location} -> {new_location}")
        return transaction


class TransactionProducer:
    """Kafka producer for transaction events"""
    
    def __init__(self, bootstrap_servers='localhost:9092', topic='transactions'):
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self.generator = TransactionGenerator()
        self.connect()
    
    def connect(self):
        """Connect to Kafka broker with retry logic"""
        max_retries = 5
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    acks='all',
                    retries=3
                )
                logger.info(f"Connected to Kafka at {self.bootstrap_servers}")
                return
            except NoBrokersAvailable:
                retry_count += 1
                logger.warning(f"Kafka not available. Retry {retry_count}/{max_retries}...")
                time.sleep(5)
        
        raise Exception("Failed to connect to Kafka after maximum retries")
    
    def send_transaction(self, transaction):
        """Send transaction to Kafka topic"""
        try:
            future = self.producer.send(self.topic, value=transaction)
            future.get(timeout=10)
            return True
        except Exception as e:
            logger.error(f"Failed to send transaction: {e}")
            return False
    
    def run(self, num_transactions=1000, fraud_ratio=0.15, delay=1.0):
        """
        Run the producer with controlled fraud injection
        
        Args:
            num_transactions: Total number of transactions to generate
            fraud_ratio: Ratio of fraudulent transactions (default 15%)
            delay: Delay between transactions in seconds
        """
        logger.info(f"Starting producer: {num_transactions} transactions, {fraud_ratio*100}% fraud rate")
        
        fraud_count = 0
        normal_count = 0
        
        for i in range(num_transactions):
            # Determine if this should be a fraud transaction
            if random.random() < fraud_ratio:
                # 60% high value fraud, 40% impossible travel fraud
                if random.random() < 0.6:
                    transaction = self.generator.generate_high_value_fraud()
                else:
                    transaction = self.generator.generate_impossible_travel_fraud()
                fraud_count += 1
            else:
                transaction = self.generator.generate_normal_transaction()
                normal_count += 1
            
            # Send to Kafka
            if self.send_transaction(transaction):
                if (i + 1) % 100 == 0:
                    logger.info(f"Progress: {i+1}/{num_transactions} | Normal: {normal_count} | Fraud: {fraud_count}")
            
            time.sleep(delay)
        
        logger.info(f"Producer finished: Normal={normal_count}, Fraud={fraud_count}")
        self.producer.close()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Transaction Producer')
    parser.add_argument('--bootstrap-servers', default='localhost:9092', help='Kafka bootstrap servers')
    parser.add_argument('--topic', default='transactions', help='Kafka topic name')
    parser.add_argument('--num-transactions', type=int, default=1000, help='Number of transactions')
    parser.add_argument('--fraud-ratio', type=float, default=0.15, help='Fraud ratio (0-1)')
    parser.add_argument('--delay', type=float, default=1.0, help='Delay between transactions (seconds)')
    
    args = parser.parse_args()
    
    producer = TransactionProducer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic
    )
    
    producer.run(
        num_transactions=args.num_transactions,
        fraud_ratio=args.fraud_ratio,
        delay=args.delay
    )
