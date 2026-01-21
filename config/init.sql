-- Initialize fraud detection database

CREATE TABLE IF NOT EXISTS fraud_transactions (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    merchant_category VARCHAR(100) NOT NULL,
    amount DOUBLE PRECISION NOT NULL,
    location VARCHAR(10) NOT NULL,
    fraud_reason VARCHAR(255) NOT NULL,
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_fraud_user_id ON fraud_transactions(user_id);
CREATE INDEX idx_fraud_timestamp ON fraud_transactions(timestamp);
CREATE INDEX idx_fraud_merchant ON fraud_transactions(merchant_category);

-- Create reconciliation table for batch processing
CREATE TABLE IF NOT EXISTS reconciliation_reports (
    id SERIAL PRIMARY KEY,
    report_date TIMESTAMP NOT NULL,
    total_transactions BIGINT NOT NULL,
    total_amount DOUBLE PRECISION NOT NULL,
    fraud_count BIGINT NOT NULL,
    fraud_amount DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
