@echo off
echo Running Transaction Producer...

REM Install dependencies if not already installed
pip install kafka-python

REM Run producer with default settings
REM Generate 500 transactions with 15% fraud rate
python producers/transaction_producer.py --bootstrap-servers localhost:9092 --topic transactions --num-transactions 500 --fraud-ratio 0.15 --delay 0.5

echo Producer finished!
