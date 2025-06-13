# Fraud Detection ML System

This system implements a real-time fraud detection pipeline using Kafka for message streaming, MongoDB for transaction storage, and Redis for feature caching.

## Prerequisites

1. Install required services using Homebrew:
```bash
brew install kafka
brew install mongodb-community
brew install redis
```

2. Start the required services:
```bash
brew services start zookeeper
brew services start kafka
brew services start mongodb-community
brew services start redis
```

3. Install Python dependencies using uv:
```bash
uv pip install kafka-python pymongo redis pandas
```

## System Components

1. **Kafka Producer** (`kafka_producer.py`):
   - Generates random transaction data
   - Sends transactions to Kafka topic 'transactions'

2. **Kafka Consumer & Feature Engineer** (`kafka_consumer_feature_engineer.py`):
   - Consumes transactions from Kafka
   - Stores raw transactions in MongoDB
   - Engineers features for fraud detection
   - Caches features in Redis

## Running the System

1. Start the consumer in one terminal:
```bash
uv run kafka_consumer_feature_engineer.py
```

2. Start the producer in another terminal:
```bash
uv run kafka_producer.py
```

The producer will send 10 random transactions by default. You can modify the number of transactions by changing the `num_transactions` parameter in `kafka_producer.py`.

## Expected Output

When running, you should see:

1. In the producer terminal:
   - Example transaction data
   - Confirmation of sent transactions

2. In the consumer terminal:
   - Received transaction messages
   - User history from MongoDB
   - Engineered features for each transaction

## Data Flow

1. Producer generates random transactions with:
   - Transaction ID
   - User ID
   - Amount
   - Merchant details
   - Location
   - Timestamp

2. Consumer processes each transaction:
   - Stores raw transaction in MongoDB
   - Retrieves user's transaction history
   - Engineers features including:
     - Time-based features (hour, day)
     - Amount statistics
     - Location frequency
     - Transaction patterns
   - Caches features in Redis

## Troubleshooting

If you encounter any issues:

1. Verify all services are running:
```bash
brew services list
```

2. Check MongoDB connection:
```bash
mongosh
```

3. Check Redis connection:
```bash
redis-cli ping
```

4. Check Kafka topics:
```bash
kafka-topics --list --bootstrap-server localhost:9092
```
