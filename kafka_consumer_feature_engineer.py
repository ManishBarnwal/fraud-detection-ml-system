# kafka consumer and feature engineering
# Sets up the kafka consumer
# sets up the MongoDB for storing the features for machine learning model
# sets up the Redis client for caching the features

from kafka import KafkaConsumer
from pymongo import MongoClient
from redis import Redis
import json
class FeatureEngineer:
    def __init__(self, mongo_db_name='fraud_detection', mongo_collection_name='transactions'):
        self.mongo_client = MongoClient('mongodb://localhost:27017/')
        self.mongo_db = self.mongo_client[mongo_db_name]
        self.mongo_collection = self.mongo_db[mongo_collection_name]
        self.redis_client = Redis(host='localhost', port=6379, db=0)
        
    def test_connections(self):
        try:
            # Test MongoDB connection
            self.mongo_client.server_info()
            print("✅ MongoDB connection successful!")
            
            # Test Redis connection
            self.redis_client.ping()
            print("✅ Redis connection successful!")
            
            return True
        except Exception as e:
            print(f"❌ Connection error: {str(e)}")
            return False

    def consume_and_engineer_features(self):
        consumer = KafkaConsumer(
            'transactions',
            bootstrap_servers='localhost:9092',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

        for message in consumer:
            print(f"Received message: {message.value}")
            # TODO: engineer features
            # TODO: store features in MongoDB
            # TODO: cache features in Redis

    def engineer_features(self):
        pass

# Create instance and test connections
feature_engineer = FeatureEngineer()
feature_engineer.test_connections()
feature_engineer.consume_and_engineer_features()  # Uncomment when ready to start consuming
# TODO start here
# 1. Install mongodb and redis using brew
