# kafka consumer and feature engineering
# Sets up the kafka consumer
# sets up the MongoDB for storing the features for machine learning model
# sets up the Redis client for caching the features

from kafka import KafkaConsumer
from pymongo import MongoClient
from redis import Redis
import pandas as pd
import json
class FeatureEngineer:
    def __init__(self, mongo_db_name='fraud_detection', transaction_collection_name='transactions'):
        self.mongo_client = MongoClient('mongodb://localhost:27017/')
        self.mongo_db = self.mongo_client[mongo_db_name]
        self.transaction_collection = self.mongo_db[transaction_collection_name]
        self.redis_client = Redis(host='localhost', port=6379, db=0)
        
    def test_connections(self):
        try:
            # Test MongoDB connection
            self.mongo_client.server_info()
            print("MongoDB connection successful!")
            
            # Test Redis connection
            self.redis_client.ping()
            print("Redis connection successful!")
            
            return True
        except Exception as e:
            print(f"Connection error: {str(e)}")
            return False

    def consume_and_engineer_features(self):
        consumer = KafkaConsumer(
            'transactions',
            bootstrap_servers='localhost:9092',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

        for message in consumer:
            print(f"Received message: {message.value}")
            print('-'*20)
            transaction = message.value


            # store raw transaction in MongoDB
            self.transaction_collection.insert_one(transaction)

            # engineer features
            features = self.engineer_features(transaction)

            # store features in Redis for real-time access
            feature_key = f"features:{transaction['transaction_id']}"
            self.redis_client.hset(feature_key, mapping=features)
            self.redis_client.expire(name=feature_key, time=3600)

            # send to feature store
            self.send_to_feature_store(features)

    def send_to_feature_store(self, features):
        print('~'*20)
        print(f"Features engineered for {features['transaction_id']}: {features}")
        print('*'*20)

# example transaction
# {'transaction_id': 'tx_1749796063_89303', 'user_id': 'user_2462', 
#  'transaction_amount': 3415, 'merchant': 'Flipkart', 'location': 'HSR', 
#  'merchant_category': 'petrol', 'card_type': 'debit', 'time': '2025-06-13T11:57:43.976711'}

    def engineer_features(self, transaction):

        user_id = transaction['user_id']

        # get 100 most recent user_history from MongoDB table based on the user_id
        user_history = list(self.transaction_collection.find(
            {'user_id': user_id},
            sort=[('timestamp', -1)],
            limit=100
        ))

        print(f"user_history: {user_history}")


        features = {
            'transaction_id': transaction['transaction_id'],
            'user_id': transaction['user_id'],
            'amount': transaction['transaction_amount'],
            'hour_of_day': pd.to_datetime(transaction['time']).hour,
            'day_of_week': pd.to_datetime(transaction['time']).weekday(),
        }

        if len(user_history) > 1:
            amounts = [t['transaction_amount'] for t in user_history[1:]]  # Exclude current transaction
            features.update({
                'avg_amount_30d': sum(amounts) / len(amounts),
                'amount_std_30d': pd.Series(amounts).std(),
                'transaction_count_24h': len([t for t in user_history[1:] 
                                            if (pd.to_datetime(transaction['timestamp']) - 
                                                pd.to_datetime(t['timestamp'])).total_seconds() < 86400]),
                'amount_deviation': abs(transaction['amount'] - (sum(amounts) / len(amounts))),
            })
            
            # Location-based features
            locations = [t['location'] for t in user_history[1:6]]  # Last 5 locations
            features['location_frequency'] = locations.count(transaction['location']) / len(locations) if locations else 0
        else:
            # New user - set default values
            features.update({
                'avg_amount_30d': transaction['transaction_amount'],
                'amount_std_30d': 0,
                'transaction_count_24h': 0,
                'amount_deviation': 0,
                'location_frequency': 1.0
            })
        
        return features

        

# Create instance and test connections
feature_engineer = FeatureEngineer()
feature_engineer.test_connections()
print("Running consume_and_engineer_features")
feature_engineer.consume_and_engineer_features()  # Uncomment when ready to start consuming

# TODO start here
# 1. Install mongodb and redis using brew
