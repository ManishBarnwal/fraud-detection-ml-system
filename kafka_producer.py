from kafka import KafkaProducer
import time
import random
import json
from datetime import datetime

class TransactionProducer:
    """
    TransactionProducer is a class that generates random transaction stream data and sends it to the kafka topic
    """
    def __init__(self, bootstrap_servers=["localhost:9092"]):
        self.producer = KafkaProducer(
            # the kafka broker/server that the producer will connect to; a broker is a server that manages the kafka cluster and is responsible for storing and serving the data to consumers
            # broker/server utility: it acts as a load balancer, handles data replication for fault tolerance, and ensures data durability and availability across the cluster
            bootstrap_servers=bootstrap_servers,  
            # serialize the transcation data from dict to json and encode it to utf-8 so that consumer can understand it
            value_serializer = lambda x: json.dumps(x).encode('utf-8')  
        )

    def generate_transaction(self):
        """
        Generate a random transaction data
        """
        return {
            'transaction_id': f'tx_{int(time.time())}_{random.randint(1000, 99999)}',
            'user_id': f'user_{random.randint(1, 10000)}',
            'transaction_amount': random.randint(100, 10000),
            'merchant': random.choice(['Amazon', 'Flipkart', 'Swiggy', 'Yatra', 'ShellPump', 'ThirdWave']),
            'location': random.choice(['Koramangala', 'Sarjapura', 'HSR', 'Bomanahalli', 'Indiranagar']),

            'merchant_category': random.choice(['shopping', 'food', 'restuarant', 'petrol']),
            'card_type': random.choice(['credit', 'debit']),
            'time': datetime.now().isoformat()
        }
    
    def send_transaction(self, topic='transactions', num_transactions=100):
        """
        Send a random transaction data to the kafka topic
        """
        for i in range(num_transactions):
            transaction = self.generate_transaction() # generate a random transaction data
            self.producer.send(value=transaction, topic=topic) # send the transaction data to the kafka topic
            time.sleep(0.1) # wait for 0.1 second before sending the next transaction
        self.producer.flush()  # force any buffered messages to be sent to the kafka broker/server before closing the producer


transaction_generator = TransactionProducer()
example_transaction = transaction_generator.generate_transaction()
print(example_transaction)

transaction_generator.send_transaction(num_transactions=10) # send 10 random transaction data to the kafka topic

