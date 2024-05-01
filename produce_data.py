from faker import Faker
from confluent_kafka import Producer
import json
import random
from datetime import datetime, timedelta

# Initialize Faker for data generation
fake = Faker()

# Function to generate a fake user document
def generate_user_document():
    return {
        "name": fake.name(),
        "phone_number": fake.phone_number(),
        "service_start_date": (datetime.now() - timedelta(days=random.randint(1, 365))).strftime('%Y-%m-%d'),
        "current_balance_SMS": random.randint(0, 500),
        "data_bundles_MB": random.randint(0, 10000)
    }

# Function to generate and send data to Redpanda
def produce_fake_data(producer, topic_name, num_messages=100):
    for _ in range(num_messages):
        user_document = generate_user_document()
        # Convert the user document to a JSON string
        user_document_json = json.dumps(user_document)
        # Send the data to Redpanda
        producer.produce(topic_name, value=user_document_json)
        producer.flush()

if __name__ == "__main__":
    # Configuration for connecting to your Redpanda cluster
    conf = {'bootstrap.servers': 'localhost:9092'}
    
    # Create a Producer instance
    producer = Producer(**conf)
    
    # Name of the Redpanda topic
    topic_name = 'user_documents'
    
    # Generate and send the data
    produce_fake_data(producer, topic_name, 100)  # Change 100 to desired number of messages
