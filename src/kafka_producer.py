import json
import os
from kafka import KafkaProducer

# Define the path to the config.json file
config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.json')

# Load the configuration from the JSON file
with open(config_path, 'r') as file:
    config = json.load(file)

def create_producer():
    kafka_bootstrap_servers = config['kafka_bootstrap_servers']
    return KafkaProducer(bootstrap_servers=kafka_bootstrap_servers,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def produce_stock_data(producer, stock_data):
    topic_name = config['topic_name']
    print(f"Producing data to topic {topic_name}: {stock_data}")
    producer.send(topic_name, stock_data)
    print("Data produced.")
    producer.flush()

if __name__ == "__main__":
    producer = create_producer()
    stock_data = {"example_key": "example_value"}  # Dummy data for testing
    produce_stock_data(producer, stock_data)
    producer.close()
