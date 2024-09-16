import time
import os
import json
from api_call import fetch_stock_data
from kafka_producer import create_producer, produce_stock_data

# Define the path to the config.json file
config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.json')

# Load the configuration from the JSON file
with open(config_path, 'r') as file:
    config = json.load(file)

def main():
    producer = create_producer()  # Create Kafka producer
    print("Producer created.")
    while True:
        data = fetch_stock_data() # Fetch stock data from API
        print(f"Fetched data: {data}") 
        produce_stock_data(producer, data)  # Send data to Kafka
        print("Data sent to Kafka.")
        time.sleep(config['interval'])

if __name__ == "__main__":
    main()
