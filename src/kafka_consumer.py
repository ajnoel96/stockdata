from kafka import KafkaConsumer
import json
import os
from database import insert_stock_data  # Make sure this import is correct

# Define the path to the config.json file
config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.json')

# Load the configuration from the JSON file
with open(config_path, 'r') as file:
    config = json.load(file)

def consume_from_kafka():
    consumer = KafkaConsumer(
        config['topic_name'],
        bootstrap_servers=config['kafka_bootstrap_servers'],
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    for message in consumer:
        data = message.value
        print(f"Consumed message: {data}")

        # Extract relevant stock data from the consumed message
        stock_data = []
        for timestamp, values in data.get('Time Series (5min)', {}).items():
            stock_data.append({
                'symbol': data.get('Meta Data', {}).get('2. Symbol'),
                'timestamp': timestamp,
                'open': float(values.get('1. open')),
                'high': float(values.get('2. high')),
                'low': float(values.get('3. low')),
                'close': float(values.get('4. close')),
                'volume': int(values.get('5. volume'))
            })

        # Insert data into the database
        if stock_data:
            insert_stock_data(stock_data)

if __name__ == "__main__":
    consume_from_kafka()
