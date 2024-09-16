import psycopg2
import json
import os

# Define the path to the config.json file
config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.json')

# Load the configuration from the JSON file
with open(config_path, 'r') as file:
    config = json.load(file)

def get_db_connection():
    #Establish and return a database connection.
    return psycopg2.connect(
        dbname=config['database_name'],
        user=config['database_user'],
        password=config['database_password'],
        host=config['database_host'],
        port=config['database_port']
    )

def insert_stock_data(stock_data):
    #Insert stock data into the database.
    conn = get_db_connection()
    cursor = conn.cursor()
    
    insert_query = """
    INSERT INTO stock_data (symbol, timestamp, open, high, low, close, volume)
    VALUES (%s, %s, %s, %s, %s, %s, %s);
    """
    
    # Process each record
    for record in stock_data:
        cursor.execute(insert_query, (
            record.get('symbol'),
            record.get('timestamp'),
            record.get('open'),
            record.get('high'),
            record.get('low'),
            record.get('close'),
            record.get('volume')
        ))
    
    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    # Example usage
    sample_data = [
        {
            'symbol': 'AAPL',
            'timestamp': '2024-09-13 19:55:00',
            'open': 222.26,
            'high': 222.26,
            'low': 222.22,
            'close': 222.25,
            'volume': 8729
        }
    ]
    insert_stock_data(sample_data)
