import requests
import json
import os

# Define the path to the config.json file
config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.json')

# Load the configuration from the JSON file
with open(config_path, 'r') as file:
    config = json.load(file)
    
def fetch_stock_data():
    api_key = config['alpha_vantage_api_key']
    symbol = config['symbol']
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval=5min&apikey={api_key}"

    response = requests.get(url)
    data = response.json()
    
    # Save data or return it to the calling function
    return data

if __name__ == "__main__":
    data = fetch_stock_data()
    
    # Define the path to the data folder
    data_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'stock_data.json')
    
    # Save the data to the file
    with open(data_path, 'w') as f:
        json.dump(data, f)

