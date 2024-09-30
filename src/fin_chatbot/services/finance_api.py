import os
import requests
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def fetch_stock_price(symbol):
    # Get the API key
    api_key = os.getenv('ALPHA_VANTAGE_API_KEY')

    # Debug print statement
    print(f"API key loaded from environment: {api_key}")

    if not api_key:
        raise Exception("API key not found. Make sure ALPHA_VANTAGE_API_KEY is set in the environment.")

    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval=5min&apikey={api_key}'
    response = requests.get(url)
    data = response.json()

    # Extract the latest stock price
    latest_price = data['Time Series (5min)'][list(data['Time Series (5min)'].keys())[0]]['4. close']
    return latest_price
