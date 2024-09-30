import requests

def fetch_stock_price(symbol):
    api_key = 'YOUR_API_KEY'
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval=5min&apikey={api_key}'
    response = requests.get(url)
    data = response.json()
    return data['Time Series (5min)'][list(data['Time Series (5min)'].keys())[0]]['4. close']
