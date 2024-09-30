import yfinance as yf
import requests
import os

# API keys for Alpha Vantage and Finnhub
ALPHA_VANTAGE_API_KEY = os.getenv('ALPHA_VANTAGE_API_KEY', 'your_alpha_vantage_api_key')
FINNHUB_API_KEY = os.getenv('FINNHUB_API_KEY', 'your_finnhub_api_key')

# List of symbols (stocks and index funds)
SYMBOLS = ['AAPL', 'MSFT', 'GOOG', 'AMZN', 'SPY', 'VIX']

# Fetch current price and historical data using yfinance for multiple symbols
# Fetch current price and historical data using yfinance for multiple symbols
def fetch_yahoo_finance_data(symbols):
    results = []
    for symbol in symbols:
        stock = yf.Ticker(symbol)
        # Current price
        current_price = stock.history(period='1d')['Close'].iloc[-1]

        # Historical data (past 1 year as an example)
        historical_data = stock.history(period='1y')
        # Convert historical returns (Pandas Series) to a list or dictionary
        historical_returns = historical_data['Close'].pct_change().dropna().tolist()  # Convert to list

        results.append({
            'symbol': symbol,
            'current_price': current_price,
            'historical_returns': historical_returns,  # Now it's a list
        })
    return results



# Fetch VIX (Volatility Index) using Finnhub API
def fetch_volatility_index():
    url = f'https://finnhub.io/api/v1/quote?symbol=VIX&token={FINNHUB_API_KEY}'
    response = requests.get(url)
    data = response.json()

    if 'c' in data:  # 'c' stands for current price
        return {
            'symbol': 'VIX',
            'current_price': data['c'],
        }
    return {'error': 'Failed to fetch VIX data'}


# Fetch current prices using Alpha Vantage for multiple symbols
def fetch_alpha_vantage_prices(symbols):
    results = []
    for symbol in symbols:
        url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval=5min&apikey={ALPHA_VANTAGE_API_KEY}'
        response = requests.get(url)
        data = response.json()

        if 'Time Series (5min)' in data:
            latest_time = list(data['Time Series (5min)'].keys())[0]
            latest_price = data['Time Series (5min)'][latest_time]['4. close']
            results.append({'symbol': symbol, 'current_price': latest_price})
        else:
            results.append({'symbol': symbol, 'error': f"Failed to fetch data for {symbol}"})

    return results


# Fetch data for a variety of stocks and index funds
def fetch_all_data(symbols):
    # Fetch from Yahoo Finance
    yahoo_finance_data = fetch_yahoo_finance_data(symbols)

    # Fetch volatility index (VIX) from Finnhub
    vix_data = fetch_volatility_index()

    # Fetch current prices from Alpha Vantage
    alpha_vantage_data = fetch_alpha_vantage_prices(symbols)

    return {
        'yahoo_finance_data': yahoo_finance_data,
        'vix_data': vix_data,
        'alpha_vantage_data': alpha_vantage_data,
    }
