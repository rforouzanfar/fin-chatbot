from .finance_api import fetch_stock_price

def generate_recommendations(amount, period):
    # Example logic to fetch stock data and generate recommendations
    stock_price = fetch_stock_price('AAPL')  # Fetch stock price from external API
    if period >= 5:
        return {
            "recommendation": f"For {period} years, invest ${amount} in AAPL, currently priced at ${stock_price}."
        }
    else:
        return {
            "recommendation": f"For {period} years, consider bonds or ETFs. AAPL is currently priced at ${stock_price}."
        }
