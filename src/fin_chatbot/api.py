from flask import Flask, request, jsonify
from .services.recommendations import generate_recommendations
from .services.fetch_data import fetch_all_data  # Import the new fetch_data functionality

def create_app():
    app = Flask(__name__)

    @app.route('/invest', methods=['POST'])
    def get_investment_suggestions():
        data = request.json

        # Extract investment amount, period, and symbols from the request
        amount = data.get('amount')
        period = data.get('period')
        symbols = data.get('symbols', ['AAPL', 'MSFT', 'GOOG'])  # Default symbols if none provided

        if not amount or not period:
            return jsonify({"error": "Missing required parameters"}), 400

        # Fetch stock data based on the symbols provided
        stock_data = fetch_all_data(symbols)

        # Generate investment recommendations based on the amount, period, and stock data
        recommendations = generate_recommendations(amount, period, symbols)

        # Return the recommendations and stock data as a JSON response
        return jsonify({
            'recommendations': recommendations,
            'stock_data': stock_data
        }), 200

    return app

if __name__ == '__main__':
    app = create_app()
    app.run(debug=True)
