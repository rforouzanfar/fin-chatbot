from flask import Flask, request, jsonify
from .services.recommendations import generate_recommendations

def create_app():
    app = Flask(__name__)

    @app.route('/invest', methods=['POST'])
    def get_investment_suggestions():
        data = request.json

        # Extract investment amount and period from the request
        amount = data.get('amount')
        period = data.get('period')

        if not amount or not period:
            return jsonify({"error": "Missing required parameters"}), 400

        # Generate investment recommendations
        recommendations = generate_recommendations(amount, period)

        # Return the recommendations as a JSON response
        return jsonify(recommendations), 200

    return app

if __name__ == '__main__':
    app = create_app()
    app.run(debug=True)
