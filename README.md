Fin-Chatbot
Fin-Chatbot is a financial assistant chatbot that retrieves stock data and generates investment recommendations based on a specified investment amount and time frame. It collects financial data from multiple sources, including APIs and web scraping, to provide real-time stock prices and investment options.

Features
Fetch real-time stock prices from Alpha Vantage, IEX Cloud, Finnhub, and Yahoo Finance.
Scrape stock data from Yahoo Finance using Scrapy.
Combine data from multiple sources to generate investment recommendations.
Built-in Flask API for querying stock prices or investment suggestions.
Tech Stack
Flask: Used for the chatbot API.
Scrapy: Web scraping framework for retrieving stock prices from Yahoo Finance.
Requests: To make API calls to Alpha Vantage, IEX Cloud, and Finnhub.
yfinance: Alternative to scrape-free access to Yahoo Finance data.
OpenAI (optional): For generating natural language responses (investment recommendations) using ChatGPT.
Project Structure

fin_chatbot/
├── scrapy.cfg                     # Scrapy configuration file
├── src/
│   ├── fin_chatbot/
│   │   ├── __init__.py
│   │   ├── api.py                 # Flask API for chatbot
│   │   ├── config.py              # Configuration file for API keys
│   │   ├── services/
│   │   │   ├── __init__.py
│   │   │   ├── recommendations.py # Investment recommendation logic
│   │   │   ├── fetch_data.py      # Logic to fetch data from APIs and Scrapy
│   │   └── scrapy_spiders/
│   │       ├── __init__.py
│   │       ├── yahoo_finance_spider.py  # Scrapy spider for Yahoo Finance
│   ├── tests/
│   │   ├── __init__.py
│   │   ├── test_fetch_data.py     # Unit tests for fetching data
│   ├── requirements.txt           # Project dependencies
│   ├── settings.py                # Scrapy settings (if needed)
├── setup.py                       # Project setup (optional)


Setup and Installation
Prerequisites
Python 3.8+
Alpha Vantage, IEX Cloud, and Finnhub API keys (optional but recommended for API access).
1. Clone the repository
git clone https://github.com/your-username/fin-chatbot.git
cd fin-chatbot
2. Set up the Python environment
Using Anaconda (recommended) or virtualenv, create and activate the environment:
conda create -n chatbot python=3.8
conda activate chatbot
Alternatively, using virtualenv:
python -m venv chatbot
source chatbot/bin/activate  # On Windows: chatbot\Scripts\activate
3. Install dependencies
pip install -r requirements.txt
4. Set API Keys
Add your API keys to src/fin_chatbot/config.py or use environment variables:

ALPHA_VANTAGE_API_KEY = os.getenv('ALPHA_VANTAGE_API_KEY', 'your_alpha_vantage_api_key')
IEX_CLOUD_API_KEY = os.getenv('IEX_CLOUD_API_KEY', 'your_iex_cloud_api_key')
FINNHUB_API_KEY = os.getenv('FINNHUB_API_KEY', 'your_finnhub_api_key')
Alternatively, you can set environment variables:


export ALPHA_VANTAGE_API_KEY=your_alpha_vantage_api_key
export IEX_CLOUD_API_KEY=your_iex_cloud_api_key
export FINNHUB_API_KEY=your_finnhub_api_key
5. Run the Flask API
You can run the Flask API to query stock prices and get investment recommendations:

bash
Copy code
python src/fin_chatbot/api.py
By default, the API will be available at http://localhost:5000.

6. Run Scrapy Spiders
You can run the Yahoo Finance Scrapy spider from the command line to retrieve stock prices:

cd fin_chatbot
scrapy crawl yahoo_finance -a symbol=AAPL
This will scrape the stock price for Apple (AAPL) from Yahoo Finance.

Usage
1. Fetch stock data via the API
Make a POST request to the /recommend endpoint to get stock prices or investment recommendations based on an amount and time frame.

Example Request:

curl -X POST http://localhost:5000/recommend \
    -H "Content-Type: application/json" \
    -d '{"amount": 10000, "years": 5}'
2. Scrape stock prices using Scrapy
You can use the provided Scrapy spider to scrape Yahoo Finance for stock prices. Example:

scrapy crawl yahoo_finance -a symbol=GOOG
Running Tests
Unit tests are located in the src/tests/ directory. Run the tests using pytest:

pytest src/tests/
API Providers
The chatbot fetches data from the following providers:

Alpha Vantage: Real-time stock data (requires API key).
IEX Cloud: Stock prices and financial information (requires API key).
Finnhub: Stock data, news, and forex (requires API key).
Yahoo Finance: Stock data scraped via Scrapy.
Future Improvements
Integrate with OpenAI's GPT-3 to generate investment advice in natural language.
Expand the chatbot's capabilities to include more financial data, such as bonds and cryptocurrencies.
Add caching for API results to avoid hitting rate limits.
License
This project is licensed under the MIT License. See the LICENSE file for details.

Contributing
Contributions are welcome! Feel free to open issues or submit pull requests to enhance the functionality of the chatbot.

Contact
For any questions, please contact:

Your Name
Email: marykhlj@gmail.com
GitHub: @maryamkhlj
