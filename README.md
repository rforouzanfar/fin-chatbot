# Fin-Chatbot

**Fin-Chatbot** is a financial assistant chatbot that retrieves stock data and generates investment recommendations based on a specified investment amount and time frame. It collects financial data from multiple sources, including APIs and web scraping, to provide real-time stock prices and investment options.

## Features

- Fetch real-time stock prices from **Alpha Vantage**, **IEX Cloud**, **Finnhub**, and **Yahoo Finance**.
- Scrape stock data from **Yahoo Finance** using **Scrapy**.
- Combine data from multiple sources to generate investment recommendations.
- Built-in Flask API for querying stock prices or investment suggestions.

## Tech Stack

- **Flask**: Used for the chatbot API.
- **Scrapy**: Web scraping framework for retrieving stock prices from Yahoo Finance.
- **Requests**: To make API calls to Alpha Vantage, IEX Cloud, and Finnhub.
- **yfinance**: Alternative to scrape-free access to Yahoo Finance data.
- **OpenAI** (optional): For generating natural language responses (investment recommendations) using ChatGPT.

---

## Project Structure

```bash
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
# Fin-Chatbot

**Fin-Chatbot** is a financial assistant chatbot that retrieves stock data and generates investment recommendations based on a specified investment amount and time frame. It collects financial data from multiple sources, including APIs and web scraping, to provide real-time stock prices and investment options.

## Features

- Fetch real-time stock prices from **Alpha Vantage**, **IEX Cloud**, **Finnhub**, and **Yahoo Finance**.
- Scrape stock data from **Yahoo Finance** using **Scrapy**.
- Combine data from multiple sources to generate investment recommendations.
- Built-in Flask API for querying stock prices or investment suggestions.

## Tech Stack

- **Flask**: Used for the chatbot API.
- **Scrapy**: Web scraping framework for retrieving stock prices from Yahoo Finance.
- **Requests**: To make API calls to Alpha Vantage, IEX Cloud, and Finnhub.
- **yfinance**: Alternative to scrape-free access to Yahoo Finance data.
- **OpenAI** (optional): For generating natural language responses (investment recommendations) using ChatGPT.

---

## Project Structure

```bash
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
