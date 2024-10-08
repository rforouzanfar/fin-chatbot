import finnhub
import os
import logging
from dotenv import load_dotenv
import time
import asyncio
import aiohttp
import ssl
import json
from asyncio import Semaphore

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class RedisProcessor:
    def __init__(self, redis_manager, verify_ssl=False):
        self.redis_manager = redis_manager
        self.api_key = os.getenv('FINNHUB_API_KEY')
        if not self.api_key:
            raise ValueError("Finnhub API key not found")
        self.finnhub_client = finnhub.Client(api_key=self.api_key)
        self.verify_ssl = verify_ssl
        self.semaphore = Semaphore(30)  # Limit concurrent requests to 30
        self.retry_delay = 1  # Start with 1 second delay
        self.max_retries = 5
        self.api_call_limit = int(os.getenv('FINNHUB_API_CALL_LIMIT', 60))
        self.fetch_interval_hours = float(os.getenv('FETCH_INTERVAL_HOURS', 4))

    def get_ssl_context(self):
        if self.verify_ssl:
            return None
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        return ssl_context

    async def fetch_stock_data_async(self, session, symbol):
        url = f"https://finnhub.io/api/v1/quote?symbol={symbol}&token={self.api_key}"
        for attempt in range(self.max_retries):
            async with self.semaphore:
                try:
                    async with session.get(url, ssl=self.get_ssl_context()) as response:
                        if response.status == 200:
                            data = await response.json()
                            price = data.get('c')
                            if price:
                                self.redis_manager.set_stock_price(symbol, price)
                                logger.debug(f"Stock data fetched and stored for symbol: {symbol}")
                                return True
                            else:
                                logger.warning(f"No price data for symbol: {symbol}")
                                return False
                        elif response.status == 429:
                            logger.warning(f"Rate limit hit for {symbol}. Retrying after delay.")
                            await asyncio.sleep(self.retry_delay)
                            self.retry_delay *= 2  # Exponential backoff
                        else:
                            logger.error(f"Error fetching data for {symbol}: HTTP {response.status}")
                            return False
                except Exception as e:
                    logger.error(f"Error fetching stock data for {symbol}: {e}")
                    await asyncio.sleep(1)
        logger.error(f"Failed to fetch data for {symbol} after {self.max_retries} attempts")
        return False

    async def fetch_all_stocks_async(self):
        stocks = self.get_available_stocks('US')
        total_stocks = len(stocks)
        fetched_count = 0
        failed_count = 0
        async with aiohttp.ClientSession() as session:
            for stock in stocks:
                success = await self.fetch_stock_data_async(session, stock['symbol'])
                if success:
                    fetched_count += 1
                else:
                    failed_count += 1
                if fetched_count + failed_count >= self.api_call_limit:
                    logger.warning("API call limit reached. Stopping fetch process.")
                    break
        return total_stocks, fetched_count, failed_count

    def fetch_all_stocks(self):
        logger.info("Starting to fetch all stock data...")
        total_stocks, fetched_count, failed_count = asyncio.run(self.fetch_all_stocks_async())
        logger.info(f"Total stocks available via Finnhub API: {total_stocks}")
        logger.info(f"Successfully fetched and stored data for {fetched_count} stocks")
        logger.info(f"Failed to fetch data for {failed_count} stocks")
        return total_stocks, fetched_count, failed_count

    def get_available_stocks(self, exchange='US'):
        try:
            stocks = self.redis_manager.get_cached_stock_list(exchange)
            if not stocks:
                stocks = self.finnhub_client.stock_symbols(exchange)
                self.redis_manager.cache_stock_list(exchange, stocks)
            logger.info(f"Retrieved {len(stocks)} stocks for exchange {exchange}")
            return stocks
        except Exception as e:
            logger.error(f"Error fetching stock symbols: {e}")
            return []

    def get_stock_data(self, symbol, start_time='-inf', end_time='+inf', limit=10):
        prices = self.redis_manager.get_stock_prices(symbol, start_time, end_time, limit)
        if prices:
            logger.info(f"Stock data retrieved from Redis for symbol: {symbol}")
        else:
            logger.warning(f"No stock data found in Redis for symbol: {symbol}")
        return prices

    def get_latest_stock_data(self, symbol):
        price, timestamp = self.redis_manager.get_latest_stock_price(symbol)
        if price is not None:
            logger.info(f"Latest stock data retrieved from Redis for symbol: {symbol}")
            return price, timestamp
        else:
            logger.warning(f"No stock data found in Redis for symbol: {symbol}")
            return None, None

    def get_available_symbols(self):
        symbols = self.redis_manager.get_all_symbols()
        return symbols, len(symbols)

    def process_all_available_stocks(self):
        logger.info("Processing all available stocks...")
        total_stocks, fetched_count, failed_count = self.fetch_all_stocks()
        symbols, stored_count = self.get_available_symbols()
        logger.info(f"Total stocks stored in Redis: {stored_count}")
        return total_stocks, fetched_count, failed_count, stored_count

    def erase_all_stock_data(self):
        try:
            stock_keys = self.redis_manager.get_all_keys("stock:*")
            deleted_count = self.redis_manager.delete_keys(stock_keys)
            logger.info(f"Erased {deleted_count} stock data entries from Redis")
            return deleted_count
        except Exception as e:
            logger.error(f"Error erasing stock data: {e}")
            return 0

    def get_fetch_interval_seconds(self):
        return self.fetch_interval_hours * 3600