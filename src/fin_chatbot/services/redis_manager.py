import redis
import logging
import os
import json
import time
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class RedisManager:
    def __init__(self):
        self.redis_host = os.getenv('REDIS_HOST', 'localhost')
        self.redis_port = int(os.getenv('REDIS_PORT', 6379))
        self.redis_db = int(os.getenv('REDIS_DB', 0))
        self.connect()

    def connect(self):
        try:
            self.redis_client = redis.Redis(
                host=self.redis_host, 
                port=self.redis_port, 
                db=self.redis_db,
                decode_responses=True
            )
            self.redis_client.ping()  # Test the connection
            logger.info(f"Connected to Redis at {self.redis_host}:{self.redis_port}")
        except redis.ConnectionError as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise

    def set_stock_price(self, symbol, price, timestamp=None):
        try:
            key = f"stock:{symbol}:price"
            if timestamp is None:
                timestamp = int(time.time())
            self.redis_client.zadd(key, {f"{price}:{timestamp}": timestamp})
            logger.debug(f"Stock price set for {symbol}: {price} at {timestamp}")
        except redis.RedisError as e:
            logger.error(f"Error setting stock price: {e}")

    def get_stock_prices(self, symbol, start_time='-inf', end_time='+inf', limit=10):
        try:
            key = f"stock:{symbol}:price"
            data = self.redis_client.zrevrangebyscore(key, end_time, start_time, start=0, num=limit, withscores=True)
            parsed_data = [(float(item[0].split(':')[0]), int(item[1])) for item in data]
            logger.debug(f"Stock prices retrieved for {symbol}")
            return parsed_data
        except redis.RedisError as e:
            logger.error(f"Error getting stock prices: {e}")
            return []

    def get_latest_stock_price(self, symbol):
        try:
            key = f"stock:{symbol}:price"
            data = self.redis_client.zrevrange(key, 0, 0, withscores=True)
            if data:
                price, timestamp = data[0][0].split(':')[0], data[0][1]
                return float(price), int(timestamp)
            return None, None
        except redis.RedisError as e:
            logger.error(f"Error getting latest stock price: {e}")
            return None, None

    def get_all_symbols(self):
        try:
            keys = self.redis_client.keys("stock:*:price")
            return [key.split(':')[1] for key in keys]
        except redis.RedisError as e:
            logger.error(f"Error getting stock symbols: {e}")
            return []

    def get_all_keys(self, pattern="*"):
        try:
            return self.redis_client.keys(pattern)
        except redis.RedisError as e:
            logger.error(f"Error getting keys: {e}")
            return []

    def delete_keys(self, keys):
        try:
            return self.redis_client.delete(*keys)
        except redis.RedisError as e:
            logger.error(f"Error deleting keys: {e}")
            return 0

    def cache_stock_list(self, exchange, stocks):
        try:
            key = f"stock_list:{exchange}"
            self.redis_client.set(key, json.dumps(stocks))
            logger.info(f"Cached stock list for exchange {exchange}")
        except redis.RedisError as e:
            logger.error(f"Error caching stock list: {e}")

    def get_cached_stock_list(self, exchange):
        try:
            key = f"stock_list:{exchange}"
            cached_stocks = self.redis_client.get(key)
            if cached_stocks:
                return json.loads(cached_stocks)
            return None
        except redis.RedisError as e:
            logger.error(f"Error getting cached stock list: {e}")
            return None