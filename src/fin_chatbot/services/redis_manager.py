import redis
import json
import time
import logging
from dotenv import load_dotenv
import os

load_dotenv()

logger = logging.getLogger(__name__)

class RedisManager:
    def __init__(self):
        self.redis = redis.Redis(
            host=os.getenv('REDIS_HOST', 'localhost'),
            port=int(os.getenv('REDIS_PORT', 6379)),
            db=int(os.getenv('REDIS_DB', 0)),
            password=os.getenv('REDIS_PASSWORD')
        )

    def validate_stock_data(self, symbol, price, timestamp=None):
        if not isinstance(symbol, str) or len(symbol) > 10:
            raise ValueError("Invalid symbol")
        
        if not isinstance(price, (int, float)) or price < 0:
            raise ValueError("Invalid price")
        
        if timestamp is not None and not isinstance(timestamp, (int, float)):
            raise ValueError("Invalid timestamp")
        
        return True
    
    def get(self, key):
        try:
            value = self.redis.get(key)
            return value.decode('utf-8') if value else None
        except redis.RedisError as e:
            logger.error(f"Redis error getting key {key}: {e}")
            return None

    def set(self, key, value):
        try:
            self.redis.set(key, value)
        except redis.RedisError as e:
            logger.error(f"Redis error setting key {key}: {e}")

    # def symbol_exists(self, symbol):
    #     try:
    #         return self.redis.exists(f"stock:{symbol}")
    #     except redis.RedisError as e:
    #         logger.error(f"Redis error checking if symbol {symbol} exists: {e}")
    #         return False
    
    def set_stock_price(self, symbol, price, timestamp=None):
        try:
            if self.validate_stock_data(symbol, price, timestamp):
                if timestamp is None:
                    timestamp = time.time()
                self.redis.zadd(f"stock:{symbol}", {price: timestamp})
                logger.info(f"Stock price set for {symbol}: {price} at {timestamp}")
        except ValueError as e:
            logger.error(f"Data validation failed for {symbol}: {e}")
        except redis.RedisError as e:
            logger.error(f"Redis error setting stock price for {symbol}: {e}")

    def get_stock_prices(self, symbol, start_time='-inf', end_time='+inf', limit=10):
        try:
            prices = self.redis.zrevrangebyscore(f"stock:{symbol}", end_time, start_time, withscores=True, start=0, num=limit)
            return [(float(price), timestamp) for price, timestamp in prices]
        except redis.RedisError as e:
            logger.error(f"Redis error getting stock prices for {symbol}: {e}")
            return []

    def get_latest_stock_price(self, symbol):
        prices = self.get_stock_prices(symbol, limit=1)
        return prices[0] if prices else (None, None)

    def get_all_symbols(self):
        try:
            keys = self.redis.keys("stock:*")
            return [key.decode().split(':')[1] for key in keys]
        except redis.RedisError as e:
            logger.error(f"Redis error getting all symbols: {e}")
            return []

    # def cache_stock_list(self, exchange, stocks):
    #     try:
    #         self.redis.set(f"stocklist:{exchange}", json.dumps(stocks))
    #         logger.info(f"Cached stock list for exchange {exchange}")
    #     except redis.RedisError as e:
    #         logger.error(f"Redis error caching stock list for {exchange}: {e}")

    def get_cached_stock_list(self, exchange):
        try:
            cached = self.redis.get(f"stocklist:{exchange}")
            if cached:
                return json.loads(cached)
            return None
        except redis.RedisError as e:
            logger.error(f"Redis error getting cached stock list for {exchange}: {e}")
            return None

    def get_total_stored_stocks(self):
        return len(self.get_all_symbols())

    # def get_all_keys(self, pattern):
    #     try:
    #         return self.redis.keys(pattern)
    #     except redis.RedisError as e:
    #         logger.error(f"Redis error getting keys with pattern {pattern}: {e}")
    #         return []

    # def delete_keys(self, keys):
    #     try:
    #         return self.redis.delete(*keys)
    #     except redis.RedisError as e:
    #         logger.error(f"Redis error deleting keys: {e}")
    #         return 0
    
    def delete_stock_data(self, symbol):
        try:
            keys_to_delete = [f"stock:{symbol}", f"stock:{symbol}:price"]
            deleted = self.redis.delete(*keys_to_delete)
            return deleted > 0
        except Exception as e:
            logger.error(f"Error deleting data for {symbol}: {e}")
            return False