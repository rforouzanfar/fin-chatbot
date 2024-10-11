import os
from datetime import datetime, date
import logging
from dotenv import load_dotenv
from redis_manager import RedisManager
from postgres_manager import PostgresManager

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def transfer_to_postgres(redis_manager, postgres_manager):
    logger.info("Transferring today's latest data from Redis to PostgreSQL...")
    symbols = redis_manager.get_all_symbols()
    today = date.today()
    transferred_count = 0

    for symbol in symbols:
        prices = redis_manager.get_stock_prices(symbol, limit=1)  # Get only the latest price
        if prices:
            price, timestamp = prices[0]
            price_date = datetime.fromtimestamp(timestamp).date()
            if price_date == today:
                try:
                    postgres_manager.insert_stock_price(symbol, price_date, price)
                    transferred_count += 1
                    logger.info(f"Transferred {symbol}: {price} on {price_date}")
                except Exception as e:
                    logger.error(f"Error inserting data for {symbol} on {price_date}: {e}")

    logger.info(f"Transferred {transferred_count} records to PostgreSQL")

def main():
    redis_manager = RedisManager()
    postgres_manager = PostgresManager()

    try:
        postgres_manager.connect()
        postgres_manager.create_database()
        postgres_manager.create_table()
        transfer_to_postgres(redis_manager, postgres_manager)
    except Exception as e:
        logger.error(f"Failed to transfer data to PostgreSQL: {e}")
    finally:
        postgres_manager.close_connection()

if __name__ == "__main__":
    main()