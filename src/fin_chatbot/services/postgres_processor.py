import os
from datetime import datetime, timedelta
import logging
from dotenv import load_dotenv
from postgres_manager import PostgresManager
from redis_manager import RedisManager
from redis_processor import RedisProcessor

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PostgresProcessor:
    def __init__(self):
        self.postgres_manager = PostgresManager()
        self.redis_manager = RedisManager()
        self.redis_processor = RedisProcessor(self.redis_manager)
        self.batch_size = 30  # Adjusted for Finnhub free tier

    def initialize_database(self):
        try:
            self.postgres_manager.create_pool()
            self.postgres_manager.create_database()
            self.postgres_manager.create_tables()
            self.postgres_manager.create_indexes()
            logger.info("Database initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            raise

    def save_daily_closing_prices(self):
        symbols = self.redis_processor.get_available_symbols()
        today = datetime.now().date()
        inserted_count = 0
        error_count = 0
        batch = []

        for symbol in symbols:
            try:
                price, timestamp = self.redis_manager.get_latest_stock_price(symbol)
                if price is not None and timestamp is not None:
                    date = datetime.fromtimestamp(timestamp).date()
                    if date == today:
                        batch.append((symbol, date, price))
                        if len(batch) >= self.batch_size:
                            self.postgres_manager.batch_insert_stock_prices(batch)
                            inserted_count += len(batch)
                            batch = []
                    else:
                        logger.warning(f"Skipping {symbol}: Latest price is not from today")
                else:
                    logger.warning(f"No price data found for {symbol}")
            except Exception as e:
                logger.error(f"Error processing {symbol}: {e}")
                error_count += 1

        # Insert any remaining records in the batch
        if batch:
            self.postgres_manager.batch_insert_stock_prices(batch)
            inserted_count += len(batch)

        logger.info(f"Daily update completed. Inserted/updated {inserted_count} records. Errors: {error_count}")
        return inserted_count, error_count

    def check_missing_data(self, start_date, end_date):
        symbols = self.redis_processor.get_available_symbols()
        missing_data = []

        for symbol in symbols:
            dates = set(date for date, _ in self.postgres_manager.get_stock_prices(symbol, start_date, end_date))
            all_dates = set(start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1))
            missing = all_dates - dates
            if missing:
                missing_data.append((symbol, list(missing)))

        return missing_data

    def fill_missing_data(self, missing_data):
        filled_count = 0
        for symbol, dates in missing_data:
            for date in dates:
                price = self.fetch_historical_price(symbol, date)
                if price is not None:
                    self.postgres_manager.batch_insert_stock_prices([(symbol, date, price)])
                    filled_count += 1
                else:
                    logger.warning(f"Could not fetch price for {symbol} on {date}")
        
        logger.info(f"Filled {filled_count} missing data points")
        return filled_count

    def fetch_historical_price(self, symbol, date):
        # Implement this method to fetch historical price data
        # This could involve using an external API or your Redis cache
        logger.warning(f"fetch_historical_price not implemented. Couldn't fetch price for {symbol} on {date}")
        return None

    def update_metrics(self):
        logger.info("Updating metrics for all symbols")
        self.postgres_manager.update_all_metrics()

    def run_daily_update(self):
        logger.info("Starting daily update of stock prices in PostgreSQL")
        try:
            self.initialize_database()
            inserted, errors = self.save_daily_closing_prices()
            total_records = self.postgres_manager.get_total_records()
            logger.info(f"Daily update completed. Total records in database: {total_records}")
            
            # Check for missing data in the last 7 days
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=7)
            missing_data = self.check_missing_data(start_date, end_date)
            if missing_data:
                logger.info(f"Found {len(missing_data)} symbols with missing data in the last 7 days")
                filled_count = self.fill_missing_data(missing_data)
                logger.info(f"Filled {filled_count} missing data points")
            
            # Update metrics
            self.update_metrics()
            
            # Cleanup old data
            deleted_count = self.postgres_manager.cleanup_old_data()
            logger.info(f"Cleaned up {deleted_count} old records")

        except Exception as e:
            logger.error(f"Error during daily update: {e}")
        finally:
            self.postgres_manager.close_pool()

if __name__ == "__main__":
    processor = PostgresProcessor()
    processor.run_daily_update()