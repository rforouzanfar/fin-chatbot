from redis_manager import RedisManager
from redis_processor import RedisProcessor
import time
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    redis_manager = RedisManager()
    redis_processor = RedisProcessor(redis_manager, verify_ssl=False)

    while True:
        start_time = time.time()

        logger.info(f"Starting scheduled fetch (every {redis_processor.fetch_interval_hours} hours)")

        # This will fetch and store data for all available stocks
        total_stocks, fetched_count, failed_count, stored_count = redis_processor.process_all_available_stocks()

        end_time = time.time()
        duration = end_time - start_time

        logger.info("Fetch Summary:")
        logger.info(f"Total stocks available via Finnhub API: {total_stocks}")
        logger.info(f"Stocks successfully fetched in this run: {fetched_count}")
        logger.info(f"Stocks failed to fetch in this run: {failed_count}")
        logger.info(f"Total stocks stored in Redis: {stored_count}")
        logger.info(f"Time taken for this fetch: {duration:.2f} seconds")

        if total_stocks > 0:
            coverage = (stored_count / total_stocks) * 100
            logger.info(f"Coverage: {coverage:.2f}% of available stocks are stored in Redis")

        # Calculate sleep time
        sleep_time = redis_processor.get_fetch_interval_seconds() - (time.time() - start_time)
        if sleep_time > 0:
            logger.info(f"Sleeping for {sleep_time:.2f} seconds until next fetch")
            time.sleep(sleep_time)
        else:
            logger.warning("Fetch took longer than the specified interval. Starting next fetch immediately.")

if __name__ == "__main__":
    main()