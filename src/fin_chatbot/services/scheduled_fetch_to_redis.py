import asyncio
import time
import logging
from datetime import datetime
import os
from dotenv import load_dotenv
from redis_manager import RedisManager
from redis_processor import RedisProcessor
from prometheus_client import start_http_server, Counter, Gauge

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Define Prometheus metrics
STOCKS_FETCHED = Counter('stocks_fetched_total', 'Number of stocks fetched')
STOCKS_FAILED = Counter('stocks_failed_total', 'Number of stocks failed to fetch')
STOCKS_STORED = Gauge('stocks_stored_total', 'Total number of stocks stored in Redis')

FETCH_ALL_STOCKS = os.getenv('FETCH_ALL_STOCKS', 'False').lower() == 'true'
API_CALLS_PER_MINUTE = 60  # Finnhub free tier limit
FETCH_INTERVAL_MINUTES = float(os.getenv('FETCH_INTERVAL_MINUTES', 15))

async def fetch_stocks(redis_processor, max_calls):
    common_total, common_fetched, common_failed, common_stored = await redis_processor.process_common_stocks(max_calls=max_calls)
    
    if FETCH_ALL_STOCKS and max_calls > common_fetched + common_failed:
        batch = redis_processor.get_next_batch_of_stocks()
        all_total, all_fetched, all_failed = await redis_processor.process_stocks_async(
            batch,
            max_calls=max_calls - (common_fetched + common_failed)
        )
    else:
        all_total, all_fetched, all_failed = 0, 0, 0
    
    return common_total, common_fetched, common_failed, common_stored, all_total, all_fetched, all_failed

async def main():
    # Start Prometheus metrics server
    start_http_server(8000)

    redis_manager = RedisManager()
    redis_processor = RedisProcessor(redis_manager, verify_ssl=False)

    api_calls_this_minute = 0
    last_reset_time = time.time()

    while True:
        current_time = time.time()
        
        # Reset the counter if a minute has passed
        if current_time - last_reset_time >= 60:
            api_calls_this_minute = 0
            last_reset_time = current_time

        logger.info(f"Starting scheduled fetch (every {FETCH_INTERVAL_MINUTES} minutes)")

        try:
            common_total, common_fetched, common_failed, common_stored, all_total, all_fetched, all_failed = await fetch_stocks(
                redis_processor, 
                API_CALLS_PER_MINUTE - api_calls_this_minute
            )
            
            api_calls_this_minute += common_fetched + common_failed + all_fetched + all_failed
            all_stored = redis_manager.get_total_stored_stocks()

            # Update Prometheus metrics
            STOCKS_FETCHED.inc(common_fetched + all_fetched)
            STOCKS_FAILED.inc(common_failed + all_failed)
            STOCKS_STORED.set(all_stored)

            logger.info("Fetch Summary:")
            logger.info(f"Common Stocks - Total: {common_total}, Fetched: {common_fetched}, Failed: {common_failed}, Stored: {common_stored}")
            if FETCH_ALL_STOCKS:
                logger.info(f"All Stocks Batch - Total: {all_total}, Fetched: {all_fetched}, Failed: {all_failed}")
            logger.info(f"Total stocks stored in Redis: {all_stored}")
            logger.info(f"API calls this minute: {api_calls_this_minute}")

        except Exception as e:
            logger.error(f"An error occurred during the fetch process: {e}", exc_info=True)

        # Calculate sleep time to respect the fetch interval
        sleep_time = max(0, FETCH_INTERVAL_MINUTES * 60 - (time.time() - current_time))
        await asyncio.sleep(sleep_time)

if __name__ == "__main__":
    asyncio.run(main())