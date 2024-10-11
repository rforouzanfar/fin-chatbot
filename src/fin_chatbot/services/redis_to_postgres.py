import os
import time
import logging
from datetime import datetime
from dotenv import load_dotenv
from postgres_processor import PostgresProcessor

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def transfer_to_postgres():
    start_time = time.time()
    processor = PostgresProcessor()
    
    try:
        logger.info("Starting daily transfer of stock prices from Redis to PostgreSQL")
        
        # Run the daily update process
        processor.run_daily_update()
        
        # Get some statistics
        total_records = processor.postgres_manager.get_total_records()
        today = datetime.now().date()
        inserted_today = processor.postgres_manager.get_inserted_count(today)
        
        end_time = time.time()
        duration = end_time - start_time
        
        logger.info(f"Daily transfer completed in {duration:.2f} seconds")
        logger.info(f"Total records in PostgreSQL: {total_records}")
        logger.info(f"Records inserted/updated today: {inserted_today}")
        
        return True
    except Exception as e:
        logger.error(f"An error occurred during the transfer process: {e}")
        return False
    finally:
        # Ensure the database connection is closed
        processor.postgres_manager.close_connection()

def main():
    try:
        success = transfer_to_postgres()
        if success:
            logger.info("Daily Redis to PostgreSQL transfer completed successfully")
        else:
            logger.error("Daily Redis to PostgreSQL transfer failed")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()