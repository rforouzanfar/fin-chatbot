import os
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from dotenv import load_dotenv
import logging
from datetime import date, timedelta

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PostgresManager:
    def __init__(self):
        self.conn = None
        self.cursor = None
        self.db_params = {
            'dbname': os.getenv('POSTGRES_DB'),
            'user': os.getenv('POSTGRES_USER'),
            'password': os.getenv('POSTGRES_PASSWORD'),
            'host': os.getenv('POSTGRES_HOST'),
            'port': os.getenv('POSTGRES_PORT')
        }

    def create_database(self):
        try:
            conn = psycopg2.connect(
                dbname='postgres',
                user=self.db_params['user'],
                password=self.db_params['password'],
                host=self.db_params['host'],
                port=self.db_params['port']
            )
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = conn.cursor()

            cursor.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s", (self.db_params['dbname'],))
            exists = cursor.fetchone()
            
            if not exists:
                cursor.execute(sql.SQL("CREATE DATABASE {}").format(
                    sql.Identifier(self.db_params['dbname'])
                ))
                logger.info(f"Database '{self.db_params['dbname']}' created successfully")
            else:
                logger.info(f"Database '{self.db_params['dbname']}' already exists")

            cursor.close()
            conn.close()

        except (Exception, psycopg2.Error) as error:
            logger.error(f"Error while creating database: {error}")
            raise

    def connect(self):
        try:
            self.conn = psycopg2.connect(**self.db_params)
            self.cursor = self.conn.cursor()
            logger.info("Connected to PostgreSQL database")
        except (Exception, psycopg2.Error) as error:
            logger.error(f"Error while connecting to PostgreSQL: {error}")
            raise

    def create_table(self):
        try:
            create_table_query = '''
            CREATE TABLE IF NOT EXISTS stock_prices (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(10) NOT NULL,
                date DATE NOT NULL,
                closing_price DECIMAL(10, 2) NOT NULL,
                UNIQUE (symbol, date)
            )
            '''
            self.cursor.execute(create_table_query)
            self.conn.commit()
            logger.info("Table 'stock_prices' created successfully")
        except (Exception, psycopg2.Error) as error:
            logger.error(f"Error creating table: {error}")
            self.conn.rollback()
            raise

    def insert_stock_price(self, symbol, date, closing_price):
        try:
            insert_query = '''
            INSERT INTO stock_prices (symbol, date, closing_price)
            VALUES (%s, %s, %s)
            ON CONFLICT (symbol, date) DO UPDATE
            SET closing_price = EXCLUDED.closing_price
            '''
            self.cursor.execute(insert_query, (symbol, date, closing_price))
            self.conn.commit()
            logger.info(f"Stock price inserted/updated for {symbol} on {date}")
            return True
        except (Exception, psycopg2.Error) as error:
            logger.error(f"Error inserting stock price for {symbol}: {error}")
            self.conn.rollback()
            return False

    def get_stock_prices(self, symbol, start_date, end_date):
        try:
            select_query = '''
            SELECT date, closing_price
            FROM stock_prices
            WHERE symbol = %s AND date BETWEEN %s AND %s
            ORDER BY date
            '''
            self.cursor.execute(select_query, (symbol, start_date, end_date))
            return self.cursor.fetchall()
        except (Exception, psycopg2.Error) as error:
            logger.error(f"Error retrieving stock prices: {error}")
            return []

    def get_inserted_count(self, date):
        try:
            query = "SELECT COUNT(*) FROM stock_prices WHERE date = %s"
            self.cursor.execute(query, (date,))
            return self.cursor.fetchone()[0]
        except (Exception, psycopg2.Error) as error:
            logger.error(f"Error getting inserted count: {error}")
            return 0

    def get_total_records(self):
        try:
            self.cursor.execute("SELECT COUNT(*) FROM stock_prices")
            return self.cursor.fetchone()[0]
        except (Exception, psycopg2.Error) as error:
            logger.error(f"Error getting total records: {error}")
            return 0

    def cleanup_old_data(self):
        try:
            five_years_ago = date.today() - timedelta(days=5*365)
            delete_query = '''
            DELETE FROM stock_prices
            WHERE date < %s
            '''
            self.cursor.execute(delete_query, (five_years_ago,))
            deleted_count = self.cursor.rowcount
            self.conn.commit()
            logger.info(f"Deleted {deleted_count} records older than 5 years")
            return deleted_count
        except (Exception, psycopg2.Error) as error:
            logger.error(f"Error cleaning up old data: {error}")
            self.conn.rollback()
            return 0
    
    def close_connection(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            logger.info("PostgreSQL connection closed")