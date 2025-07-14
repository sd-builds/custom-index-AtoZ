import yfinance as yf
import pandas as pd
import sqlite3
from typing import List, Dict, Tuple, Optional
import os

from config import *

# Configure logging
logger = setup_logging()

class DatabaseHandler:
    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = None
        self.setup_database()

    def setup_database(self):
        """Set up the SQLite database with required tables."""
        try:
            if os.path.exists(self.db_path):
                os.remove(self.db_path)
                logger.warning(f"Existing database at {self.db_path} deleted.")
        
            self.conn = sqlite3.connect(DEFAULT_DB_PATH)
            self.conn.execute("PRAGMA foreign_keys = ON")
            
            # Create all tables from schema in config
            for table_name, create_sql in DATABASE_SCHEMA.items():
                self.conn.execute(create_sql)
            self.conn.commit()
            logger.info("Database setup completed successfully\n")
            
        except Exception as e:
            logger.error(f"Database setup failed: {e}")
            raise

    def store_stock_data(self, df: pd.DataFrame):
        """Store stock data in the database."""
        try:
            logger.info(f"Storing data for {len(df)} rows across {len(df['symbol'].unique())} symbols")
            logger.debug(f"DataFrame columns: {df.columns.tolist()}")
            
            # Store stock metadata
            stocks_df = df.groupby('symbol').agg({
                'market_cap': 'last',
                'Date': 'max'
            }).reset_index()
            stocks_df.columns = ['symbol', 'market_cap', 'last_updated']
            stocks_df['company_name'] = stocks_df['symbol']
            stocks_df['sector'] = 'Unknown'
            
            logger.debug(f"Storing {len(stocks_df)} stock records")
            stocks_df.to_sql('stocks', self.conn, if_exists='replace', index=False)
            
            # Store daily prices - handle flexible column structure
            price_df = df.copy()
            
            # Map columns dynamically based on what's available
            expected_columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
            available_columns = [col for col in expected_columns if col in price_df.columns]
            
            logger.debug(f"Available price columns: {available_columns}")
            
            # Select and rename columns
            if len(available_columns) >= 5:
                price_df = price_df[available_columns + ['symbol', 'shares_outstanding', 'market_cap']]
                
                # Rename columns to match database schema
                column_mapping = {
                    'Date': 'date',
                    'Open': 'open_price',
                    'High': 'high_price', 
                    'Low': 'low_price',
                    'Close': 'close_price',
                    'Volume': 'volume'
                }
                
                price_df = price_df.rename(columns=column_mapping)
                
                # Ensure volume column exists
                if 'volume' not in price_df.columns:
                    price_df['volume'] = 0
                
                # Reorder columns
                price_df = price_df[['symbol', 'date', 'open_price', 'high_price', 'low_price', 'close_price', 'volume', 'market_cap', 'shares_outstanding']]
                
                logger.debug(f"Storing {len(price_df)} price records")
                price_df.to_sql('daily_prices', self.conn, if_exists='replace', index=False)
                
                self.conn.commit()
                logger.info("Stock data stored successfully")
                
                # Log some sample data for verification
                sample_data = price_df.head(3)
                logger.debug(f"Sample stored data:\n{sample_data}")
                
            else:
                logger.error(f"Insufficient columns for price data: {available_columns}")
                raise ValueError("Missing required price columns")
            
        except Exception as e:
            logger.error(f"Failed to store stock data: {e}")
            logger.error(f"DataFrame shape: {df.shape}")
            logger.error(f"DataFrame columns: {df.columns.tolist()}")
            raise

    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")
