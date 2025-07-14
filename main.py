
"""
Equal-Weighted Custom Index Tracker
===================================

This module is the entry point to the implementation of a comprehensive system for tracking an equal-weighted 
custom index of the top 100 US stocks by market capitalization.
"""

import pandas as pd
from datetime import datetime, timedelta
import os, shutil

from config import *
from fetch_data import *
from database_operations import DatabaseHandler
from index_calculation import *
from end_output import *

# Configure logging
logger = setup_logging()

class IndexTracker:
    """
    Main class for tracking an equal-weighted custom index of top 100 US stocks.
    
    This class calls everything from fethcing the data, storing it, index construction, and performance tracking and output preparon.
    """  
    def run_full_analysis(self, days_back):
        try:
            self.db = DatabaseHandler(db_path=DEFAULT_DB_PATH)
            # Calculate date range
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days_back)
            
            start_date_str = start_date.strftime('%Y-%m-%d')
            end_date_str = end_date.strftime('%Y-%m-%d')
            
            logger.info("=" * 60)
            logger.info("STARTING EQUAL-WEIGHTED INDEX ANALYSIS")
            logger.info("=" * 60)
            logger.info(f"Analysis period: {start_date_str} to {end_date_str}")
            logger.info(f"Days back: {days_back}")
            
            # Step 1: Get stock symbols
            logger.info("\n---STEP 1: GETTING STOCK SYMBOLS---")
            symbols = get_sp500_symbols()
            logger.info(f"Total symbols to process: {len(symbols)}")
            
            # Step 2: Fetch stock data
            logger.info("\n---STEP 2: FETCHING STOCK DATA---")
            stock_data = fetch_stock_data(symbols[:100], start_date_str, end_date_str)  # Limit to first 100 for testing
            
            if stock_data.empty:
                logger.error("No stock data was fetched. Analysis cannot proceed.")
                return
            
            logger.info(f"Stock data fetched successfully: {len(stock_data)} rows")
            
            # Step 3: Store data
            logger.info("\n---STEP 3: STORING DATA IN DATABASE---")
            self.db.store_stock_data(stock_data)
            
            # Verify data storage
            stock_count = pd.read_sql_query("SELECT COUNT(DISTINCT symbol) as count FROM daily_prices", self.db.conn)
            date_count = pd.read_sql_query("SELECT COUNT(DISTINCT date) as count FROM daily_prices", self.db.conn)
            logger.info(f"Data verification - Stocks: {stock_count.iloc[0]['count']}, Trading days: {date_count.iloc[0]['count']}")
            
            # Step 4: Calculate index
            logger.info("\n---STEP 4: CALCULATING INDEX---")
            calculate_daily_index(self.db.conn, start_date_str, end_date_str)
            
            # Step 5: Export results
            logger.info("\n---STEP 5: EXPORTING RESULTS---")
            export_to_excel(self.db.conn)
            
            logger.info("=" * 60)
            logger.info("ANALYSIS COMPLETED SUCCESSFULLY!")
            logger.info("=" * 60)
            
            # Print summary
            final_performance = pd.read_sql_query("SELECT * FROM index_performance ORDER BY date DESC LIMIT 1", self.db.conn)
            if not final_performance.empty:
                final_return = final_performance.iloc[0]['cumulative_return'] * 100
                logger.info(f"Final index return: {final_return:.2f}%")
            
        except Exception as e:
            logger.error(f"Full analysis failed: {e}")
            logger.error("Stack trace:", exc_info=True)
            raise
    
        self.db.close()

def main():
    """Main function to run the index tracker."""
    try:
        output_dir = "output"
        os.makedirs(output_dir, exist_ok=True)

        tracker = IndexTracker()
        tracker.run_full_analysis(days_back=30)

        for root, dirs, files in os.walk('.'):
            if '__pycache__' in dirs:
                pycache_path = os.path.join(root, '__pycache__')
                shutil.rmtree(pycache_path)
            
    except Exception as e:
        logger.error(f"Application failed: {e}")
        raise

if __name__ == "__main__":
    main()