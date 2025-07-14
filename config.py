"""
Configuration and Constants Module
"""

import logging, os

# Output file names
DEFAULT_DB_PATH = "output/stock_data.db"
DEFAULT_OUTPUT_PATH = "output/index_analysis.xlsx"
LOG_FILE_PATH = "output/index_tracker.log"

# API Configurations
batch_size = 10
API_RATE_LIMIT_DELAY = 2

# Index Configurations
INDEX_SIZE = 100
BASE_INDEX_VALUE = 100.0
ANNUALIZATION_FACTOR = 252

# Fallback Stock Symbols for backup
FALLBACK_SYMBOLS = [
    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'TSLA', 'META', 'BRK-B',
    'UNH', 'JNJ', 'JPM', 'V', 'PG', 'MA', 'HD', 'CVX', 'LLY', 'ABBV',
    'BAC', 'KO', 'AVGO', 'PEP', 'COST', 'TMO', 'WMT', 'DIS', 'ABT',
    'ACN', 'VZ', 'ADBE', 'DHR', 'NFLX', 'TXN', 'NKE', 'RTX', 'QCOM',
    'CRM', 'NEE', 'ORCL', 'INTC', 'AMD', 'IBM', 'GS', 'INTU', 'CAT',
    'HON', 'SPGI', 'BKNG', 'LOW', 'BA', 'SBUX', 'GILD', 'AXP', 'BLK',
    'SYK', 'MDLZ', 'ADP', 'ISRG', 'TGT', 'LRCX', 'ADI', 'VRTX', 'PLD',
    'MU', 'ANTM', 'CI', 'SO', 'ZTS', 'MMM', 'FIS', 'DUK', 'CSX', 'BSX',
    'EQIX', 'CL', 'ITW', 'NSC', 'AON', 'CME', 'WM', 'SHW', 'GD', 'USB',
    'PNC', 'MCO', 'COP', 'EMR', 'WELL', 'ICE', 'KLAC', 'APD', 'RACE',
    'NOC', 'F', 'GM', 'PYPL', 'ATVI', 'UBER', 'ABNB', 'SNOW', 'COIN'
]

# S&P 500 Data Source
SP500_WIKIPEDIA_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

# Logging Configuration
def setup_logging():
    os.makedirs(os.path.dirname(LOG_FILE_PATH), exist_ok=True)
    """Configure logging for the application."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(LOG_FILE_PATH),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

# Database Schema - Change here for modifying schema
DATABASE_SCHEMA = {
    'stocks': """
        CREATE TABLE IF NOT EXISTS stocks (
            symbol TEXT PRIMARY KEY,
            company_name TEXT,
            sector TEXT,
            market_cap REAL,
            last_updated DATE
        )
    """,
    'daily_prices': """
        CREATE TABLE IF NOT EXISTS daily_prices (
            symbol TEXT,
            date DATE,
            open_price REAL,
            high_price REAL,
            low_price REAL,
            close_price REAL,
            volume INTEGER,
            market_cap REAL,
            shares_outstanding REAL,
            PRIMARY KEY (symbol, date),
            FOREIGN KEY (symbol) REFERENCES stocks(symbol)
        )
    """,
    'index_composition': """
        CREATE TABLE IF NOT EXISTS index_composition (
            date DATE,
            symbol TEXT,
            rank_by_market_cap INTEGER,
            market_cap REAL,
            weight REAL,
            PRIMARY KEY (date, symbol)
        )
    """,
    'index_performance': """
        CREATE TABLE IF NOT EXISTS index_performance (
            date DATE PRIMARY KEY,
            index_value REAL,
            daily_return REAL,
            cumulative_return REAL,
            num_constituents INTEGER
        )
    """
}