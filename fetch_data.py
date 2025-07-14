import yfinance as yf
import pandas as pd
from typing import List
import time

from config import *

# Configure logging
logger = setup_logging()

def get_sp500_symbols() -> List[str]:
    """
    Fetch S&P 500 symbols as a starting point for US stocks.
    
    Returns:
        List[str]: List of stock symbols
    """
    try:
        # Get S&P 500 symbols from Wikipedia
        tables = pd.read_html(SP500_WIKIPEDIA_URL)
        sp500_df = tables[0]
        symbols = sp500_df['Symbol'].tolist()
        
        # Clean symbols (remove dots and other special characters)
        symbols = [symbol.replace('.', '-') for symbol in symbols]
        
        logger.info(f"Retrieved {len(symbols)} S&P 500 symbols")
        return symbols
        
    except Exception as e:
        logger.error(f"Failed to fetch S&P 500 symbols: {e}")
        # Fallback to a manually curated list of top stocks
        return FALLBACK_SYMBOLS
    
def fetch_stock_data(symbols: List[str], start_date: str, end_date: str) -> pd.DataFrame:
    """
    Fetch historical stock data for given symbols.
    
    Args:
        symbols (List[str]): List of stock symbols
        start_date (str): Start date in YYYY-MM-DD format
        end_date (str): End date in YYYY-MM-DD format
        
    Returns:
        pd.DataFrame: Historical stock data
    """
    logger.info(f"Starting data fetch for {len(symbols)} symbols from {start_date} to {end_date}")
    all_data = []
    successful_symbols = []
    failed_symbols = []
    
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i+batch_size]
        batch_num = i//batch_size + 1
        total_batches = (len(symbols)-1)//batch_size + 1
        
        logger.info(f"Processing batch {batch_num}/{total_batches}: {batch}")
        
        # Process each symbol individually to avoid batch failures
        for symbol in batch:
            try:
                logger.debug(f"Fetching data for {symbol}")
                
                # Fetch data for individual symbol
                ticker = yf.Ticker(symbol)
                
                # Get historical data
                hist_data = ticker.history(start=start_date, end=end_date, auto_adjust=True, prepost=True)
                
                if hist_data.empty:
                    logger.warning(f"No historical data found for {symbol}")
                    failed_symbols.append(symbol)
                    continue
                
                # Get company info for market cap calculation
                try:
                    info = ticker.info
                    shares_outstanding = info.get('sharesOutstanding', 0)
                    if shares_outstanding == 0:
                        # Try alternative field names
                        shares_outstanding = info.get('impliedSharesOutstanding', 0)
                        if shares_outstanding == 0:
                            shares_outstanding = info.get('floatShares', 0)
                    
                    logger.debug(f"Shares outstanding for {symbol}: {shares_outstanding:,}")
                    
                except Exception as e:
                    logger.warning(f"Could not get company info for {symbol}: {e}")
                    # Use market cap from info if available
                    market_cap = info.get('marketCap', 0) if 'info' in locals() else 0
                    if market_cap > 0:
                        shares_outstanding = market_cap / hist_data['Close'].iloc[-1]
                    else:
                        shares_outstanding = 0
                
                if shares_outstanding > 0:
                    # Calculate market cap for each day
                    hist_data['symbol'] = symbol
                    hist_data['shares_outstanding'] = shares_outstanding
                    hist_data['market_cap'] = hist_data['Close'] * shares_outstanding
                    hist_data.reset_index(inplace=True)
                    
                    # Convert timezone-aware datetime to naive datetime (date only)
                    hist_data['Date'] = pd.to_datetime(hist_data['Date']).dt.date
                    
                    all_data.append(hist_data)
                    successful_symbols.append(symbol)
                    
                    logger.debug(f"Successfully processed {symbol}: {len(hist_data)} days of data")
                else:
                    logger.warning(f"No valid shares outstanding data for {symbol}")
                    failed_symbols.append(symbol)
                    
            except Exception as e:
                logger.error(f"Failed to fetch data for {symbol}: {e}")
                failed_symbols.append(symbol)
                continue
        
        # Rate limiting to avoid API throttling
        logger.debug(f"Batch {batch_num} completed. Waiting 2 seconds...")
        time.sleep(API_RATE_LIMIT_DELAY)
    
    logger.info(f"Data fetch completed. Success: {len(successful_symbols)}, Failed: {len(failed_symbols)}")
    
    if failed_symbols:
        logger.warning(f"Failed symbols: {failed_symbols}")
    
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        logger.info(f"Final dataset: {len(combined_df)} rows, {len(combined_df['symbol'].unique())} unique symbols")
        logger.info(f"Date range in data: {combined_df['Date'].min()} to {combined_df['Date'].max()}")
        return combined_df
    else:
        logger.error("No data was successfully fetched")
        return pd.DataFrame()
