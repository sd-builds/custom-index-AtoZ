import pandas as pd

from config import *
from fetch_data import *

# Configure logging
logger = setup_logging()

def calculate_daily_index(conn, start_date: str, end_date: str):
    """Calculate the equal-weighted index for each trading day."""
    try:
        logger.info(f"Calculating daily index from {start_date} to {end_date}")
        
        # Get all trading dates
        query = """
            SELECT DISTINCT date 
            FROM daily_prices 
            WHERE date BETWEEN ? AND ?
            ORDER BY date
        """
        dates_df = pd.read_sql_query(query, conn, params=(start_date, end_date))
        
        if dates_df.empty:
            logger.error("No trading dates found in the specified range")
            return
        
        logger.info(f"Found {len(dates_df)} trading days")
        
        index_values = []
        previous_index_value = BASE_INDEX_VALUE
        
        for idx, date in enumerate(dates_df['date']):
            logger.debug(f"Processing date {idx+1}/{len(dates_df)}: {date}")
            
            # Get top 100 stocks by market cap for this date
            top_stocks_query = """
                SELECT symbol, market_cap, close_price
                FROM daily_prices
                WHERE date = ?
                AND market_cap > 0
                AND close_price > 0
                ORDER BY market_cap DESC
                LIMIT 100
            """
            top_stocks = pd.read_sql_query(top_stocks_query, conn, params=(date,))
            
            if len(top_stocks) == 0:
                logger.warning(f"No valid stocks found for date {date}")
                continue
            
            logger.debug(f"Found {len(top_stocks)} valid stocks for {date}")
            
            # Calculate equal weights
            weight = 1.0 / len(top_stocks)
            top_stocks['weight'] = weight
            top_stocks['rank_by_market_cap'] = range(1, len(top_stocks) + 1)
            
            # Clear existing composition for this date
            conn.execute("DELETE FROM index_composition WHERE date = ?", (date,))
            
            # Store composition
            composition_data = top_stocks[['symbol', 'rank_by_market_cap', 'market_cap', 'weight']].copy()
            composition_data['date'] = date
            composition_data.to_sql('index_composition', conn, if_exists='append', index=False)
            
            # Calculate index performance
            if len(index_values) == 0:
                # First day - set base value
                index_value = BASE_INDEX_VALUE
                daily_return = 0.0
                cumulative_return = 0.0
                logger.debug(f"First day - setting base index value: {index_value}")
            else:
                # Calculate return based on previous day's composition
                prev_date = dates_df['date'].iloc[len(index_values) - 1]
                
                logger.debug(f"Calculating return from {prev_date} to {date}")
                
                # Get previous day's composition
                prev_composition_query = """
                    SELECT symbol, weight
                    FROM index_composition
                    WHERE date = ?
                """
                prev_composition = pd.read_sql_query(prev_composition_query, conn, params=(prev_date,))
                
                if prev_composition.empty:
                    logger.warning(f"No previous composition found for {prev_date}")
                    index_value = previous_index_value
                    daily_return = 0.0
                else:
                    # Calculate returns for overlapping stocks
                    total_return = 0.0
                    valid_weights = 0.0
                    
                    for _, row in prev_composition.iterrows():
                        symbol = row['symbol']
                        weight = row['weight']
                        
                        # Get current and previous prices
                        current_price_query = """
                            SELECT close_price FROM daily_prices
                            WHERE symbol = ? AND date = ?
                        """
                        prev_price_query = """
                            SELECT close_price FROM daily_prices
                            WHERE symbol = ? AND date = ?
                        """
                        
                        current_price = pd.read_sql_query(current_price_query, conn, params=(symbol, date))
                        prev_price = pd.read_sql_query(prev_price_query, conn, params=(symbol, prev_date))
                        
                        if not current_price.empty and not prev_price.empty:
                            curr_price_val = current_price.iloc[0]['close_price']
                            prev_price_val = prev_price.iloc[0]['close_price']
                            
                            if prev_price_val > 0:
                                stock_return = (curr_price_val / prev_price_val) - 1
                                total_return += weight * stock_return
                                valid_weights += weight
                                logger.debug(f"{symbol}: {prev_price_val:.2f} -> {curr_price_val:.2f} ({stock_return*100:.2f}%)")
                    
                    # Normalize return by valid weights
                    if valid_weights > 0:
                        daily_return = total_return / valid_weights
                    else:
                        daily_return = 0.0
                    
                    index_value = previous_index_value * (1 + daily_return)
                    logger.debug(f"Daily return: {daily_return*100:.4f}%, Index value: {index_value:.2f}")
                
                cumulative_return = (index_value / BASE_INDEX_VALUE) - 1
            
            # Clear existing performance for this date
            conn.execute("DELETE FROM index_performance WHERE date = ?", (date,))
            
            # Store index performance
            performance_data = {
                'date': date,
                'index_value': index_value,
                'daily_return': daily_return,
                'cumulative_return': cumulative_return,
                'num_constituents': len(top_stocks)
            }
            
            pd.DataFrame([performance_data]).to_sql('index_performance', conn, if_exists='append', index=False)
            
            index_values.append(index_value)
            previous_index_value = index_value
            
            if idx % 5 == 0 or idx == len(dates_df) - 1:
                logger.info(f"Processed {idx+1}/{len(dates_df)} days. Latest index value: {index_value:.2f}")
        
        conn.commit()
        logger.info(f"Index calculation completed. Final index value: {index_value:.2f}")
        
    except Exception as e:
        logger.error(f"Failed to calculate daily index: {e}")
        raise
