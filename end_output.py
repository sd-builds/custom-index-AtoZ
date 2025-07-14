import pandas as pd
import numpy as np
import os
from openpyxl.chart import LineChart, Reference

from config import *
from fetch_data import *

def export_to_excel(conn, output_path: str = DEFAULT_OUTPUT_PATH):
    """Export all analysis results to Excel."""
    try:
        if os.path.exists(output_path):
                os.remove(output_path)
                logger.warning(f"Existing output at {output_path} deleted.")
        
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            # Index Performance Sheet
            performance_query = """
                SELECT date, index_value, daily_return * 100 as daily_return_pct, 
                        cumulative_return * 100 as cumulative_return_pct
                FROM index_performance
                ORDER BY date
            """
            performance_df = pd.read_sql_query(performance_query, conn)
            
            # Convert date to date-only format for Excel
            performance_df['date'] = pd.to_datetime(performance_df['date']).dt.date
            
            performance_df.to_excel(writer, sheet_name='index_performance', index=False)

            # Adding graph view for bonus
            workbook = writer.book
            graph_sheet = workbook.create_sheet('Graph')
            
            # Create chart
            chart = LineChart()
            chart.title = "Index Value Over Time"
            chart.x_axis.title = "Date"
            chart.y_axis.title = "Index Value"
            chart.width = 20
            chart.height = 10
            
            # Get data references from the performance sheet
            data_sheet = workbook['index_performance']
            dates = Reference(data_sheet, min_col=1, min_row=2, max_row=len(performance_df)+1)
            values = Reference(data_sheet, min_col=2, min_row=2, max_row=len(performance_df)+1)
            
            chart.add_data(values, titles_from_data=False)
            chart.set_categories(dates)
            
            graph_sheet.add_chart(chart, "A1")
            
            # Daily Composition Sheet (2)
            composition_query = """
                SELECT date, GROUP_CONCAT(symbol) as constituents
                FROM (
                    SELECT date, symbol, rank_by_market_cap
                    FROM index_composition
                    ORDER BY date, rank_by_market_cap
                )
                GROUP BY date
                ORDER BY date
            """
            composition_df = pd.read_sql_query(composition_query, conn)
            
            # Convert date to date-only format for Excel
            composition_df['date'] = pd.to_datetime(composition_df['date']).dt.date
            
            composition_df.to_excel(writer, sheet_name='daily_composition', index=False)
            
            # Composition Changes Sheet (3)
            changes_df = calculate_composition_changes(conn)
            
            # Convert date to date-only format for Excel
            if not changes_df.empty:
                changes_df['date'] = pd.to_datetime(changes_df['date']).dt.date
            
            changes_df.to_excel(writer, sheet_name='composition_changes', index=False)
            
            # Summary Metrics Sheet (4)
            metrics_df = calculate_summary_metrics(conn)
            metrics_df.to_excel(writer, sheet_name='summary_metrics', index=False)
            
        logger.info(f"Excel file exported successfully to {output_path}\n")
        
    except Exception as e:
        logger.error(f"Failed to export to Excel: {e}")
        raise

def calculate_composition_changes(conn) -> pd.DataFrame:
    """Calculate daily composition changes with improved logic."""
    try:
        logger.info("Calculating composition changes...")
        
        # Get all dates with composition data
        dates_query = """
            SELECT DISTINCT date FROM index_composition 
            ORDER BY date
        """
        dates_df = pd.read_sql_query(dates_query, conn)
        
        if len(dates_df) < 2:
            logger.warning("Need at least 2 days of data to calculate composition changes")
            return pd.DataFrame(columns=['date', 'tickers_added', 'tickers_removed', 'num_added', 'num_removed', 'change_type'])
        
        dates = dates_df['date'].tolist()
        changes_data = []
        
        logger.info(f"Analyzing composition changes across {len(dates)} trading days")
        
        for i in range(1, len(dates)):
            current_date = dates[i]
            previous_date = dates[i-1]
            
            logger.debug(f"Comparing {previous_date} vs {current_date}")
            
            # Get current and previous compositions
            current_query = """
                SELECT symbol FROM index_composition 
                WHERE date = ? 
                ORDER BY rank_by_market_cap
            """
            previous_query = """
                SELECT symbol FROM index_composition 
                WHERE date = ? 
                ORDER BY rank_by_market_cap
            """
            
            current_symbols = set(pd.read_sql_query(current_query, conn, params=(current_date,))['symbol'].tolist())
            previous_symbols = set(pd.read_sql_query(previous_query, conn, params=(previous_date,))['symbol'].tolist())
            
            # Calculate changes
            added = current_symbols - previous_symbols
            removed = previous_symbols - current_symbols
            
            logger.debug(f"Date {current_date}: Added {len(added)}, Removed {len(removed)}")
            
            # Record the change
            changes_data.append({
                'date': current_date,
                'tickers_added': ', '.join(sorted(added)) if added else None,
                'tickers_removed': ', '.join(sorted(removed)) if removed else None,
                'num_added': len(added),
                'num_removed': len(removed),
                'change_type': 'Rebalance' if (added or removed) else 'No Change'
            })
            
            if added or removed:
                logger.info(f"Composition change on {current_date}: +{len(added)} -{len(removed)}")
                if added:
                    logger.debug(f"  Added: {sorted(added)}")
                if removed:
                    logger.debug(f"  Removed: {sorted(removed)}")
        
        changes_df = pd.DataFrame(changes_data)
        logger.info(f"Composition changes calculated: {len(changes_df)} total records")
        
        # Log summary
        total_changes = len(changes_df[changes_df['change_type'] == 'Rebalance'])
        logger.info(f"Total rebalancing events: {total_changes}")
        
        return changes_df
        
    except Exception as e:
        logger.error(f"Failed to calculate composition changes: {e}")
        return pd.DataFrame(columns=['date', 'tickers_added', 'tickers_removed', 'num_added', 'num_removed', 'change_type'])

def calculate_summary_metrics(conn) -> pd.DataFrame:
    """Calculate summary metrics for the index."""
    try:
        # Get performance data
        performance_df = pd.read_sql_query(
            "SELECT * FROM index_performance ORDER BY date", conn)
        
        # Calculate metrics
        composition_changes = calculate_composition_changes(conn)
        total_changes = len(composition_changes[composition_changes['change_type'] != 'No Change'])
        
        best_day = performance_df.loc[performance_df['daily_return'].idxmax()]
        worst_day = performance_df.loc[performance_df['daily_return'].idxmin()]
        final_return = performance_df['cumulative_return'].iloc[-1] if not performance_df.empty else 0
        
        volatility = performance_df['daily_return'].std() * np.sqrt(ANNUALIZATION_FACTOR)  # Annualized volatility
        sharpe_ratio = (final_return / volatility) if volatility > 0 else 0
        
        metrics_data = [
            {'metric': 'Total Composition Changes', 'value': total_changes},
            {'metric': 'Best Performing Day', 'value': f"{best_day['date']} ({best_day['daily_return']*100:.2f}%)"},
            {'metric': 'Worst Performing Day', 'value': f"{worst_day['date']} ({worst_day['daily_return']*100:.2f}%)"},
            {'metric': 'Total Return (%)', 'value': f"{final_return*100:.2f}%"},
            {'metric': 'Annualized Volatility (%)', 'value': f"{volatility*100:.2f}%"},
            {'metric': 'Sharpe Ratio', 'value': f"{sharpe_ratio:.2f}"},
            {'metric': 'Number of Trading Days', 'value': len(performance_df)},
            {'metric': 'Average Daily Return (%)', 'value': f"{performance_df['daily_return'].mean()*100:.4f}%"}
        ]
        
        return pd.DataFrame(metrics_data)
        
    except Exception as e:
        logger.error(f"Failed to calculate summary metrics: {e}")
        return pd.DataFrame()