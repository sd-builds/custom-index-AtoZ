# Equal-Weighted Custom Index Tracker

A comprehensive Python application for constructing and tracking an equal-weighted custom index comprising the top 100 US stocks based on market capitalization.

## Project Overview

This project implements a complete data engineering pipeline that:
- Fetches real-time stock market data from multiple sources
- Stores data in a structured SQL database
- Constructs an equal-weighted index of top 100 US stocks by market cap
- Tracks daily index performance and composition changes
- Exports comprehensive analysis to Excel format in multiple sheets

## Features

### Core Functionality
- **Data Acquisition**: Fetches historical stock data using Yahoo Finance API
- **SQL Database**: In-memory SQLite database for efficient data storage and querying
- **Index Construction**: Daily rebalancing of equal-weighted index
- **Performance Tracking**: Comprehensive performance metrics and analytics
- **Excel Export**: Structured output with multiple analysis sheets

### Key Components
- **IndexTracker**: Main class handling all operations
- **DatabaseHandler**: Class for handling schema creation and data persistence
- **Error Handling**: Robust error handling and logging throughout the repo
- **Modular Design**: Clean, maintainable code structure for achieving best code quality.
- **Metrics and Graph**: Useful metrics and graph in the excel output for better performance understanding.

## Installation

### Setup Instructions

1. **Clone the repository**
   ```
   git clone <repository-url>
   ```

2. **Create virtual environment (recommended)**
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```
   pip install -r requirements.txt
   ```

4. **Run the application**
   ```
   python main.py
   ```

## Output Excel Report Structure

The application generates a comprehensive Excel file with the following sheets:

#### 1. `index_performance`
- **Date**: Trading date
- **Index Value**: Daily index value (base 100)
- **Daily Return %**: Daily percentage return
- **Cumulative Return %**: Cumulative return since inception

#### 2. `graph for index_perfomance`
- **Displays a index vs date graph for better view on perfomance trends**

#### 3. `daily_composition`
- **Date**: Trading date
- **Constituents**: Comma-separated list of 100 stock symbols

#### 4. `composition_changes`
- **Date**: Date of rebalancing
- **Tickers Added**: New stocks added to index
- **Tickers Removed**: Stocks removed from index
- **Number Added/Removed**: Count of changes

#### 5. `summary_metrics`
- **Total Composition Changes**: Number of rebalancing events
- **Best/Worst Performing Days**: Dates and returns
- **Total Return**: Overall index performance
- **Volatility**: Annualized volatility
- **Sharpe Ratio**: Risk-adjusted return metric