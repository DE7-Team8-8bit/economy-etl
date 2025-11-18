# src/nasdaq_sp500/extract_yfinance.py

import os
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd

# ---------- CONFIG (No AWS/Boto3/Dotenv here!) ----------
NASDAQ_INDEX = "^IXIC"
SP500_TOP10 = ["AAPL", "MSFT", "AMZN", "GOOG", "META",
               "NVDA", "TSLA", "BRK-B", "AVGO", "JPM"]
TICKERS = [NASDAQ_INDEX] + SP500_TOP10

# ---------- HELPER FUNCTIONS (Date Calculation) ----------
def get_last_market_day() -> datetime:
    """
    Determines the date of the most recent market day that completed a trading session.
    Used by the DAG to calculate the fetch date.
    """
    today = datetime.today()
    
    if today.weekday() == 0:    # Monday: look back 3 days to Friday
        offset = 3
    elif today.weekday() in [5, 6]: # Saturday/Sunday: look back to Friday
        offset = today.weekday() - 4
    else:    # Tuesday - Friday: look back 1 day to yesterday
        offset = 1
        
    # Return date only, stripped of time components
    # NOTE: This uses the datetime object calculated in the local timezone where Airflow runs, 
    # but yfinance uses the YYYY-MM-DD string as a calendar date boundary.
    return (today - timedelta(days=offset)).replace(hour=0, minute=0, second=0, microsecond=0)


# ---------- CORE EXTRACTION FUNCTION ----------
def extract_data(fetch_date: datetime) -> pd.DataFrame:
    """
    Fetch raw hourly stock data for the specified trading day.

    Args:
        fetch_date (datetime): The date calculated by the DAG's date calculation task.
        
    Returns:
        pd.DataFrame: The raw, multi-index DataFrame from yfinance.
    """
    start = fetch_date.strftime("%Y-%m-%d")
    # Fetch until the start of the next day
    end = (fetch_date + timedelta(days=1)).strftime("%Y-%m-%d")
    
    print(f"Fetching data for trading day: {start}")
    try:
        df = yf.download(
            TICKERS, 
            start=start, 
            end=end, 
            interval="1h", 
            group_by="ticker",
            auto_adjust=True
        )
        # Drop the row where all values are NaN (often the "next day" starting row)
        df.dropna(how='all', inplace=True) 
        
        return df
    except Exception as e:
        print(f"--- ERROR: Download failed from yfinance ---")
        print(e)
        # Return an empty DataFrame on failure so the DAG can handle it gracefully
        return pd.DataFrame()