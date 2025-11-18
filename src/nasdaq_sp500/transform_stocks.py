import pandas as pd
from datetime import date, datetime
import pytz

# Define the UTC timezone object
UTC = pytz.utc
# Define the trading timezone for conversion (New York)
NY_TZ = pytz.timezone('America/New_York')

def transform_data(df: pd.DataFrame, fetch_date: date) -> pd.DataFrame:
    """
    Transforms the wide-format hourly stock data (from yfinance) into a clean, 
    tidy (long) format, standardizing the timestamp to UTC.

    Args:
        df (pd.DataFrame): The raw, multi-index DataFrame from yfinance.
        fetch_date (date): The calendar date the data was fetched for.

    Returns:
        pd.DataFrame: A clean DataFrame in long format, ready for loading.
    """
    if df.empty:
        return pd.DataFrame()

    # 1. Tidy Data Transformation (Unstacking the Ticker)
    # The raw data has a MultiIndex column (Level 0: Open, Close, etc. | Level 1: Ticker)
    # We unstack the Ticker (Level 1) to make Ticker a row index, then reset the index.
    df_long = df.stack(level=1).reset_index()

    # 2. Rename Columns for Clarity
    df_long.rename(columns={
        'level_0': 'Datetime', 
        'level_1': 'Ticker',
        'High': 'High_Price',
        'Low': 'Low_Price',
        'Open': 'Open_Price',
        'Close': 'Close_Price',
        'Volume': 'Volume',
    }, inplace=True)
    
    # Select and reorder final columns
    df_long = df_long[['Datetime', 'Ticker', 'Open_Price', 'High_Price', 'Low_Price', 'Close_Price', 'Volume']]

    # 3. Standardize Timezone to UTC
    # yfinance often returns naive datetime objects interpreted as the market's timezone (NY_TZ).
    # We first localize it to NY_TZ and then convert it to UTC for standardization.
    
    # Check if the Datetime index is naive (has no timezone info)
    if df_long['Datetime'].dt.tz is None:
        # Localize (set) the timezone to New York Time
        df_long['Datetime'] = df_long['Datetime'].dt.tz_localize(NY_TZ)
        
    # Convert to UTC and remove the timezone info for clean database storage (though +00:00 is better)
    df_long['Datetime'] = df_long['Datetime'].dt.tz_convert(UTC)

    # 4. Add Partition Key (The Date the data represents)
    # Adding a clean date column is useful for partitioning in the data warehouse (Snowflake/S3).
    df_long['Date_Key'] = df_long['Datetime'].dt.date.astype(str)

    # Ensure all numeric columns are float/int type
    numeric_cols = ['Open_Price', 'High_Price', 'Low_Price', 'Close_Price', 'Volume']
    for col in numeric_cols:
        df_long[col] = pd.to_numeric(df_long[col], errors='coerce')

    print(f"Transformation successful. Final tidy data shape: {df_long.shape}")
    return df_long