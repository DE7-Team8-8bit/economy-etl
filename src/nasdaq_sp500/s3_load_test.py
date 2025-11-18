# src/nasdaq_sp500/extract_yfinance_hourly.py
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
import boto3

load_dotenv()

# ---------- CONFIG ----------
S3_BUCKET = "economic-data-storage"
# Define the S3 folder and the local folder with the same name
RAW_DATA_FOLDER = "raw-data/nasdaq_sp500"

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

# ---------- SELECT TICKERS ----------
NASDAQ_INDEX = "^IXIC"
SP500_TOP10 = ["AAPL", "MSFT", "AMZN", "GOOG", "META",
               "NVDA", "TSLA", "BRK-B", "AVGO", "JPM"]
TICKERS = [NASDAQ_INDEX] + SP500_TOP10
COLUMNS = ["Datetime", "Ticker", "Open", "High", "Low", "Close", "Volume"]


# ---------- HELPER FUNCTIONS ----------
def get_last_market_day() -> datetime:
    """
    Determines the date of the most recent market day that completed a trading session.
    (i.e., yesterday, unless today is Monday/Sunday/Saturday, then it's Friday).
    """
    today = datetime.today()
    
    # 0=Monday, 1=Tuesday, ..., 6=Sunday
    if today.weekday() == 0:  # Monday: look back 3 days to Friday
        offset = 3
    elif today.weekday() in [5, 6]: # Saturday/Sunday: look back to Friday
        offset = today.weekday() - 4
    else:  # Tuesday - Friday: look back 1 day to yesterday
        offset = 1
        
    # Return date only, stripped of time components
    return (today - timedelta(days=offset)).replace(hour=0, minute=0, second=0, microsecond=0)

def fetch_hourly_stock_data(tickers: list) -> tuple[pd.DataFrame, datetime]:
    """Fetch hourly stock data for the last completed market day, using adjusted prices."""
    trading_day = get_last_market_day()
    
    start = trading_day.strftime("%Y-%m-%d")
    end = (trading_day + timedelta(days=1)).strftime("%Y-%m-%d")
    
    print(f"Fetching data for trading day: {start} (Interval: 1h, Adjusted: True)")
    try:
        df = yf.download(
            tickers, 
            start=start, 
            end=end, 
            interval="1h", 
            group_by="ticker",
            auto_adjust=True  # Crucial for accurate historical price series
        )
        return df, trading_day
    except Exception as e:
        print(f"--- ERROR: Download failed from yfinance ---")
        print(e)
        return pd.DataFrame(), trading_day

def normalize(df: pd.DataFrame) -> pd.DataFrame:
    """Convert multi-index yfinance DataFrame into clean row-per-ticker format."""
    
    # 1. Stack the columns: Ticker (level 0) and then OHLCV (level 1)
    # Added future_stack=True to silence FutureWarning and adopt new, better implementation.
    df_stacked = df.stack(level=0, future_stack=True).reset_index()
    
    # 2. Rename columns to the desired format
    df_stacked.rename(columns={'level_0': 'Datetime', 'level_1': 'Ticker'}, inplace=True)
    
    # 3. Handle Timezone Conversion
    # The Datetime column is already timezone-aware (tz-aware) from yfinance.
    # We must use tz_convert to change the timezone, not tz_localize.
    clean_df = df_stacked[df_stacked['Ticker'].isin(TICKERS)].copy()
    
    # Check if 'Datetime' is tz-aware and convert it to UTC.
    # If it fails (rarely, if yfinance changed output), you may need to add .dt.tz_localize(None) first.
    clean_df['Datetime'] = pd.to_datetime(clean_df['Datetime']).dt.tz_convert('UTC').dt.strftime("%Y-%m-%d %H:%M:%S+00:00")

    # 4. Select and reorder columns
    COLUMNS = ["Datetime", "Ticker", "Open", "High", "Low", "Close", "Volume"]
    return clean_df[COLUMNS]

def save_to_csv(df: pd.DataFrame, trading_day: datetime) -> str:
    """Save the DataFrame to a local CSV file."""
    os.makedirs(RAW_DATA_FOLDER, exist_ok=True)
    filename = f"hourly_stock_data_{trading_day.strftime('%Y-%m-%d')}.csv"
    file_path = os.path.join(RAW_DATA_FOLDER, filename)
    df.to_csv(file_path, index=False)
    return file_path

def upload_to_s3(file_path: str):
    """Upload the local CSV file to the configured S3 bucket."""
    s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION
    )
    # Cleaner S3 Key: RAW_DATA_FOLDER/filename.csv
    s3_key = f"{RAW_DATA_FOLDER}/{os.path.basename(file_path)}"
    
    try:
        s3.upload_file(file_path, S3_BUCKET, s3_key)
        print(f"Uploaded file: {os.path.basename(file_path)}")
        print(f"S3 Path: s3://{S3_BUCKET}/{s3_key}")
    except Exception as e:
        print(f"--- ERROR: Failed to upload to S3 ---")
        print(e)
        
# ---------- MAIN ----------
def main():
    raw_df, trading_day = fetch_hourly_stock_data(TICKERS)
    
    if raw_df.empty:
        print(f"No data available for {trading_day.strftime('%Y-%m-%d')}. Exiting.")
        return

    clean_df = normalize(raw_df)

    if clean_df.empty:
         print(f"Normalization resulted in an empty DataFrame. Exiting.")
         return

    file_path = save_to_csv(clean_df, trading_day)
    print(f"Saved CSV: {file_path}")

    upload_to_s3(file_path)
    print("Process complete!")

if __name__ == "__main__":
    main()