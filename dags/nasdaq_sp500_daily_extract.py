from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta, timezone
import pandas as pd
import yfinance as yf
import io

# Configuration
NASDAQ_INDEX = "^IXIC"
SP500_TOP10 = ["AAPL", "MSFT", "AMZN", "GOOG", "META",
               "NVDA", "TSLA", "BRK-B", "AVGO", "JPM"]
TICKERS = [NASDAQ_INDEX] + SP500_TOP10
BUCKET_NAME = "economic-data-storage"

default_args = {
    "owner": "Yeoreum",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

def get_last_market_day():
    """Calculate the most recent completed trading day"""
    today = datetime.today()
    
    if today.weekday() == 0:  # Monday
        offset = 3
    elif today.weekday() in [5, 6]:  # Weekend
        offset = today.weekday() - 4
    else:  # Tuesday-Friday
        offset = 1
        
    return (today - timedelta(days=offset)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

def extract_and_transform_stocks():
    """
    Fetch stock data and transform it to long format in one step.
    Returns CSV string ready for upload.
    """
    # Calculate fetch date
    fetch_date = get_last_market_day()
    start = fetch_date.strftime("%Y-%m-%d")
    end = (fetch_date + timedelta(days=1)).strftime("%Y-%m-%d")
    
    print(f"Fetching data for trading day: {start}")
    
    try:
        # Fetch data from yfinance
        df = yf.download(
            TICKERS, 
            start=start, 
            end=end, 
            interval="1h", 
            group_by="ticker",
            auto_adjust=True
        )
        
        df.dropna(how='all', inplace=True)
        
        if df.empty:
            print("No data returned from yfinance")
            return None
        
        print(f"Raw data shape: {df.shape}")
        print(f"Raw columns: {df.columns}")
        print(f"Index name: {df.index.name}")
        print(f"Index type: {type(df.index)}")
        
        # Handle the datetime index/column
        # yfinance returns data with a DatetimeIndex, not a column
        if isinstance(df.index, pd.DatetimeIndex):
            df = df.reset_index()
            # The reset_index creates a column named 'Date' or 'Datetime' or 'index'
            if 'Date' in df.columns:
                df.rename(columns={'Date': 'Datetime'}, inplace=True)
            elif 'index' in df.columns:
                df.rename(columns={'index': 'Datetime'}, inplace=True)
        
        print(f"Columns after reset_index: {df.columns}")
        
        # Flatten MultiIndex columns if present
        if isinstance(df.columns, pd.MultiIndex):
            new_cols = []
            for col in df.columns:
                if isinstance(col, tuple):
                    ticker, price = col
                    # Special handling for Datetime column (which comes as ('Datetime', ''))
                    if ticker == 'Datetime' and price == '':
                        new_cols.append('Datetime')
                    elif price == '':
                        # Just in case there are other columns with empty second level
                        new_cols.append(ticker)
                    else:
                        # Normal ticker columns
                        new_cols.append(f"{ticker}_{price}")
                else:
                    new_cols.append(col)
            df.columns = new_cols
        
        print(f"Columns after flattening: {df.columns}")
        
        # Transform to long format
        id_vars = ['Datetime']
        value_vars = [c for c in df.columns if c != 'Datetime']
        
        df_long = df.melt(
            id_vars=id_vars, 
            value_vars=value_vars,
            var_name='Ticker_Price', 
            value_name='Value'
        )
        
        # Split Ticker_Price into separate columns
        df_long[['Ticker', 'Price']] = df_long['Ticker_Price'].str.rsplit('_', n=1, expand=True)
        
        # Pivot to get separate columns for each price type
        df_clean = df_long.pivot_table(
            index=['Datetime', 'Ticker'], 
            columns='Price', 
            values='Value'
        ).reset_index()
        
        # Rename columns to match expected format
        df_clean = df_clean.rename(columns={
            'Open': 'Open_Price',
            'High': 'High_Price',
            'Low': 'Low_Price',
            'Close': 'Close_Price',
            'Volume': 'Volume'
        })
        
        # Ensure column order
        expected_cols = ['Datetime', 'Ticker', 'Open_Price', 'High_Price', 'Low_Price', 'Close_Price', 'Volume']
        df_clean = df_clean[[col for col in expected_cols if col in df_clean.columns]]
        
        print(f"Final data shape: {df_clean.shape}")
        print(f"Final columns: {df_clean.columns.tolist()}")
        print(f"Sample data:\n{df_clean.head()}")
        
        # Convert to CSV string
        csv_buffer = io.StringIO()
        df_clean.to_csv(csv_buffer, index=False)
        csv_data = csv_buffer.getvalue()
        
        # Return both CSV data and the date string for the filename
        return {
            'csv_data': csv_data,
            'date_str': start
        }
        
    except Exception as e:
        print(f"Error in extract_and_transform: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

def upload_to_s3(**context):
    """Upload the CSV data to S3"""
    # Get data from previous task
    ti = context['ti']
    result = ti.xcom_pull(task_ids='extract_and_transform')
    
    if result is None:
        print("No data to upload")
        return
    
    csv_data = result['csv_data']
    date_str = result['date_str']
    
    # Define S3 path
    s3_key = f"raw-data/nasdaq_sp500/stocks_{date_str}.csv"
    
    print(f"Uploading to s3://{BUCKET_NAME}/{s3_key}")
    
    try:
        hook = S3Hook(aws_conn_id="my_s3")
        
        hook.load_string(
            string_data=csv_data,
            key=s3_key,
            bucket_name=BUCKET_NAME,
            replace=True,
        )
        
        print(f"✅ Successfully uploaded to s3://{BUCKET_NAME}/{s3_key}")
        
        # Verify upload
        if hook.check_for_key(key=s3_key, bucket_name=BUCKET_NAME):
            print("✅ Verified: File exists in S3!")
        else:
            print("⚠️ Warning: Could not verify file in S3")
            
    except Exception as e:
        print(f"❌ Upload failed: {str(e)}")
        raise

with DAG(
    dag_id="nasdaq_sp500_daily_extract",
    default_args=default_args,
    description="Fetch and upload NASDAQ + S&P500 data for the previous trading day.",
    schedule_interval="0 9 * * 1-5",  # 9 AM on weekdays
    start_date=datetime(2025, 11, 17, tzinfo=timezone.utc),
    catchup=False,
    tags=["finance", "raw_data", "s3"],
) as dag:
    
    extract_transform_task = PythonOperator(
        task_id="extract_and_transform",
        python_callable=extract_and_transform_stocks,
    )
    
    upload_task = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
        provide_context=True,
    )
    
    extract_transform_task >> upload_task