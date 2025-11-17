# src/nasdaq_sp500/load_stocks.py

import os
import pandas as pd
from datetime import date
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# Define constants based on your setup
# This is the path INSIDE the Docker container where temporary files are stored
LOCAL_DATA_DIR = "/opt/airflow/data/temp/"
S3_RAW_PREFIX = "raw-data/nasdaq_sp500/"
S3_BUCKET_NAME = "economic-data-storage" # Replace with your actual bucket name if different

def load_data_to_s3(df_data: pd.DataFrame, execution_date: date):
    """
    Takes the final clean DataFrame, saves it as a CSV locally, and uploads it to S3.
    
    This function will be called by the Airflow PythonOperator task.
    
    Args:
        df_data (pd.DataFrame): The DataFrame containing the cleaned stock data (Long Format).
        execution_date (date): The date the data was fetched for (from the get_last_market_day function).
    """
    if df_data.empty:
        print("DataFrame is empty. Skipping file creation and S3 upload.")
        return

    # 1. Prepare file path and directory
    # Filename format: stocks_2025-11-14.csv
    date_str = execution_date.strftime("%Y-%m-%d")
    file_name = f"stocks_{date_str}.csv"
    local_file_path = os.path.join(LOCAL_DATA_DIR, file_name)
    
    # Ensure the local directory exists inside the container
    os.makedirs(LOCAL_DATA_DIR, exist_ok=True)
    
    # 2. Save the DataFrame to a local CSV file
    df_data.to_csv(local_file_path, index=False)
    print(f"Successfully saved data locally to: {local_file_path}")
    
    # 3. Upload to S3 using Airflow S3Hook
    try:
        # The Hook securely uses the 'aws_s3_connection' defined in docker-compose.yaml
        s3_hook = S3Hook(aws_conn_id='aws_s3_connection') 
        
        # Define the target S3 Key (path)
        s3_key = S3_RAW_PREFIX + file_name
        
        # Use the Hook's method to upload the file
        s3_hook.load_file(
            filename=local_file_path,
            key=s3_key,
            bucket_name=S3_BUCKET_NAME,
            replace=True
        )
        print(f"✅ Successfully uploaded {file_name} to s3://{S3_BUCKET_NAME}/{s3_key}")
        
    except Exception as e:
        print(f"❌ S3 Upload Failed: {e}")
        # In a real pipeline, you might raise an AirflowSkipException or AirflowException here
        raise
    finally:
        # Optional: Clean up the local file after upload
        os.remove(local_file_path)
        print(f"Cleaned up local file: {local_file_path}")