from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from dotenv import load_dotenv
import os
from datetime import datetime
import yfinance as yf
import pandas as pd
import io
import requests

BUCKET_NAME = "economic-data-storage"
S3_KEY_INTEREST_RATE = "raw-data/interest_rate.csv"

load_dotenv

def fetch_interest_rate():
    # 금리 가져오기
    API_KEY = os.getenv("FRED_API_KEY")
    series_id = "FEDFUNDS"
    
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": API_KEY,
        "file_type": "json",
        "observation_start": "2025-01-01",
    }

    res = requests.get(url, params=params)
    data = res.json()["observations"]
    df = pd.DataFrame(data)[["date", "value"]]
    
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    return csv_buffer.getvalue()

def upload_to_s3(csv_data: str, s3_key: str):

    hook = S3Hook(aws_conn_id="my_s3")
    hook.load_string(
        string_data=csv_data,
        key=s3_key,
        bucket_name=BUCKET_NAME,
        replace=True,
    )

with DAG(
    dag_id="interest_rate_etl",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,   # 스케줄 고려 X → 수동 실행용
    # schedule_interval="*/10 * * * *",
    catchup=False,
) as dag_gold:

    fetch_interest = PythonOperator(
        task_id="fetch_interest_rate",
        python_callable=fetch_interest_rate,
    )

    upload_gold = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
        op_args=[fetch_interest.output],     # CSV 결과 전달
        op_kwargs={"s3_key": S3_KEY_INTEREST_RATE}
    )

    fetch_interest >> upload_gold