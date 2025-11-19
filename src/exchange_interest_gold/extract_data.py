from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import yfinance as yf
import pandas as pd
import boto3
import io
import os
import requests

def fetch_gold_price():
    # 금 가격 가져오기
    df = yf.download("GC=F", period="5d", interval="1h")

    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer)
    return csv_buffer.getvalue()

def fetch_krw_usd_rate():
    # KRW/USD 환율 가져오기
    df = yf.download("KRW=X", period="10d", interval="1h")
    
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer)
    return csv_buffer.getvalue()

    
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