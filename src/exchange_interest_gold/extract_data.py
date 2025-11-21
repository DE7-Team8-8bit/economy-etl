from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
import boto3
import io
import os
import requests


def fetch_gold_price(**context):
    # 금 가격 가져오기
    # df = yf.download("GC=F", period="5d", interval="1h")

    logical_dt = context["logical_date"]
    end = logical_dt
    start = end - timedelta(hours=4)
    df = yf.download("GC=F", start=start, end=end, interval="1h")


    latest = df.tail(1)

    csv_buffer = io.StringIO()
    # df.to_csv(csv_buffer)
    latest.to_csv(csv_buffer)
    return csv_buffer.getvalue()

def fetch_krw_usd_rate(**context):
    # KRW/USD 환율 가져오기

    logical_dt = context["logical_date"]
    end = logical_dt
    start = end - timedelta(hours=4)
    df = yf.download("GC=F", start=start, end=end, interval="1h")

    
    latest = df.tail(1)

    csv_buffer = io.StringIO()
    # df.to_csv(csv_buffer)
    latest.to_csv(csv_buffer)
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