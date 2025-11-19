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

from src.exchange_interest_gold.extract_data import fetch_interest_rate
from src.exchange_interest_gold.load_data import upload_to_s3

BUCKET_NAME = "economic-data-storage"
S3_KEY_INTEREST_RATE = "raw-data/interest_rate/interest_rate.csv"

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
        op_kwargs={"s3_key": S3_KEY_INTEREST_RATE, "s3_bucket_name": BUCKET_NAME}
    )

    fetch_interest >> upload_gold