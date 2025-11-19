from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import yfinance as yf
import pandas as pd
import boto3
import io

from src.exchange_interest_gold.extract_data import fetch_krw_usd_rate
from src.exchange_interest_gold.load_data import upload_to_s3

BUCKET_NAME = "economic-data-storage"
S3_KEY_RAW_EXCHANGE_RATE = "raw-data/exchange_rate/exchange_rate.csv"

with DAG(
    dag_id="exchange_rate_etl",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,   # 스케줄 고려 X → 수동 실행용
    # schedule_interval="*/10 * * * *",
    catchup=False,
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_exchange_rate",
        python_callable=fetch_krw_usd_rate,
    )

    upload_task = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
        op_args=[fetch_task.output],     # CSV 결과 전달
        op_kwargs={"s3_key": S3_KEY_RAW_EXCHANGE_RATE, "s3_bucket_name": BUCKET_NAME}
    )

    fetch_task >> upload_task
