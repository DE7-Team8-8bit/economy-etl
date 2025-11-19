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
from src.exchange_interest_gold.load_data import upload_to_s3, fetch_csv_from_s3
from src.exchange_interest_gold.transform_data import filter_interest_rate

BUCKET_NAME = "economic-data-storage"
S3_KEY_RAW_INTEREST_RATE = "raw-data/interest_rate/interest_rate.csv"
S3_KEY_FILTERED_INTEREST_RATE = "processed-data/interest_rate/interest_rate.csv"


def interest_rate_transform(bucket: str, raw_key: str, filtered_key: str):
    # 1) S3에서 raw CSV 가져오기
    raw_csv = fetch_csv_from_s3(bucket_name=bucket, key=raw_key)

    # 2) 전처리 (필터링 + 결측치 제거 + 반올림)
    filtered_csv = filter_interest_rate(raw_csv)

    # 3) filtered CSV를 S3에 업로드
    upload_to_s3(
        csv_data=filtered_csv,
        s3_key=filtered_key,
        s3_bucket_name=bucket,
    )

with DAG(
    dag_id="interest_rate_etl",
    start_date=datetime(2025, 1, 1),
    # schedule_interval=None,   # 스케줄 고려 X → 수동 실행용
    schedule_interval="*/10 * * * *",
    catchup=False,
) as dag_gold:

    fetch_interest = PythonOperator(
        task_id="fetch_interest_rate",
        python_callable=fetch_interest_rate,
    )

    upload_interest = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
        op_args=[fetch_interest.output],     # CSV 결과 전달
        op_kwargs={"s3_key": S3_KEY_RAW_INTEREST_RATE, "s3_bucket_name": BUCKET_NAME}
    )

    upload_filtered_interest_rate = PythonOperator(
        task_id="upload_filtered_interest_rate",
        python_callable=interest_rate_transform,
        op_kwargs={
            "bucket": BUCKET_NAME,
            "raw_key": S3_KEY_RAW_INTEREST_RATE,
            "filtered_key": S3_KEY_FILTERED_INTEREST_RATE,
        },
    )

    fetch_interest >> upload_interest >> upload_filtered_interest_rate