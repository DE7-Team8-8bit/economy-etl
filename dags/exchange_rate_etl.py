from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import yfinance as yf
import pandas as pd
import boto3
import io

from src.exchange_interest_gold.extract_data import fetch_krw_usd_rate
from src.exchange_interest_gold.load_data import upload_to_s3, fetch_csv_from_s3
from src.exchange_interest_gold.transform_data import filter_exchange_rate

BUCKET_NAME = "economic-data-storage"
S3_KEY_RAW_EXCHANGE_RATE = "raw-data/exchange_rate/exchange_rate.csv"
S3_KEY_FILTERED_EXCHANGE_RATE = "processed-data/exchange_rate/exchange_rate.csv"

def exchange_rate_transform(bucket: str, raw_key: str, filtered_key: str):
    # 1) S3에서 raw CSV 가져오기
    raw_csv = fetch_csv_from_s3(bucket_name=bucket, key=raw_key)

    # 2) 전처리 (필터링 + 결측치 제거 + 반올림)
    filtered_csv = filter_exchange_rate(raw_csv)

    # 3) filtered CSV를 S3에 업로드
    upload_to_s3(
        csv_data=filtered_csv,
        s3_key=filtered_key,
        s3_bucket_name=bucket,
    )

with DAG(
    dag_id="exchange_rate_etl",
    start_date=datetime(2025, 1, 1),
    # schedule_interval=None,   # 스케줄 고려 X → 수동 실행용
    schedule_interval="*/10 * * * *",
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

    upload_filtered_gold_price = PythonOperator(
        task_id="upload_filtered_exchange_rate",
        python_callable=exchange_rate_transform,
        op_kwargs={
            "bucket": BUCKET_NAME,
            "raw_key": S3_KEY_RAW_EXCHANGE_RATE,
            "filtered_key": S3_KEY_FILTERED_EXCHANGE_RATE,
        },
    )

    fetch_task >> upload_task >> upload_filtered_gold_price
