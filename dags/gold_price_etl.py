from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import yfinance as yf
import pandas as pd
import io

from src.exchange_interest_gold.extract_data import fetch_gold_price
from src.exchange_interest_gold.load_data import upload_to_s3, fetch_csv_from_s3
from src.exchange_interest_gold.transform_data import filter_gold_price

BUCKET_NAME = "economic-data-storage"
S3_KEY_RAW_GOLD = "raw-data/gold_price/gold_price.csv"
S3_KEY_FILTERED_GOLD = "processed-data/gold_price/gold_price.csv"

def gold_price_transform(bucket: str, raw_key: str, filtered_key: str):
    # 1) S3에서 raw CSV 가져오기
    raw_csv = fetch_csv_from_s3(bucket_name=bucket, key=raw_key)

    # 2) 전처리 (필터링 + 결측치 제거 + 반올림)
    filtered_csv = filter_gold_price(raw_csv)

    # 3) filtered CSV를 S3에 업로드
    upload_to_s3(
        csv_data=filtered_csv,
        s3_key=filtered_key,
        s3_bucket_name=bucket,
    )

with DAG(
    dag_id="gold_price_etl",
    start_date=datetime(2025, 1, 1),
    # schedule_interval=None,   # 스케줄 고려 X → 수동 실행용
    schedule_interval="*/10 * * * *",
    catchup=False,
) as dag_gold:

    fetch_gold = PythonOperator(
        task_id="fetch_gold_price",
        python_callable=fetch_gold_price,
    )

    upload_gold = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
        op_args=[fetch_gold.output],     # CSV 결과 전달
        op_kwargs={"s3_key": S3_KEY_RAW_GOLD, "s3_bucket_name": BUCKET_NAME}
    )

    upload_filtered_gold_price = PythonOperator(
        task_id="upload_filtered_gold_price",
        python_callable=gold_price_transform,
        op_kwargs={
            "bucket": BUCKET_NAME,
            "raw_key": S3_KEY_RAW_GOLD,
            "filtered_key": S3_KEY_FILTERED_GOLD,
        },
    )

    fetch_gold >> upload_gold >> upload_filtered_gold_price