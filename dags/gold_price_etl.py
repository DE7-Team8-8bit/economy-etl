from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import yfinance as yf
import pandas as pd
import io

BUCKET_NAME = "economic-data-storage"
S3_KEY_RAW_GOLD = "raw-data/gold_price.csv"

def fetch_gold_price():
    # 금 가격 가져오기
    df = yf.download("GC=F", period="5d", interval="1h")

    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer)
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
    dag_id="gold_price_etl",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,   # 스케줄 고려 X → 수동 실행용
    # schedule_interval="*/10 * * * *",
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
        op_kwargs={"s3_key": S3_KEY_RAW_GOLD}
    )

    fetch_gold >> upload_gold