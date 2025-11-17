from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import yfinance as yf
import pandas as pd
import boto3
import io

BUCKET_NAME = "economic-data-storage"
S3_KEY_RAW = "raw-data/exchange_rate.csv"

def fetch_krw_usd_rate():
    # KRW/USD 환율 가져오기
    df = yf.download("KRW=X", period="2d", interval="1h")
    
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer)
    return csv_buffer.getvalue()

def upload_to_s3(csv_data):

    hook = S3Hook(aws_conn_id="my_s3")
    hook.load_string(
        string_data=csv_data,
        key=S3_KEY_RAW,
        bucket_name=BUCKET_NAME,
        replace=True,
    )

with DAG(
    dag_id="exchange_rate_etl",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,   # 스케줄 고려 X → 수동 실행용
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
    )

    fetch_task >> upload_task
