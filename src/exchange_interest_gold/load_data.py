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

def upload_to_s3(csv_data: str, s3_key: str, s3_bucket_name: str):

    hook = S3Hook(aws_conn_id="my_s3")
    hook.load_string(
        string_data=csv_data,
        key=s3_key,
        bucket_name=s3_bucket_name,
        replace=True,
    )

def fetch_csv_from_s3(bucket_name: str, key: str) -> str:
    """
    S3에 올라가 있는 CSV 파일을 읽어서 그대로 문자열로 반환합니다.
    """
    hook = S3Hook(aws_conn_id="my_s3")

    # S3 객체 내용을 문자열로 읽어오기
    csv_text = hook.read_key(
        key=key,
        bucket_name=bucket_name,
    )

    return csv_text