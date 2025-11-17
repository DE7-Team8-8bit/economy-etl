import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")
PROJECT_ROOT = os.path.join(AIRFLOW_HOME, "src")
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from src.dollarindex_wti.extract_dxy_yfinance import fetch_dxy_yfinance
from src.dollarindex_wti.extract_wti_yfinance import fetch_wti_yfinance
from src.dollarindex_wti.transform_wti_dxy import transform_dxy, transform_wti
from src.dollarindex_wti.load_wti_dxy import upload_df_to_s3


def run_dxy_etl():
    df = fetch_dxy_yfinance()
    df = transform_dxy(df)
    upload_df_to_s3(df, "dxy")


def run_wti_etl():
    df = fetch_wti_yfinance()
    df = transform_wti(df)
    upload_df_to_s3(df, "wti")


default_args = {
    "owner": "soojin",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dxy_wti_pipeline",
    default_args=default_args,
    description="Dollar Index & WTI hourly pipeline",
    schedule_interval="0 * * * *",  # 매 정시 실행
    start_date=datetime(2025, 11, 17),
    catchup=False,
) as dag:

    dxy_task = PythonOperator(
        task_id="run_dxy_etl",
        python_callable=run_dxy_etl
    )

    wti_task = PythonOperator(
        task_id="run_wti_etl",
        python_callable=run_wti_etl
    )

    dxy_task >> wti_task
