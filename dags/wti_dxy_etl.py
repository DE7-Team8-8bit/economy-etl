from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from io import StringIO


from dollarindex_wti.extract_dxy_yfinance import fetch_dxy_yfinance
from dollarindex_wti.extract_wti_yfinance import fetch_wti_yfinance
from dollarindex_wti.transform_wti_dxy import transform_dxy, transform_wti
from dollarindex_wti.load_wti_dxy import upload_raw_to_s3, upload_processed_to_s3


def run_dxy_etl():
    df_raw = fetch_dxy_yfinance()       
    upload_raw_to_s3(df_raw, "dxy")     
    
    df_processed = transform_dxy(df_raw) 
    upload_processed_to_s3(df_processed, "dxy") 


def run_wti_etl():
    df_raw = fetch_wti_yfinance()
    upload_raw_to_s3(df_raw, "wti")

    df_processed = transform_wti(df_raw)
    upload_processed_to_s3(df_processed, "wti")


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
