from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


from src.exchange_interest_gold.extract_data import fetch_gold_price
from src.exchange_interest_gold.load_data import upload_to_s3, fetch_csv_from_s3
from src.exchange_interest_gold.transform_data import filter_gold_price
from src.exchange_interest_gold.load_snowflake import make_snowflake_stage,make_snowflake_table,copy_into_snowflake,make_snowflake_view

BUCKET_NAME = "economic-data-storage"
S3_KEY_RAW_GOLD = "raw-data/gold_price/gold_price"
S3_KEY_FILTERED_GOLD = "processed-data/gold_price/gold_price"

def upload_raw_gold_price(s3_key: str, s3_bucket_name: str, **context):

    raw_csv = fetch_gold_price(**context)
    logical_dt = context["logical_date"]
    s3_key_with_ts = f"{s3_key}_{logical_dt.strftime("%Y%m%d_%H")}.csv"
    ti = context["ti"]
    ti.xcom_push(key="logical_dt", value=logical_dt.strftime("%Y%m%d_%H"))
    upload_to_s3(raw_csv, s3_key_with_ts, s3_bucket_name)


def gold_price_transform(bucket: str, raw_key:str, filtered_key: str, **context):
    
    ti = context["ti"]
    logical_dt = ti.xcom_pull(task_ids="upload_gold_price",key="logical_dt")
    raw_key_with_ts = f"{raw_key}_{logical_dt}.csv"
    print(f"[DEBUG][upload] upload to S3:{raw_key_with_ts}")

    # 1) S3에서 raw CSV 가져오기
    raw_csv = fetch_csv_from_s3(bucket_name=bucket, key=raw_key_with_ts)

    # 2) 전처리 (필터링 + 결측치 제거 + 반올림)
    filtered_csv = filter_gold_price(raw_csv)
    filterd_key_with_ts = f"{filtered_key}_{logical_dt}.csv"

    # 3) filtered CSV를 S3에 업로드
    upload_to_s3(filtered_csv,filterd_key_with_ts,bucket)


def load_data_snowflake(table_name: str, **context):

    make_snowflake_stage(table_name)
    make_snowflake_table(table_name)
    copy_into_snowflake(table_name, **context)
    make_snowflake_view(table_name)


default_args = {
    "owner": "dy",
    "retries": 3,
    "retry_delay": timedelta(seconds=1),
}

with DAG(
    dag_id="gold_price_etl",
    start_date=datetime(2025, 11, 20, 1, 0),
    schedule_interval="28 * * * *",
    # schedule_interval=None,   # 스케줄 고려 X → 수동 실행용
    catchup=True,                             
    # catchup=False,
    max_active_runs=1,
    default_args=default_args,
    
) as dag_gold:

    task_upload_raw_gold_price = PythonOperator(
        task_id="upload_gold_price",
        python_callable=upload_raw_gold_price,
        op_kwargs={"s3_key": S3_KEY_RAW_GOLD, "s3_bucket_name": BUCKET_NAME}
    )

    task_upload_filtered_gold_price = PythonOperator(
        task_id="upload_filtered_gold_price",
        python_callable=gold_price_transform,
        op_kwargs={
            "bucket": BUCKET_NAME,
            "raw_key": S3_KEY_RAW_GOLD,
            "filtered_key": S3_KEY_FILTERED_GOLD,
        },
    )

    task_upload_data_snowflake = PythonOperator(
        task_id = "upload_data_snowflake",
        python_callable=load_data_snowflake,
        op_kwargs={"table_name": "gold_price"},
    )

    
    task_upload_raw_gold_price >> task_upload_filtered_gold_price >> task_upload_data_snowflake