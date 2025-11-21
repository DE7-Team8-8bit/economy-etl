from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from crypto.load_snowflake import load_crypto_to_snowflake

import os

# 우리가 수정한 모듈들 불러오기
# (파일 위치: dags/crypto/ 폴더)
from crypto.extract_binance import extract_binance
from crypto.transform_crypto import transform_crypto
from crypto.load_crypto import upload_to_s3, read_from_s3

# 환경 변수 (없으면 기본값 사용)
BUCKET = os.getenv("S3_BUCKET", "economic-data-storage")

# 폴더 경로 정의 (요구하신 대로 설정)
RAW_PREFIX = "raw-data/crypto"
PROCESSED_PREFIX = "processed-data/crypto"

def extract_and_load_raw(**kwargs):
    """
    [Step 1] API Raw 데이터 추출 -> S3 'raw_data' 폴더에 저장
    """
    # 1. Raw Data 추출 (수정된 extract_binance.py 사용)
    print("[Extract] Fetching raw data from Binance...")
    df_raw = extract_binance()
    
    if df_raw.empty:
        raise ValueError("No data fetched from Binance API")

    # 2. 저장할 파일 경로 생성 (예: raw_data/crypto/2025-11-19/15-00.csv)
    execution_date = kwargs['execution_date']
    filename = execution_date.strftime("%Y-%m-%d/%H-%M.csv")
    raw_key = f"{RAW_PREFIX}/{filename}"
    
    # 3. S3에 원본 저장 (Bronze Layer)
    print(f"[Load Raw] Uploading to s3://{BUCKET}/{raw_key}")
    upload_to_s3(df_raw, BUCKET, raw_key)
    
    # 4. 다음 단계에 파일 위치 알려주기
    return raw_key

def transform_and_load_processed(**kwargs):
    """
    [Step 2] S3 Raw 읽기 -> 전처리 -> S3 'processed_data' 폴더에 저장
    """
    ti = kwargs['ti']
    # 1. 이전 단계에서 저장한 Raw 파일 경로 가져오기
    raw_key = ti.xcom_pull(task_ids='extract_raw')
    
    if not raw_key:
        raise ValueError("Raw file key not found in XCom")

    # 2. S3에서 원본 읽기
    print(f"[Read Raw] Reading from s3://{BUCKET}/{raw_key}")
    df_raw = read_from_s3(BUCKET, raw_key)
    
    # 3. 전처리 수행 (transform_crypto.py 사용)
    print("[Transform] Cleaning data...")
    df_clean = transform_crypto(df_raw)
    
    # 4. 저장할 파일 경로 생성 (raw_data -> processed_data 로 변경)
    # 예: processed_data/crypto/2025-11-19/15-00.csv
    processed_key = raw_key.replace(RAW_PREFIX, PROCESSED_PREFIX)
    
    # 5. S3에 가공본 저장 (Silver Layer)
    print(f"[Load Processed] Uploading to s3://{BUCKET}/{processed_key}")
    upload_to_s3(df_clean, BUCKET, processed_key)


# DAG 설정
default_args = {
    "owner": "yoond",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="crypto_top10_elt_pipeline", # ELT로 이름 변경
    start_date=days_ago(1),
    schedule_interval="0 * * * *", # 매 시간 정각
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["crypto", "binance", "elt"]
) as dag:

    # Task 1: 원본 추출 및 저장
    task_extract = PythonOperator(
        task_id="extract_raw",
        python_callable=extract_and_load_raw,
        provide_context=True
    )

    # Task 2: 전처리 및 저장
    task_transform = PythonOperator(
        task_id="transform_process",
        python_callable=transform_and_load_processed,
        provide_context=True
    )

    task_load_snowflake = PythonOperator(
        task_id="load_to_snowflake",
        python_callable=load_crypto_to_snowflake
    )

    # 실행 순서: Extract -> Transform -> Load to Snowflake
    task_extract >> task_transform >> task_load_snowflake