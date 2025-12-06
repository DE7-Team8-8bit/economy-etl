"""
DAG: test_s3_upload

- 목적: 로컬에서 생성한 테스트 데이터를 S3에 업로드하고 정상 업로드 여부를 확인
- 스케줄: 수동 실행 (schedule_interval=None)
- 주요 Task:
    1. 테스트용 데이터 생성 (DataFrame → CSV 문자열)
    2. CSV 데이터를 S3에 업로드 및 검증
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import pandas as pd
import io

# ===========================
# Configuration
# ===========================
BUCKET_NAME = "economic-data-storage"
S3_KEY_TEST = "test-data/simple_test.csv"

# ===========================
# Helper Functions
# ===========================
def create_test_data():
    """
    테스트용 DataFrame 생성 후 CSV 문자열로 반환
    Returns:
        str: CSV 문자열
    """
    print("Creating test data...")
    df = pd.DataFrame({
        'date': ['2025-11-18', '2025-11-18', '2025-11-18'],
        'ticker': ['AAPL', 'MSFT', 'GOOGL'],
        'price': [150.0, 380.0, 140.0],
        'volume': [1000000, 800000, 900000]
    })
    print(f"Test DataFrame:\n{df}")

    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    return csv_buffer.getvalue()


def upload_to_s3(csv_data):
    """
    CSV 데이터를 S3에 업로드 후 검증
    Args:
        csv_data (str): 업로드할 CSV 문자열
    """
    print(f"Uploading to s3://{BUCKET_NAME}/{S3_KEY_TEST}")
    
    hook = S3Hook(aws_conn_id="my_s3")
    hook.load_string(
        string_data=csv_data,
        key=S3_KEY_TEST,
        bucket_name=BUCKET_NAME,
        replace=True,
    )
    
    print(f"✅ Successfully uploaded!")
    
    if hook.check_for_key(key=S3_KEY_TEST, bucket_name=BUCKET_NAME):
        print("✅ Verified: File exists in S3!")


# ===========================
# DAG Definition
# ===========================
with DAG(
    dag_id="test_s3_upload",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # 수동 실행
    catchup=False,
    tags=["test", "s3"],
) as dag:
    
    fetch_task = PythonOperator(
        task_id="create_test_data",
        python_callable=create_test_data,
        doc_md="""
        ### create_test_data
        로컬에서 테스트용 주식 데이터를 생성하고 CSV 문자열로 반환
        """
    )
    
    upload_task = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
        op_args=[fetch_task.output],
        doc_md="""
        ### upload_to_s3
        CSV 문자열을 S3에 업로드하고 정상 업로드 여부를 검증
        """
    )
    
    # Task 순서 정의
    fetch_task >> upload_task
