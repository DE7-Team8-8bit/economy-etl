from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import pandas as pd
import io

BUCKET_NAME = "economic-data-storage"
S3_KEY_TEST = "test-data/simple_test.csv"

def create_test_data():
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

with DAG(
    dag_id="test_s3_upload",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["test", "s3"],
) as dag:
    
    fetch_task = PythonOperator(
        task_id="create_test_data",
        python_callable=create_test_data,
    )
    
    upload_task = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
        op_args=[fetch_task.output],
    )
    
    fetch_task >> upload_task