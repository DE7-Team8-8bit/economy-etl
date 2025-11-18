from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
import os, sys

# Import ETL functions
from crypto.extract_binance import extract_binance_top10_prices
from crypto.transform_crypto import transform_crypto
from crypto.load_crypto import upload_to_s3

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC_DIR = os.path.join(BASE_DIR, "src")

if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

def extract_callable(**kwargs):
    df = extract_binance_top10_prices()
    return df.to_json(orient="records")


def transform_callable(**kwargs):
    ti = kwargs['ti']
    raw_json = ti.xcom_pull(task_ids='extract')
    import pandas as pd
    df = pd.read_json(raw_json)
    df = transform_crypto(df)
    return df.to_json(orient="records")


def load_s3_callable(**kwargs):
    ti = kwargs['ti']
    raw_json = ti.xcom_pull(task_ids='transform')
    import pandas as pd
    df = pd.read_json(raw_json)

    # filename and key보
    filename = datetime.utcnow().strftime("%Y-%m-%d-%H.csv")
    prefix = os.getenv("S3_PREFIX") or "raw-data/crypto"
    key = f"{prefix}/{filename}"

    bucket = os.getenv("S3_BUCKET")
    if not bucket:
        raise RuntimeError("S3_BUCKET environment variable is not set in DAG environment")

    print(f"[dag] Uploading to s3://{bucket}/{key}")
    upload_to_s3(df, bucket=bucket, key=key)
    print(f"[dag] Upload finished: s3://{bucket}/{key}")


# DAG definition
default_args = {
    "owner": "yoond",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="crypto_top10_pipeline",
    start_date=days_ago(0),
    schedule_interval="0 * * * *",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
) as dag:

    extract = PythonOperator(
        task_id="extract",
        python_callable=extract_callable,
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_callable,
    )

    load_s3 = PythonOperator(
        task_id="load_s3",
        python_callable=load_s3_callable,
    )

    extract >> transform >> load_s3
