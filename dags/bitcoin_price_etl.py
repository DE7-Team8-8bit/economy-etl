from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import json
import os

def fetch_bitcoin_price(**context):
    url = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"
    response = requests.get(url).json()

    # 저장 경로: Airflow 컨테이너 내부
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    output_path = f"/opt/airflow/logs/btc_price_{ts}.json"

    # 파일 저장
    with open(output_path, "w") as f:
        json.dump(response, f)

    print(f"Saved BTC price to {output_path}")

default_args = {
    "owner": "airflow",
}

with DAG(
    dag_id="bitcoin_price_collector",
    default_args=default_args,
    schedule_interval="0 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["crypto", "bitcoin", "etl"],
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_bitcoin_price",
        python_callable=fetch_bitcoin_price,
    )