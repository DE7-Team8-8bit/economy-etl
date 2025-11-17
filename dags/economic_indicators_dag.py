from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone

# Import the modular functions from your src folder
from src.nasdaq_sp500.extract_yfinance import extract_data
from src.nasdaq_sp500.transform_stocks import transform_data
from src.nasdaq_sp500.load_stocks import load_data_to_s3
from src.nasdaq_sp500.extract_yfinance import get_last_market_day # Assuming this helper is here

default_args = {
    "owner": "Yeoreum",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_retry": False,
    "email_on_failure": False,
}

# The execution_date for the DAG run (which is 9 AM UTC *today*) will be passed to tasks.
# We need to manually calculate the *actual* date to fetch data for (e.g., yesterday/Friday).

def get_data_fetch_date(**kwargs):
    """Calculates the date we actually want to fetch data for (yesterday/Friday) and returns it."""
    # This helper function calculates the last *trading* day
    fetch_date = get_last_market_day()
    
    # Push the date object to XCom for downstream tasks to use
    kwargs['ti'].xcom_push(key='data_fetch_date', value=fetch_date)
    return fetch_date

def extract_task_callable(**kwargs):
    """Retrieves the fetch date and calls the extraction function."""
    ti = kwargs['ti']
    fetch_date = ti.xcom_pull(key='data_fetch_date', task_ids='calculate_fetch_date')
    
    # The extraction function returns the raw DataFrame
    raw_df = extract_data(fetch_date=fetch_date)
    
    # Push the raw DataFrame to XCom
    ti.xcom_push(key='raw_stock_data', value=raw_df)

def transform_task_callable(**kwargs):
    """Retrieves raw data, transforms it, and pushes the clean DataFrame."""
    ti = kwargs['ti']
    # Pull both the raw data and the fetch date
    raw_df = ti.xcom_pull(key='raw_stock_data', task_ids='extract_stock_data')
    fetch_date = ti.xcom_pull(key='data_fetch_date', task_ids='calculate_fetch_date')
    
    # The transformation function returns the clean, long-format DataFrame
    clean_df = transform_data(df=raw_df, fetch_date=fetch_date)
    
    # Push the clean DataFrame to XCom
    ti.xcom_push(key='clean_stock_data', value=clean_df)

def load_task_callable(**kwargs):
    """Retrieves clean data and loads it to S3."""
    ti = kwargs['ti']
    # Pull the clean data and the fetch date
    clean_df = ti.xcom_pull(key='clean_stock_data', task_ids='transform_stock_data')
    fetch_date = ti.xcom_pull(key='data_fetch_date', task_ids='calculate_fetch_date')
    
    # The loading function saves the CSV and uploads it to S3
    load_data_to_s3(df_data=clean_df, execution_date=fetch_date)


dag = DAG(
    dag_id="nasdaq_sp500_daily_extract", 
    default_args=default_args,
    description="Fetch and upload NASDAQ + S&P500 data for the previous trading day.",
    schedule_interval="0 9 * * 1-5",
    start_date=datetime(2025, 11, 17, tzinfo=timezone.utc),
    catchup=False,
    tags=['finance', 'raw_data', 's3']
)

# 1. Calculate the target trading date
calculate_date_task = PythonOperator(
    task_id="calculate_fetch_date",
    python_callable=get_data_fetch_date,
    dag=dag,
)

# 2. Extract Data (Pulls fetch_date, Pushes raw_df)
extract_task = PythonOperator(
    task_id="extract_stock_data",
    python_callable=extract_task_callable,
    dag=dag,
)

# 3. Transform Data (Pulls raw_df, Pushes clean_df)
transform_task = PythonOperator(
    task_id="transform_stock_data",
    python_callable=transform_task_callable,
    dag=dag,
)

# 4. Load Data (Pulls clean_df and fetch_date, Uploads to S3)
load_task = PythonOperator(
    task_id="load_to_s3",
    python_callable=load_task_callable,
    dag=dag,
)

# Define the task dependencies
calculate_date_task >> extract_task >> transform_task >> load_task