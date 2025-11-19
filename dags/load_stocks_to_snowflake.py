from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "Yeoreum",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="load_stocks_to_snowflake",
    default_args=default_args,
    description="Load stock data from S3 to Snowflake",
    schedule_interval="0 11 * * 1-5",  # 11 AM weekdays
    start_date=datetime(2025, 11, 17),
    catchup=False,
    tags=["snowflake", "stocks"],
) as dag:
    
    load_to_snowflake = SnowflakeOperator(
        task_id="copy_s3_to_snowflake",
        snowflake_conn_id="snowflake_conn",
        sql="""
        -- S3에서 Snowflake로 로드
        COPY INTO ECONOMIC_DATA.RAW_DATA.STOCK_PRICES (Datetime, Ticker, Open, High, Low, Close, Volume)
        FROM @ECONOMIC_DATA.RAW_DATA.S3_STOCK_STAGE        
        FILES = ('stocks_{{ macros.ds_add(ds, -1) }}.csv')
        ON_ERROR = 'CONTINUE'
        PURGE = FALSE;
        """,
    )
    
    verify_load = SnowflakeOperator(
        task_id="verify_data_loaded",
        snowflake_conn_id="snowflake_conn",
        sql="""
        -- 로드 확인
        SELECT 
            '{{ ds }}' AS load_date,
            COUNT(*) AS rows_loaded,
            COUNT(DISTINCT Ticker) AS unique_tickers,
            MIN(Datetime) AS first_timestamp,
            MAX(Datetime) AS last_timestamp,
            SUM(Volume) AS total_volume
        FROM ECONOMIC_DATA.RAW_DATA.STOCK_PRICES
        WHERE DATE(Datetime) = '{{ ds }}';
        """,
    )
    
    load_to_snowflake >> verify_load