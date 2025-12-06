"""
DAG: load_stocks_to_snowflake

- 목적: S3에 저장된 주식 데이터를 Snowflake RAW_DATA.STOCK_PRICES 테이블에 적재
- 스케줄: 주중 11 AM KST
- 주요 Task:
    1. S3 → Snowflake COPY
    2. 적재 확인 (row count, unique tickers, min/max timestamp, total volume)
"""

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta

# ===========================
# Default DAG Arguments
# ===========================
default_args = {
    "owner": "Yeoreum",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# ===========================
# DAG Definition
# ===========================
with DAG(
    dag_id="load_stocks_to_snowflake",
    default_args=default_args,
    description="Load stock data from S3 to Snowflake",
    schedule_interval="0 11 * * 1-5",  # 11 AM KST weekdays
    start_date=datetime(2025, 11, 17),
    catchup=False,
    tags=["snowflake", "stocks"],
) as dag:
    
    # ===========================
    # Task: Load CSV from S3 to Snowflake
    # ===========================
    load_to_snowflake = SnowflakeOperator(
        task_id="copy_s3_to_snowflake",
        snowflake_conn_id="snowflake_conn",
        sql="""
        -- S3에서 Snowflake로 데이터 로드
        COPY INTO ECONOMIC_DATA.RAW_DATA.STOCK_PRICES (Datetime, Ticker, Open, High, Low, Close, Volume)
        FROM @ECONOMIC_DATA.RAW_DATA.S3_STOCK_STAGE        
        FILES = ('stocks_{{ macros.ds_add(ds, -1) }}.csv')
        ON_ERROR = 'CONTINUE'
        PURGE = FALSE;
        """,
        # Task-level docstring equivalent
        doc_md="""
        ### copy_s3_to_snowflake
        Load the previous trading day's stock CSV from S3 stage to Snowflake table.
        - Table: ECONOMIC_DATA.RAW_DATA.STOCK_PRICES
        - CSV file: stocks_{{ macros.ds_add(ds, -1) }}.csv
        - Error handling: CONTINUE
        """
    )
    
    # ===========================
    # Task: Verify data loaded
    # ===========================
    verify_load = SnowflakeOperator(
        task_id="verify_data_loaded",
        snowflake_conn_id="snowflake_conn",
        sql="""
        -- 적재 확인
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
        doc_md="""
        ### verify_data_loaded
        Verify that the data loaded correctly:
        - row count
        - unique tickers
        - min/max timestamp
        - total volume
        """
    )
    
    # ===========================
    # Task Dependencies
    # ===========================
    load_to_snowflake >> verify_load
