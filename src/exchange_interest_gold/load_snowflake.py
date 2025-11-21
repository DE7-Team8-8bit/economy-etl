import os
import snowflake.connector

def get_snowflake_connection():
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE")
    )
    return conn

def make_snowflake_stage(table_name: str):
    conn = get_snowflake_connection()
    cs = conn.cursor()

    aws_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")

    try:
        query = f"""
        CREATE OR REPLACE STAGE ECONOMIC_DATA.RAW_DATA.{table_name.upper()}
        URL='s3://economic-data-storage/processed-data/{table_name}/'
        CREDENTIALS = (
            AWS_KEY_ID = '{aws_key}'
            AWS_SECRET_KEY = '{aws_secret}'
        )
        FILE_FORMAT = (
            TYPE = CSV,
            FIELD_DELIMITER = ',',
            SKIP_HEADER = 1
        );
        """
        cs.execute(query)
        print(f"[INFO] STAGE {table_name} 성공")

    except Exception as e:
        print(f"[INFO] STAGE {table_name} 실패", e)
        raise
    finally:
        cs.close()
        conn.close()

def make_snowflake_table(table_name: str):
    conn = get_snowflake_connection()
    cs = conn.cursor()

    try:
        query = f"""
        CREATE TABLE IF NOT EXISTS ECONOMIC_DATA.RAW_DATA.{table_name.upper()} (
            DATETIME TIMESTAMP_TZ,
            PRICE    NUMBER(18,4),
            VOLUME   NUMBER(18,0)
        );
        """
        cs.execute(query)
        print(f"[INFO] {table_name} 테이블 생성 성공")
    except Exception as e:
        print(f"[INFO] {table_name} 테이블 생성 실패", e)
        raise
    finally:
        cs.close()
        conn.close()
    

def copy_into_snowflake(table_name: str, **context):
    conn = get_snowflake_connection()
    cs = conn.cursor()
    logical_date = context["logical_date"]

    try:
        query=f"""
        COPY INTO ECONOMIC_DATA.RAW_DATA.{table_name.upper()}
            (DATETIME, PRICE, VOLUME)
        FROM @ECONOMIC_DATA.RAW_DATA.{table_name.upper()}
        FILES = ('{table_name}_{ logical_date.strftime("%Y%m%d_%H") }.csv')   -- S3에 올라간 파일 이름
        FILE_FORMAT = (
            TYPE = CSV
            FIELD_DELIMITER = ','
            SKIP_HEADER = 1
            NULL_IF = ('NULL', 'null')
        )
        ON_ERROR = 'CONTINUE';"""

        cs.execute(query)
        print(f"[INFO] {table_name} 테이블 copy 성공")
    except Exception as e:
        print(f"[INFO] {table_name} copy 실패", e)
        raise
    finally:
        cs.close()
        conn.close()

    
def make_snowflake_view(table_name: str):
    conn = get_snowflake_connection()
    cs = conn.cursor()
    view_name = f"vm_{table_name}"

    try:
        query=f"""
        CREATE OR REPLACE VIEW ECONOMIC_DATA.ANALYTICS.{view_name.upper()} AS
        SELECT
            DATETIME,
            PRICE,
            VOLUME,
            -- 이전 시간대 가격 불러오기
            LAG(PRICE) OVER (ORDER BY DATETIME) AS PREV_PRICE,
            -- 변동성 계산
            CASE
                WHEN LAG(PRICE) OVER (ORDER BY DATETIME) IS NULL THEN NULL
                ELSE (PRICE - LAG(PRICE) OVER (ORDER BY DATETIME)) / LAG(PRICE) OVER (ORDER BY DATETIME) * 100
            END AS VOLATILITY_PERCENT
        FROM ECONOMIC_DATA.RAW_DATA.{table_name.upper()}
        ORDER BY DATETIME;"""

        cs.execute(query)
        print(f"[INFO] {table_name} 뷰 생성 성공")
    except Exception as e:
        print(f"[INFO] {table_name} 뷰 생성 실패", e)
        raise
    finally:
        cs.close()
        conn.close()