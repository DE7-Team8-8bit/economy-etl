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

def run_copy_into(table_name: str, folder: str):
    conn = get_snowflake_connection()
    cs = conn.cursor()

    try:
        query = f"""
        COPY INTO {table_name}
        FROM @dollar_oil_stage/processed-data/{folder}/
        FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER=',' SKIP_HEADER=1)
        """
        cs.execute(query)
        print(f"[INFO] COPY INTO {table_name} 성공")

    except Exception as e:
        print("COPY INTO 실패:", e)
        raise
    finally:
        cs.close()
        conn.close()


def run_dxy_snowflake_load():
    run_copy_into("DOLLAR_INDEX_PRICES", "dollar_index")


def run_wti_snowflake_load():
    run_copy_into("OIL_INDEX_PRICES", "oil_index")

