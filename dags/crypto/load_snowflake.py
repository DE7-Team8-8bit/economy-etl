import os
import snowflake.connector

def get_snowflake_connection():
    """환경 변수를 이용해 Snowflake 연결 객체 생성"""
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

def load_crypto_to_snowflake():
    """S3의 가공된 Crypto 데이터를 Snowflake로 COPY"""
    conn = get_snowflake_connection()
    cur = conn.cursor()

    try:
        # CRYPTO_STAGE는 위 1단계에서 만든 Stage 이름입니다.
        # processed-data/crypto/ 경로 아래의 파일들을 로드합니다.
        query = """
        COPY INTO ECONOMIC_DATA.RAW_DATA.CRYPTO_PRICES
        FROM @ECONOMIC_DATA.RAW_DATA.CRYPTO_STAGE
        FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER=',' SKIP_HEADER=1)
        ON_ERROR = 'CONTINUE'
        """
        cur.execute(query)
        print(f"[INFO] Snowflake COPY INTO 성공")

    except Exception as e:
        print(f"[ERROR] Snowflake Load 실패: {e}")
        raise
    finally:
        cur.close()
        conn.close()