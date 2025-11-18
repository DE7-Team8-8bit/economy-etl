import pandas as pd

def transform_crypto(df: pd.DataFrame):
    """
    Snowflake 적재를 위한 컬럼 정제.
    timestamp → UTC
    컬럼명 스네이크 케이스로 정리
    price는 소수점 2자리로 반올림
    """

    df = df.copy()

    df.rename(columns={
        "symbol": "symbol",
        "price": "price",
        "timestamp": "event_time_utc"
    }, inplace=True)

    # price를 숫자로 안전하게 변환하고 소수점 2자리로 반올림
    df["price"] = pd.to_numeric(df["price"], errors="coerce").round(2).astype(float)

    return df