import pandas as pd
from io import StringIO

def filter_gold_price(csv_text: str) -> str:
    # 헤더 3줄짜리 CSV 읽기
    df = pd.read_csv(StringIO(csv_text), header=[0,1,2])

    df.columns = ['Datetime', 'Price', 'High', 'Low', 'Open', 'Volume']

    df_filtered = df[["Datetime", "Price", "Volume"]].dropna()
    df_filtered["Price"] = df_filtered["Price"].round(2)

    return df_filtered.to_csv(index=False)

def filter_exchange_rate(csv_text: str) -> str:
    # 헤더 3줄짜리 CSV 읽기
    df = pd.read_csv(StringIO(csv_text), header=[0,1,2])

    df.columns = ['Datetime', 'Price', 'High', 'Low', 'Open', 'Volume']

    df_filtered = df[["Datetime", "Price", "Volume"]].dropna()
    df_filtered["Price"] = df_filtered["Price"].round(2)

    return df_filtered.to_csv(index=False)

def filter_interest_rate(csv_text: str) -> str:
    # 헤더 3줄짜리 CSV 읽기
    df = pd.read_csv(StringIO(csv_text), header=[0])

    df.columns = ['Datetime', 'Value'] 

    df_filtered = df[['Datetime', 'Value']].dropna()
    df_filtered["Value"] = df_filtered["Value"].round(2)

    return df_filtered.to_csv(index=False)
