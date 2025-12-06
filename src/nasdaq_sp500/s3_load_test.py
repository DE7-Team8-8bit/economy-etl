"""
Module: s3_load_test.py

목적:
- yfinance에서 NASDAQ 지수 및 S&P500 Top10 종목의 시간별 데이터를 추출
- 로컬 CSV로 저장 후 AWS S3에 업로드

워크플로우:
1. 최근 완료된 시장일 계산
2. yfinance로 시간별 주가 데이터 다운로드
3. MultiIndex DataFrame → 정리된 row-per-ticker format으로 변환
4. 로컬 CSV로 저장
5. CSV를 S3에 업로드
"""

from dotenv import load_dotenv
import os
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
import boto3

load_dotenv()

# ===========================
# CONFIGURATION
# ===========================
S3_BUCKET = "economic-data-storage"
RAW_DATA_FOLDER = "raw-data/nasdaq_sp500"

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

NASDAQ_INDEX = "^IXIC"
SP500_TOP10 = ["AAPL", "MSFT", "AMZN", "GOOG", "META",
               "NVDA", "TSLA", "BRK-B", "AVGO", "JPM"]
TICKERS = [NASDAQ_INDEX] + SP500_TOP10
COLUMNS = ["Datetime", "Ticker", "Open", "High", "Low", "Close", "Volume"]

# ===========================
# HELPER FUNCTIONS
# ===========================
def get_last_market_day() -> datetime:
    """
    최근 완료된 시장일 반환
    - Monday: 3일 전 (금요일)
    - Saturday/Sunday: 이전 금요일
    - 기타: 어제
    Returns:
        datetime: 최근 거래일 (00:00 기준)
    """
    today = datetime.today()
    
    if today.weekday() == 0:  # Monday
        offset = 3
    elif today.weekday() in [5, 6]: # Saturday/Sunday
        offset = today.weekday() - 4
    else:
        offset = 1
        
    return (today - timedelta(days=offset)).replace(hour=0, minute=0, second=0, microsecond=0)


def fetch_hourly_stock_data(tickers: list) -> tuple[pd.DataFrame, datetime]:
    """
    yfinance에서 시간별 주가 데이터 다운로드
    Returns:
        tuple: (raw DataFrame, trading_day)
    """
    trading_day = get_last_market_day()
    start = trading_day.strftime("%Y-%m-%d")
    end = (trading_day + timedelta(days=1)).strftime("%Y-%m-%d")
    
    print(f"Fetching data for trading day: {start} (Interval: 1h, Adjusted: True)")
    try:
        df = yf.download(
            tickers, 
            start=start, 
            end=end, 
            interval="1h", 
            group_by="ticker",
            auto_adjust=True
        )
        return df, trading_day
    except Exception as e:
        print(f"--- ERROR: Download failed from yfinance ---\n{e}")
        return pd.DataFrame(), trading_day


def normalize(df: pd.DataFrame) -> pd.DataFrame:
    """
    MultiIndex DataFrame → row-per-ticker format으로 변환
    - Datetime을 UTC로 변환
    - 필요한 컬럼 선택 및 순서 조정
    Returns:
        pd.DataFrame: 정리된 DataFrame
    """
    df_stacked = df.stack(level=0, future_stack=True).reset_index()
    df_stacked.rename(columns={'level_0': 'Datetime', 'level_1': 'Ticker'}, inplace=True)
    
    clean_df = df_stacked[df_stacked['Ticker'].isin(TICKERS)].copy()
    clean_df['Datetime'] = pd.to_datetime(clean_df['Datetime']).dt.tz_convert('UTC').dt.strftime("%Y-%m-%d %H:%M:%S+00:00")
    
    return clean_df[COLUMNS]


def save_to_csv(df: pd.DataFrame, trading_day: datetime) -> str:
    """
    로컬 CSV로 저장
    Returns:
        str: CSV 파일 경로
    """
    os.makedirs(RAW_DATA_FOLDER, exist_ok=True)
    filename = f"hourly_stock_data_{trading_day.strftime('%Y-%m-%d')}.csv"
    file_path = os.path.join(RAW_DATA_FOLDER, filename)
    df.to_csv(file_path, index=False)
    return file_path


def upload_to_s3(file_path: str):
    """
    로컬 CSV → S3 업로드
    """
    s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION
    )
    s3_key = f"{RAW_DATA_FOLDER}/{os.path.basename(file_path)}"
    
    try:
        s3.upload_file(file_path, S3_BUCKET, s3_key)
        print(f"Uploaded file: {os.path.basename(file_path)}")
        print(f"S3 Path: s3://{S3_BUCKET}/{s3_key}")
    except Exception as e:
        print(f"--- ERROR: Failed to upload to S3 ---\n{e}")


# ===========================
# MAIN
# ===========================
def main():
    raw_df, trading_day = fetch_hourly_stock_data(TICKERS)
    
    if raw_df.empty:
        print(f"No data available for {trading_day.strftime('%Y-%m-%d')}. Exiting.")
        return

    cle
