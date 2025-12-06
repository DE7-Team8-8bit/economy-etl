"""
DAG: nasdaq_sp500_daily_extract

- 목적: yfinance를 사용하여 NASDAQ 지수 및 S&P500 Top10 종목의 데이터를 추출하고 S3에 CSV로 저장
- 스케줄: 주중 9 AM KST (미국 시장 종료 후)
- 주요 Task:
    1. 주식 데이터 추출 및 전처리 (long format, 컬럼 정리)
    2. S3에 CSV 업로드
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta, timezone
import pandas as pd
import yfinance as yf
import pytz
import io

# ===========================
# Configuration
# ===========================
NASDAQ_INDEX = "^IXIC"
SP500_TOP10 = ["AAPL", "MSFT", "AMZN", "GOOG", "META",
               "NVDA", "TSLA", "BRK-B", "AVGO", "JPM"]
TICKERS = [NASDAQ_INDEX] + SP500_TOP10
BUCKET_NAME = "economic-data-storage"

default_args = {
    "owner": "Yeoreum",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

# ===========================
# Helper Functions
# ===========================
def get_last_market_day():
    """
    미국 주식 시장 기준으로 가장 최근 거래일 계산
    - 시장 종료 시간: 미국 동부 16:00 = 한국 06:00 (다음날)
    - 주말이면 금요일로 조정
    Returns:
        datetime: target trading day (00:00시 기준)
    """
    eastern = pytz.timezone('US/Eastern')
    now_eastern = datetime.now(eastern)
    market_close = now_eastern.replace(hour=16, minute=0, second=0, microsecond=0)

    target_date = now_eastern - timedelta(days=1) if now_eastern < market_close else now_eastern

    while target_date.weekday() >= 5:
        target_date -= timedelta(days=1)

    print(f"Current time (EST): {now_eastern}")
    print(f"Market close time: {market_close}")
    print(f"Target trading day: {target_date.strftime('%Y-%m-%d %A')}")

    return target_date.replace(hour=0, minute=0, second=0, microsecond=0)


def extract_and_transform_stocks():
    """
    주식 데이터를 yfinance에서 추출하고 long format으로 변환
    Returns:
        dict: {'csv_data': CSV 문자열, 'date_str': YYYY-MM-DD 형식 문자열}
    """
    fetch_date = get_last_market_day()
    start = fetch_date.strftime("%Y-%m-%d")
    end = (fetch_date + timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"Fetching data for trading day: {start}")

    try:
        # 데이터 다운로드
        df = yf.download(
            TICKERS, 
            start=start, 
            end=end, 
            interval="1h", 
            group_by="ticker",
            auto_adjust=True
        )
        df.dropna(how='all', inplace=True)

        if df.empty:
            print("No data returned from yfinance")
            return None

        # Datetime 컬럼 처리
        if isinstance(df.index, pd.DatetimeIndex):
            df = df.reset_index()
            if 'Date' in df.columns:
                df.rename(columns={'Date': 'Datetime'}, inplace=True)
            elif 'index' in df.columns:
                df.rename(columns={'index': 'Datetime'}, inplace=True)

        # MultiIndex 컬럼 flatten
        if isinstance(df.columns, pd.MultiIndex):
            new_cols = []
            for col in df.columns:
                if isinstance(col, tuple):
                    ticker, price = col
                    if ticker == 'Datetime' and price == '':
                        new_cols.append('Datetime')
                    elif price == '':
                        new_cols.append(ticker)
                    else:
                        new_cols.append(f"{ticker}_{price}")
                else:
                    new_cols.append(col)
            df.columns = new_cols

        # Long format 변환
        id_vars = ['Datetime']
        value_vars = [c for c in df.columns if c != 'Datetime']
        df_long = df.melt(
            id_vars=id_vars,
            value_vars=value_vars,
            var_name='Ticker_Price',
            value_name='Value'
        )
        df_long[['Ticker', 'Price']] = df_long['Ticker_Price'].str.rsplit('_', n=1, expand=True)

        df_clean = df_long.pivot_table(
            index=['Datetime', 'Ticker'],
            columns='Price',
            values='Value'
        ).reset_index()

        expected_cols = ['Datetime', 'Ticker', 'Open', 'High', 'Low', 'Close', 'Volume']
        df_clean = df_clean[[col for col in expected_cols if col in df_clean.columns]]

        # CSV 문자열로 변환
        csv_buffer = io.StringIO()
        df_clean.to_csv(csv_buffer, index=False)
        return {'csv_data': csv_buffer.getvalue(), 'date_str': start}

    except Exception as e:
        print(f"Error in extract_and_transform_stocks: {e}")
        import traceback
        traceback.print_exc()
        raise


def upload_to_s3(**context):
    """
    CSV 데이터를 S3에 업로드
    - XCom에서 extract_and_transform 결과 가져옴
    """
    ti = context['ti']
    result = ti.xcom_pull(task_ids='extract_and_transform')
    if result is None:
        print("No data to upload")
        return

    csv_data = result['csv_data']
    date_str = result['date_str']
    s3_key = f"raw-data/nasdaq_sp500/stocks_{date_str}.csv"

    print(f"Uploading to s3://{BUCKET_NAME}/{s3_key}")
    try:
        hook = S3Hook(aws_conn_id="my_s3")
        hook.load_string(
            string_data=csv_data,
            key=s3_key,
            bucket_name=BUCKET_NAME,
            replace=True,
        )
        print(f"✅ Successfully uploaded to s3://{BUCKET_NAME}/{s3_key}")
        if hook.check_for_key(key=s3_key, bucket_name=BUCKET_NAME):
            print("✅ Verified: File exists in S3!")
        else:
            print("⚠️ Warning: Could not verify file in S3")
    except Exception as e:
        print(f"❌ Upload failed: {str(e)}")
        raise


# ===========================
# DAG Definition
# ===========================
with DAG(
    dag_id="nasdaq_sp500_daily_extract",
    default_args=default_args,
    description="Fetch and upload NASDAQ + S&P500 data for the previous trading day.",
    schedule_interval="0 9 * * 1-5",  # 9 AM KST on weekdays
    start_date=datetime(2025, 11, 17, tzinfo=timezone.utc),
    catchup=False,
    tags=["finance", "raw_data", "s3"],
) as dag:

    extract_transform_task = PythonOperator(
        task_id="extract_and_transform",
        python_callable=extract_and_transform_stocks,
        doc_md="""
        ### extract_and_transform
        Fetch NASDAQ & S&P500 Top10 stock data for the previous trading day,
        transform to long format, and return CSV string for S3 upload.
        """
    )

    upload_task = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
        provide_context=True,
        doc_md="""
        ### upload_to_s3
        Upload the CSV string from extract_and_transform task to S3 bucket.
        Verify that the file exists in S3 after upload.
        """
    )

    extract_transform_task >> upload_task
