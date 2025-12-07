import requests
import pandas as pd

def extract_binance():
    """
    Binance 24hr ticker에서 USDT 페어 중 거래대금(quoteVolume) 상위 10개의
    '모든 Raw Data'를 수집하여 반환합니다.
    
    - 날짜 변환(Transform) 로직 제거 (Unix Timestamp 유지)
    - 컬럼 필터링 제거 (API가 주는 모든 필드 보존)
    """
    url = "https://api.binance.com/api/v3/ticker/24hr"
    
    try:
        res = requests.get(url, timeout=10)
        res.raise_for_status()
        data = res.json() # List of dictionaries
        
        df = pd.DataFrame(data)
        
        # 1. USDT 페어 필터링
        df = df[df['symbol'].str.endswith('USDT')].copy()
        
        # 2. 정렬을 위해 quoteVolume을 숫자로 변환 (원본 데이터 보존을 위해 임시 컬럼 사용 가능하지만, 보통은 형변환까지는 Extract에 포함하기도 함)
        # 여기서는 정렬을 위해 필요한 컬럼들을 numeric으로 변환합니다.
        numeric_cols = ['lastPrice', 'quoteVolume', 'openPrice', 'highPrice', 'lowPrice', 'volume']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
        # 3. 거래대금(quoteVolume) 기준 상위 10개 필터링
        top10_df = df.sort_values('quoteVolume', ascending=False).head(10)
        
        # 인덱스 재정렬
        top10_df.reset_index(drop=True, inplace=True)
        
        return top10_df

    except Exception as e:
        print(f"Error fetching data: {e}")
        return pd.DataFrame()

if __name__ == "__main__":
    # Raw Data 추출
    df = extract_binance()
    
    # 결과 확인 (모든 컬럼이 포함되어 있는지, closeTime이 숫자로 유지되는지 확인)
    print(f"수집된 데이터 크기: {df.shape}")
    print(df.head(3))
    
    # 컬럼 목록 출력 (어떤 정보가 추가되었는지 확인)
    print("\n[Available Columns]:")
    print(df.columns.tolist())