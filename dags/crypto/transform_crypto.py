import pandas as pd

def transform_crypto(df: pd.DataFrame):
    """
    [수정된 Transform 코드]
    API 원본 컬럼(lastPrice)을 내 DB 컬럼(price)으로 매핑하여 변환
    """
    if df.empty:
        return pd.DataFrame()

    df = df.copy()

    # -------------------------------------------------------
    # [핵심 수정] 1. 컬럼 이름 매핑 (API 이름 -> 내 이름)
    # -------------------------------------------------------
    rename_map = {
        "symbol": "symbol",
        "lastPrice": "price",          # lastPrice를 price로 변경 (이게 없어서 에러 발생함)
        "quoteVolume": "volume_usdt",  # quoteVolume을 volume_usdt로 변경
        "closeTime": "timestamp"       # closeTime을 timestamp로 변경
    }
    
    # 매핑에 있는 컬럼 중, 실제 데이터에 존재하는 것만 가져오기 (안전장치)
    available_cols = [c for c in rename_map.keys() if c in df.columns]
    df = df[available_cols].copy()
    
    # 이름 변경 실행
    df.rename(columns=rename_map, inplace=True)

    # -------------------------------------------------------
    # 2. 데이터 타입 변환
    # -------------------------------------------------------
    # 이제 'price'라는 컬럼이 생겼으므로 에러가 안 납니다.
    if "price" in df.columns:
        df["price"] = pd.to_numeric(df["price"], errors="coerce").round(2).astype(float)
        # 가격이 0 이하인 데이터 삭제 (데이터 품질)
        df = df[df["price"] > 0]

    if "volume_usdt" in df.columns:
        df["volume_usdt"] = pd.to_numeric(df["volume_usdt"], errors="coerce").fillna(0)

    # -------------------------------------------------------
    # 3. 날짜 변환 (timestamp -> YYYY-MM-DD HH:MM:SS)
    # -------------------------------------------------------
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_numeric(df["timestamp"], errors="coerce")
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit='ms', utc=True)
        df["timestamp"] = df["timestamp"].dt.strftime('%Y-%m-%d %H:%M:%S')

    # 컬럼 순서 정리 (보기 좋게)
    final_cols = ["timestamp", "symbol", "price", "volume_usdt"]
    # 실제 존재하는 컬럼만 남기기
    final_cols = [c for c in final_cols if c in df.columns]
    
    return df[final_cols]