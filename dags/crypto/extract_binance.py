import requests
import pandas as pd
from datetime import datetime

def get_top_10_symbols_by_volume_usdt():
    """
    Binance 24hr ticker에서 USDT 페어만 필터해 quoteVolume 기준 상위 10개 symbol 반환
    (인도네시아 루피 등 다른 quote asset이 섞이는 문제 해결)
    """
    url = "https://api.binance.com/api/v3/ticker/24hr"
    res = requests.get(url, timeout=10)
    res.raise_for_status()
    data = res.json()

    df = pd.DataFrame(data)
    # USDT 페어만 사용
    df = df[df['symbol'].str.endswith('USDT')]
    # quoteVolume이 문자열일 수 있으니 숫자로 변환
    df['quoteVolume'] = pd.to_numeric(df.get('quoteVolume', 0), errors='coerce').fillna(0)
    top = df.sort_values('quoteVolume', ascending=False).head(10)
    return top['symbol'].tolist()


def extract_binance_top10_prices():
    """
    상위 10개(USDT 페어)의 현재 가격 정보를 가져와 DataFrame으로 반환.
    """
    symbols = get_top_10_symbols_by_volume_usdt()
    rows = []

    for symbol in symbols:
        url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}"
        try:
            res = requests.get(url, timeout=10)
            if res.status_code != 200:
                continue
            price_data = res.json()
            price = float(price_data.get("price"))
        except Exception:
            continue

        rows.append({
            "symbol": symbol,
            "price": price,
            "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        })

    df = pd.DataFrame(rows)
    return df


if __name__ == "__main__":
    df = extract_binance_top10_prices()
    print(df)