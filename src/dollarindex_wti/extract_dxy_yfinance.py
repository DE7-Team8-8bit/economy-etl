import yfinance as yf

def fetch_dxy_yfinance():
    df = yf.download(
        "DX=F", 
        interval="1h",
        period="2d", # 내부 캐싱 때문에 1d는 MissingError반환 -> 2d부터 가져오도록 함 (transform 단계에서 최신 것만 필터링)
        auto_adjust=True
    )
    df = df.reset_index()
    df["symbol"] = "DXY"
    return df

if __name__ == "__main__":
    print(fetch_dxy_yfinance().tail())
