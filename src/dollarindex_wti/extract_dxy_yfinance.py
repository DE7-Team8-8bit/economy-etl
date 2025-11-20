import yfinance as yf

def fetch_dxy_yfinance():
    df = yf.download(
        "DX=F", 
        interval="1h",
        period="4d", 
        auto_adjust=True
    )
    df = df.reset_index()
    df["symbol"] = "DXY"
    return df

if __name__ == "__main__":
    print(fetch_dxy_yfinance().tail())
