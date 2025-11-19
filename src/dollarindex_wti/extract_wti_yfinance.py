import yfinance as yf

def fetch_wti_yfinance():
    df = yf.download(
        "CL=F", 
        interval="1h",
        period="2d",
        auto_adjust=True
    )
    df = df.reset_index()
    df["symbol"] = "WTI"
    return df

if __name__ == "__main__":
    print(fetch_wti_yfinance().tail())

