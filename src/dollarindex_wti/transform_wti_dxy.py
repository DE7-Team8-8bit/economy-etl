def transform_dxy(df):
    df = df.copy()
    df = df.rename(columns={
        "Datetime": "timestamp",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Volume": "volume"
    })
    df["symbol"] = "DXY"
    df = df[["timestamp", "open", "high", "low", "close", "volume", "symbol"]]
    return df


def transform_wti(df):
    df = df.copy()
    df = df.rename(columns={
        "Datetime": "timestamp",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Volume": "volume"
    })
    df["symbol"] = "WTI"
    df = df[["timestamp", "open", "high", "low", "close", "volume", "symbol"]]
    return df
