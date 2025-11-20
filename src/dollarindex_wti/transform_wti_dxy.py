def transform_dxy(df):
    df = df.copy()

    df["TIMESTAMP"] = df["Datetime"].astype(str)
    df["PRICE"] = df["Close"].round(2)
    df["SYMBOL"] = "DXY"
    df["VOLUME"] = df["Volume"]

    df = df[["TIMESTAMP", "SYMBOL", "PRICE", "VOLUME"]]

    return df



def transform_wti(df):
    df = df.copy()

    df["TIMESTAMP"] = df["Datetime"].astype(str)
    df["PRICE"] = df["Close"].round(2)
    df["SYMBOL"] = "WTI"
    df["VOLUME"] = df["Volume"]

    df = df[["TIMESTAMP", "SYMBOL", "PRICE", "VOLUME"]]

    return df

