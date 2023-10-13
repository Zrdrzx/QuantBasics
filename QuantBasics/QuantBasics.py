
import pandas as pd
import requests
from bs4 import BeautifulSoup


def get_sp500_tickers():
    res = requests.get("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")
    soup = BeautifulSoup(res.content, 'html')
    table = soup.find_all('table')[0]
    df = pd.read_html(str(table))
    tickers = list(df[0].Symbol)
    return tickers


tickers = get_sp500_tickers()


def get_history(ticker, period_start, period_end, granularity="1d"):
    import yfinance
    df = yfinance.Ticker(ticker).history(
        start=period_start,
        end=period_end,
        interval=granularity,
        auto_adjust=True
    ).reset_index()
    df = df.rename(columns={
        "Date": "datetime",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Volume": "volume"

    })
    if df.empty:
        return pd.DataFrame()

    df["datetime"] = df["datetime"].dt.tz_convert(pytz.utc)
    df = df.drop(columns=["Dividends", "Stock Splits"])
    df = df.set_index("datetime", drop=True)
    return df
    #index datetime open high low close volume

import pytz
from datetime import datetime

period_start = datetime(2010, 1, 1, tzinfo=pytz.utc)
period_end = datetime(2023, 1, 1, tzinfo=pytz.utc)


for ticker in tickers:
    df = get_history(ticker, period_start, period_end)
    print(ticker,df)

# print(df[0].to_json(orient='records'))#