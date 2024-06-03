import requests
import pandas as pd
from typing import List, Tuple


bondsupermart_headers = {
    "authority": "www.bondsupermart.com",
    "method": "GET",
    "path": "/main/ws/v3/bond-info/bond-factsheet-chart/US91282CKQ32",
    "scheme": "https",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "max-age=0",
    "Dnt": "1",
    "Priority": "u=0, i",
    "Sec-Ch-Ua": '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
}


def convert_to_dataframe(data: List[List[Tuple[int, float]]], col_value: str):
    df = pd.DataFrame(data, columns=["Epoch", col_value])
    df["Date"] = pd.to_datetime(df["Epoch"], unit="ms")
    df.drop("Epoch", axis=1, inplace=True)
    return df[["Date", col_value]]


def get_single_historical_data(cusip: str):
    url = f"https://www.bondsupermart.com/main/ws/v3/bond-info/bond-factsheet-chart/US{cusip}"
    res = requests.get(url, headers=bondsupermart_headers)

    if res.ok:
        json_data = res.json()
        yield_bid_df = convert_to_dataframe(
            data=json_data["yieldChartMap"]["SINCE_INCEPTION"][0]["data"],
            col_value="yield bid",
        )
        yield_ask_df = convert_to_dataframe(
            data=json_data["yieldChartMap"]["SINCE_INCEPTION"][1]["data"],
            col_value="yield ask",
        )
        price_bid_df = convert_to_dataframe(
            data=json_data["priceChartMap"]["SINCE_INCEPTION"][0]["data"],
            col_value="price bid",
        )
        price_ask_df = convert_to_dataframe(
            data=json_data["priceChartMap"]["SINCE_INCEPTION"][1]["data"],
            col_value="price ask",
        )

        df = pd.concat(
            [yield_bid_df, yield_ask_df, price_bid_df, price_ask_df], ignore_index=True
        )
        df = df.groupby("Date").sum().reset_index()
        df["yield mid Price"] = (df["yield ask"] + df["yield bid"]) / 2
        df["price mid Price"] = (df["price ask"] + df["price bid"]) / 2

        return df


if __name__ == "__main__":
    df = get_single_historical_data(cusip="91282CKQ32")
    print(df)
