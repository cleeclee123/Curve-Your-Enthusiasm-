import asyncio
import http
import math
import os
import shutil
import time
from collections import OrderedDict
from datetime import datetime, timedelta
from typing import Dict, List, Optional, TypeVar, Union

import aiohttp
import httpx
import numpy as np
import pandas as pd
import requests
import ujson as json

JSONTypeVar = TypeVar("JSONTypeVar", dict, list, str, int, float, bool, type(None))
JSON = Union[Dict[str, JSONTypeVar], List[JSONTypeVar], str, int, float, bool, None]


def latest_download_file(path) -> str:
    os.chdir(path)
    files = sorted(os.listdir(os.getcwd()), key=os.path.getmtime)
    if len(files) == 0:
        return "Empty Directory"
    newest = files[-1]

    return newest


def get_treasurygov_header(year: int, cj: http.cookies = None) -> Dict[str, str]:
    cookie_str = ""
    if cj:
        cookies = {
            cookie.name: cookie.value
            for cookie in cj
            if "home.treasury.gov" in cookie.domain
        }
        cookie_str = "; ".join([f"{key}={value}" for key, value in cookies.items()])

    headers = {
        "authority": "home.treasury.gov",
        "method": "GET",
        "path": f"/resource-center/data-chart-center/interest-rates/TextView?type=daily_treasury_yield_curve&field_tdr_date_value={year}",
        "scheme": "https",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "max-age=0",
        "Cookie": cookie_str,
        "Dnt": "1",
        "Sec-Ch-Ua": '"Chromium";v="116", "Not)A;Brand";v="24", "Google Chrome";v="116"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": "Windows",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
    }

    if cookie_str == "":
        del headers["Cookie"]

    return headers


def multi_download_year_treasury_par_yield_curve_rate(
    years: List[int],
    raw_path: str,
    download=False,
    real_par_yields=False,
    cj: http.cookies = None,
    run_all=False,
    verbose=False,
) -> pd.DataFrame:
    async def fetch_from_treasurygov(
        session: aiohttp.ClientSession, url: str, curr_year: int
    ) -> pd.DataFrame:
        try:
            headers = get_treasurygov_header(curr_year, cj)
            treasurygov_data_type = "".join(url.split("?type=")[1].split("&field")[0])
            full_file_path = os.path.join(
                raw_path, "temp", f"{treasurygov_data_type}.csv"
            )
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    with open(full_file_path, "wb") as f:
                        chunk_size = 8192
                        while True:
                            chunk = await response.content.read(chunk_size)
                            if not chunk:
                                break
                            f.write(chunk)
                    return {
                        treasurygov_data_type: await convert_csv_to_excel(
                            full_file_path
                        )
                    }
                else:
                    raise Exception(f"Bad Status: {response.status}")
        except Exception as e:
            print(e) if verbose else None
            return {treasurygov_data_type: pd.DataFrame()}

    async def convert_csv_to_excel(full_file_path: str | None) -> str:
        if not full_file_path:
            return

        copy = full_file_path
        rdir_path = copy.split("\\")
        rdir_path.remove("temp")
        renamed = str.join("\\", rdir_path)
        renamed = f"{renamed.split('.')[0]}.xlsx"

        df_temp = pd.read_csv(full_file_path)
        df_temp["Date"] = pd.to_datetime(df_temp["Date"])
        df_temp["Date"] = df_temp["Date"].dt.strftime("%Y-%m-%d")
        if download:
            df_temp.to_excel(f"{renamed.split('.')[0]}.xlsx", index=False)
        os.remove(full_file_path)
        return df_temp

    async def get_promises(session: aiohttp.ClientSession):
        tasks = []
        for year in years:
            daily_par_yield_curve_url = f"https://home.treasury.gov/resource-center/data-chart-center/interest-rates/daily-treasury-rates.csv/{year}/all?type=daily_treasury_yield_curve&field_tdr_date_value={year}&page&_format=csv"
            daily_par_real_yield_curve_url = f"https://home.treasury.gov/resource-center/data-chart-center/interest-rates/daily-treasury-rates.csv/{year}/all?type=daily_treasury_real_yield_curve&field_tdr_date_value={year}&amp;page&amp;_format=csv"
            daily_treasury_bill_rates_url = f"https://home.treasury.gov/resource-center/data-chart-center/interest-rates/daily-treasury-rates.csv/{year}/all?type=daily_treasury_bill_rates&field_tdr_date_value={year}&page&_format=csv"
            daily_treaury_long_term_rates_extrapolation_factors_url = f"https://home.treasury.gov/resource-center/data-chart-center/interest-rates/daily-treasury-rates.csv/{year}/all?type=daily_treasury_long_term_rate&field_tdr_date_value={year}&page&_format=csv"
            daily_treasury_real_long_term_rates_averages = f"https://home.treasury.gov/resource-center/data-chart-center/interest-rates/daily-treasury-rates.csv/{year}/all?type=daily_treasury_real_long_term&field_tdr_date_value={year}&page&_format=csv"
            if run_all:
                tasks.extend(
                    [
                        fetch_from_treasurygov(
                            session, daily_par_yield_curve_url, year
                        ),
                        fetch_from_treasurygov(
                            session, daily_par_real_yield_curve_url, year
                        ),
                        fetch_from_treasurygov(
                            session, daily_treasury_bill_rates_url, year
                        ),
                        fetch_from_treasurygov(
                            session,
                            daily_treaury_long_term_rates_extrapolation_factors_url,
                            year,
                        ),
                        fetch_from_treasurygov(
                            session, daily_treasury_real_long_term_rates_averages, year
                        ),
                    ]
                )
            else:
                curr_url = (
                    daily_par_yield_curve_url
                    if not real_par_yields
                    else daily_par_real_yield_curve_url
                )
                task = fetch_from_treasurygov(session, curr_url, year)
                tasks.append(task)

        return await asyncio.gather(*tasks)

    async def run_fetch_all() -> List[pd.DataFrame]:
        async with aiohttp.ClientSession() as session:
            all_data = await get_promises(session)
            return all_data

    os.mkdir(f"{raw_path}/temp")
    dfs: List[Dict[str, pd.DataFrame]] = asyncio.run(run_fetch_all())
    shutil.rmtree(f"{raw_path}/temp")
    # MAX_ITERATIONS = 3
    # temp_counter = 0
    # while True:
    #     if temp_counter == MAX_ITERATIONS:
    #         raise TimeoutError("temp dir is populated")
    #     if len(os.listdir(f"{raw_path}/temp")) == 0:
    #         break
    #     time.sleep(1)
    #     temp_counter += 1
    # os.rmdir(f"{raw_path}/temp")

    if not run_all:
        dfs = [next(iter(dictionary.values())) for dictionary in dfs]
        yield_df = pd.concat(dfs, ignore_index=True)
        # if download:
        #     years_str = str.join("_", [str(x) for x in years])
        #     yield_df.to_excel(
        #         (
        #             os.path.join(raw_path, f"{years_str}_daily_treasury_rates.xlsx")
        #             if not real_par_yields
        #             else os.path.join(
        #                 raw_path, f"{years_str}_daily_real_treasury_rates.xlsx"
        #             )
        #         ),
        #         index=False,
        #     )

        return yield_df

    organized_by_ust_type_dict: Dict[str, List[pd.DataFrame]] = {}
    for dictionary in dfs:
        ust_data_type, df = next(iter(dictionary)), next(iter(dictionary.values()))
        if not ust_data_type or df is None or df.empty:
            continue
        if ust_data_type not in organized_by_ust_type_dict:
            organized_by_ust_type_dict[ust_data_type] = []
        organized_by_ust_type_dict[ust_data_type].append(df)

    organized_by_ust_type_df_dict_concated: Dict[str, pd.DataFrame] = {}
    for ust_data_type in organized_by_ust_type_dict.keys():
        dfs = organized_by_ust_type_dict[ust_data_type]
        concated_df = pd.concat(dfs, ignore_index=True)
        organized_by_ust_type_df_dict_concated[ust_data_type] = concated_df

    return organized_by_ust_type_df_dict_concated


def get_historical_treasury_auctions():
    def get_treasury_query_sizing() -> List[str]:
        MAX_TREASURY_GOV_API_CONTENT_SIZE = 10000
        base_url = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/od/auctions_query?page[number]=1&page[size]=1"
        res = requests.get(base_url)
        if res.ok:
            meta = res.json()["meta"]
            size = meta["total-count"]
            number_requests = math.ceil(size / MAX_TREASURY_GOV_API_CONTENT_SIZE)
            return [
                f"https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/od/auctions_query?page[number]={i+1}&page[size]={MAX_TREASURY_GOV_API_CONTENT_SIZE}"
                for i in range(0, number_requests)
            ]

    links = get_treasury_query_sizing()

    async def fetch(session, url):
        async with session.get(url) as response:
            json_data = await response.json()
            return json_data["data"]

    async def run_fetch(urls):
        async with aiohttp.ClientSession() as session:
            tasks = [fetch(session, url) for url in urls]
            return await asyncio.gather(*tasks)

    results: List[List[JSON]] = asyncio.run(run_fetch(links))
    return [item for sublist in results for item in sublist]


def get_on_the_run_cusips(
    return_list=False, return_dict=False, to_xlsx=False
) -> pd.DataFrame | List[str] | Dict[str, str]:
    url = "https://treasurydirect.gov/TA_WS/securities/auctioned"
    res = requests.get(url)
    if res.ok:
        json_data = res.json()
        df = pd.DataFrame(json_data)
        df = df[(df["type"] != "TIPS") & (df["type"] != "FRN") & (df["type"] != "CMB")]
        df = df.drop(
            df[
                (df["type"] == "Bill")
                & (df["originalSecurityTerm"] != df["securityTerm"])
            ].index
        )
        df["issueDate"] = pd.to_datetime(df["issueDate"])
        df = df.sort_values("issueDate", ascending=False)
        result = df.groupby("originalSecurityTerm").first().reset_index()
        final_result = result[
            ["originalSecurityTerm", "type", "cusip", "auctionDate", "issueDate"]
        ]

        if to_xlsx:
            result.to_excel("on_the_run_cusips.xlsx")

        mapping = {
            "17-Week": 0.25,
            "26-Week": 0.5,
            "52-Week": 1,
            "2-Year": 2,
            "3-Year": 3,
            "5-Year": 5,
            "7-Year": 7,
            "10-Year": 10,
            "20-Year": 20,
            "30-Year": 30,
        }
        final_result["target_tenor"] = final_result["originalSecurityTerm"].replace(
            mapping
        )

        if return_list:
            return list(final_result["cusip"])
        if return_dict:
            return dict(zip(final_result["target_tenor"], final_result["cusip"]))
        return final_result


# n = 0 >> on-the-runs
def get_last_n_off_the_run_cusips(n=0, filtered=False):
    auctions_json = get_historical_treasury_auctions()
    auctions_df = pd.DataFrame(auctions_json)
    auctions_df = auctions_df[
        (auctions_df["security_type"] != "TIPS")
        & (auctions_df["security_type"] != "TIPS Note")
        & (auctions_df["security_type"] != "TIPS Bond")
        & (auctions_df["security_type"] != "FRN")
        & (auctions_df["security_type"] != "FRN Note")
        & (auctions_df["security_type"] != "FRN Bond")
        & (auctions_df["security_type"] != "CMB")
    ]
    auctions_df = auctions_df.drop(
        auctions_df[
            (auctions_df["security_type"] == "Bill")
            & (
                auctions_df["original_security_term"]
                != auctions_df["security_term_week_year"]
            )
        ].index
    )
    auctions_df["auction_date"] = pd.to_datetime(auctions_df["auction_date"])
    current_date = pd.Timestamp.now()
    auctions_df = auctions_df[auctions_df["auction_date"] <= current_date]

    auctions_df["issue_date"] = pd.to_datetime(auctions_df["issue_date"])
    auctions_df = auctions_df.sort_values("issue_date", ascending=False)

    mapping = {
        "17-Week": 0.25,
        "26-Week": 0.5,
        "52-Week": 1,
        "2-Year": 2,
        "3-Year": 3,
        "5-Year": 5,
        "7-Year": 7,
        "10-Year": 10,
        "20-Year": 20,
        "30-Year": 30,
    }

    on_the_run = auctions_df.groupby("original_security_term").first().reset_index()
    on_the_run_result = on_the_run[
        [
            "original_security_term",
            "security_type",
            "cusip",
            "auction_date",
            "issue_date",
        ]
    ]

    off_the_run = auctions_df[~auctions_df.index.isin(on_the_run.index)]
    off_the_run_result = (
        off_the_run.groupby("original_security_term")
        .nth(list(range(1, n + 1)))
        .reset_index()
    )

    combined_result = pd.concat(
        [on_the_run_result, off_the_run_result], ignore_index=True
    )
    combined_result = combined_result.sort_values(
        by=["original_security_term", "issue_date"], ascending=[True, False]
    )

    combined_result["target_tenor"] = combined_result["original_security_term"].replace(
        mapping
    )
    mask = combined_result["original_security_term"].isin(mapping.keys())
    mapped_and_filtered_df = combined_result[mask]
    grouped = mapped_and_filtered_df.groupby("original_security_term")
    max_size = grouped.size().max()
    wrapper = []
    for i in range(max_size):
        sublist = []
        for _, group in grouped:
            if i < len(group):
                sublist.append(group.iloc[i].to_dict())
        sublist = sorted(sublist, key=lambda d: d["target_tenor"])
        if filtered:
            wrapper.append(
                {
                    auctioned_dict["target_tenor"]: auctioned_dict["cusip"]
                    for auctioned_dict in sublist
                }
            )
        else:
            wrapper.append(sublist)

    return wrapper


def find_closest_dates(
    years: List[float],
    objects: Optional[List[JSON]] = None,
    df: Optional[pd.DataFrame] = None,
    date_key="date",
) -> List[JSON] | pd.DataFrame:
    today = datetime.today()

    if objects:
        results = []
        for year in years:
            target_date = today + timedelta(days=int(year * 360))
            closest_object = min(
                objects, key=lambda obj: abs(obj[date_key] - target_date)
            )
            results.append((closest_object, target_date))

        return results

    if df is not None:
        results = []
        for year in years:
            target_date = today + timedelta(days=int(year * 360))
            df["temp_diff"] = np.abs(df[date_key] - target_date)
            closest_row = df.loc[df["temp_diff"].idxmin()].copy()
            closest_row["target_date"] = target_date
            closest_row["target_tenor"] = year
            results.append(closest_row)

        df.drop(columns="temp_diff", inplace=True)
        closest_dates_df = pd.DataFrame(results).reset_index(drop=True)

        return closest_dates_df


def fetch_historical_prices(dates: List[datetime], cusips: Optional[List[str]] = None):
    url = "https://savingsbonds.gov/GA-FI/FedInvest/selectSecurityPriceDate"
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "max-age=0",
        "Connection": "keep-alive",
        "Content-Length": "73",
        "Content-Type": "application/x-www-form-urlencoded",
        "Dnt": "1",
        "Host": "savingsbonds.gov",
        "Origin": "https://savingsbonds.gov",
        "Referer": "https://savingsbonds.gov/GA-FI/FedInvest/selectSecurityPriceDate",
        "Sec-Ch-Ua": '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    }

    def build_date_payload(date: datetime):
        return {
            "priceDate.month": date.month,
            "priceDate.day": date.day,
            "priceDate.year": date.year,
            "submit": "Show Prices",
        }

    async def fetch_prices_from_treasury_date_search(
        client: httpx.AsyncClient, date: datetime
    ) -> Dict:
        payload = build_date_payload(date)
        try:
            response = await client.post(url, data=payload, follow_redirects=True)
            response.raise_for_status()
            tables = pd.read_html(response.content)
            df = tables[0]
            missing_cusips = [
                cusip for cusip in cusips if cusip not in df["CUSIP"].values
            ]
            if missing_cusips:
                print(
                    f"The following CUSIPs are not found in the DataFrame: {missing_cusips}"
                )
            df = df[df["CUSIP"].isin(cusips)] if cusips else df
            return date, df
        except httpx.HTTPStatusError as e:
            print(f"HTTP error status: {e.response.status_code}")
            return date, pd.DataFrame()
        except Exception as e:
            print(f"An error occurred: {e}")
            return date, pd.DataFrame()

    timeout = httpx.Timeout(10)

    async def run_fetch_all(dates: List[datetime]) -> List[Dict]:
        async with httpx.AsyncClient(
            headers=headers,
            timeout=timeout,
        ) as client:
            tasks = [
                fetch_prices_from_treasury_date_search(client=client, date=date)
                for date in dates
            ]
            results = await asyncio.gather(*tasks)
            return results

    bonds = asyncio.run(run_fetch_all(dates))
    return dict(bonds)


if __name__ == "__main__":
    start = time.time()

    # dir_path = r"C:\Users\chris\trading_textbooks_workbooks\Markets\Fixed Income\Beep Beep Trade Bonds Boop\data"

    # years = [str(x) for x in range(2024, 1989, -1)]
    # years_str = "_".join(years) if len(years) > 1 else years[0]

    # df_treasuries = multi_download_year_treasury_par_yield_curve_rate(
    #     years, dir_path, run_all=True
    # )
    # print(df_treasuries)

    # df_real_treasuries = multi_download_year_treasury_par_yield_curve_rate(
    #     years, dir_path, real_par_yields=True
    # )

    # a = get_last_n_off_the_run_cusips(n=1, filtered=True)
    # print(json.dumps(a[1], indent=4, default=str))

    # aa = get_on_the_run_cusips(return_list=True)
    # print(json.dumps(aa, indent=4, sort_keys=True, default=str))

    cusips = [
        "912797LJ4",
        "912797LD7",
        "912797KS5",
        "91282CKK6",
        "91282CKJ9",
        "91282CKP5",
        "91282CKN0",
        "91282CJZ5",
        "912810TZ1",
        "912810TX6",
    ]
    dates = [datetime(2024, 5, 31), datetime(2024, 5, 30)]
    dfs = fetch_historical_prices(dates=dates, cusips=cusips)
    print(dfs)

    end = time.time()
    print(end - start, " sec")
