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
import numpy as np
import pandas as pd
import requests

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
        df["Date"] = pd.to_datetime(df["issueDate"])
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


if __name__ == "__main__":
    start = time.time()

    dir_path = r"C:\Users\chris\trading_textbooks_workbooks\Markets\Fixed Income\Beep Beep Trade Bonds Boop\data"

    years = [str(x) for x in range(2024, 1989, -1)]
    years_str = "_".join(years) if len(years) > 1 else years[0]

    df_treasuries = multi_download_year_treasury_par_yield_curve_rate(
        years, dir_path, run_all=True
    )
    print(df_treasuries)

    # df_real_treasuries = multi_download_year_treasury_par_yield_curve_rate(
    #     years, dir_path, real_par_yields=True
    # )

    end = time.time()
    print(end - start, " sec")
