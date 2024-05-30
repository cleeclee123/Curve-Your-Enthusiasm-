import asyncio
import os
import time
import warnings
from collections.abc import Mapping
from datetime import datetime
from typing import Dict, List, Optional, TypeVar, Union

import httpx
import pandas as pd
import ujson as json

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=pd.errors.SettingWithCopyWarning)

from dotenv import load_dotenv

from fetch_treasuries import (
    find_closest_dates,
    get_historical_treasury_auctions,
    get_on_the_run_cusips,
)
from schwab_authentication import SessionManager

JSONTypeVar = TypeVar("JSONTypeVar", dict, list, str, int, float, bool, type(None))
JSON = Union[Dict[str, JSONTypeVar], List[JSONTypeVar], str, int, float, bool, None]

load_dotenv()


class Schwab_UST_Seacher(SessionManager):
    def __init__(self, **kwargs):
        self.headless = kwargs.get("headless", True)
        self.browserType = kwargs.get("browserType", "firefox")
        self.go_to_page = "https://client.schwab.com/Areas/Trade/FixedIncomeSearch/FISearch.aspx/CusipSearch"
        super(Schwab_UST_Seacher, self).__init__()

        self.schwab_ust_search_headers = {
            "authority": "client.schwab.com",
            "method": "POST",
            "path": "/Areas/Trade/FixedIncomeSearch/FISearch.aspx/CusipSearch",
            "scheme": "https",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "max-age=0",
            "Content-Type": "application/x-www-form-urlencoded",
            "Dnt": "1",
            "Origin": "https://client.schwab.com",
            "Priority": "u=0, i",
            "Referer": "https://client.schwab.com/Areas/Trade/FixedIncomeSearch/FISearch.aspx/CusipSearch",
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

    def schwab_treasury_cusip_search(self, cusips: List[str] | Dict[str, str]):
        today = datetime.today()
        cusip_search_url = "https://client.schwab.com/Areas/Trade/FixedIncomeSearch/FISearch.aspx/CusipSearch"

        def build_cusip_payload(cusip: str):
            return {
                # "__RequestVerificationToken": request_verification_token,
                # "hdnPrintPageTitle": "ctl00_wpm_wpPgHdr_ucPgHdr_lblPgTtl",
                # "hdnSA": "Individual 8842-3338",
                # "hdnSANo": "8842-3338",
                "ProductSearch.ShowQuoteSelection": "buy",
                "ProductSearch.BestQuoteOnly": "true",
                "ProductSearch.Product": "Treasuries",
                "CusipSearch.Cusip": cusip,
                "Grid.PagingAndSorting.PrimarySort": "Maturity",
                "Grid.PagingAndSorting.PrimarySortOrder": "ASC",
                "Grid.PagingAndSorting.SecondarySort": "YTM",
                "Grid.PagingAndSorting.SecondarySortOrder": "DESC",
                "IsSearch": "true",
                "PageState.CurrentState": "EditSearch",
                "PageState.NextState": "SearchResults",
                "PageState.IncludeOnlyAccordion": "False",
                "PageState.MaturityAccordion": "False",
                "PageState.RatingsAccordion": "False",
                "PageState.SelectedAccount": "88423338",
                "PageState.ProductAccordion": "False",
                # "__VIEWSTATEGENERATOR": "6D0D6DE1",
                # "__EVENTVALIDATION": event_validation_token,
            }

        async def fetch_from_schwab_treasury_cusip_search(
            client: httpx.AsyncClient,
            url: str,
            cusip: str,
            id_key: Optional[str] = None,
        ) -> Dict:
            payload = build_cusip_payload(cusip)
            try:
                response = await client.post(url, data=payload, follow_redirects=True)
                response.raise_for_status()
                tables = pd.read_html(response.content)
                df = tables[0]
                max_estimated_total_row = df.loc[df["Estimated Total"].idxmax()]

                best = max_estimated_total_row.to_dict()
                if "Unnamed: 0" in best:
                    best["CUSIP"] = best.pop("Unnamed: 0")
                if id_key:
                    best["target_tenor"] = id_key

                maturity_date = datetime.strptime(best["Maturity"], "%m/%d/%Y")
                time_to_maturity_days = (maturity_date - today).days
                time_to_maturity_years = time_to_maturity_days / 365
                best["time_to_maturity"] = time_to_maturity_years

                return best
            except httpx.HTTPStatusError as e:
                print(f"HTTP error status: {e.response.status_code}")
                return {}
            except Exception as e:
                print(f"An error occurred: {e}")
                return {}

        timeout = httpx.Timeout(10)

        async def run_fetch_all(
            cusips: List[str] | Dict[str, str],
        ) -> List[Dict]:
            async with httpx.AsyncClient(
                headers=self.schwab_ust_search_headers,
                cookies=self.session.cookies,
                timeout=timeout,
            ) as client:
                if isinstance(cusips, Mapping):
                    tasks = [
                        fetch_from_schwab_treasury_cusip_search(
                            client=client,
                            url=cusip_search_url,
                            cusip=cusip,
                            id_key=target_tenor,
                        )
                        for target_tenor, cusip in cusips.items()
                    ]
                else:
                    tasks = [
                        fetch_from_schwab_treasury_cusip_search(
                            client=client,
                            url=cusip_search_url,
                            cusip=cusip,
                        )
                        for cusip in cusips
                    ]
                results = await asyncio.gather(*tasks)

            return results

        bonds = asyncio.run(run_fetch_all(cusips))
        return bonds


if __name__ == "__main__":
    t1 = time.time()
    historical_treasury_auctions_list = get_historical_treasury_auctions()
    historical_auctions_df = pd.DataFrame(historical_treasury_auctions_list)
    historical_auctions_df["issue_date"] = pd.to_datetime(
        historical_auctions_df["issue_date"]
    )
    historical_auctions_df["maturity_date"] = pd.to_datetime(
        historical_auctions_df["maturity_date"]
    )
    historical_auctions_df = historical_auctions_df[
        (historical_auctions_df["security_type"] == "Bill")
        | (historical_auctions_df["security_type"] == "Note")
        | (historical_auctions_df["security_type"] == "Bond")
    ]

    historical_auctions_df["auction_date"] = pd.to_datetime(
        historical_auctions_df["auction_date"]
    )
    historical_auctions_df = historical_auctions_df[
        (historical_auctions_df["auction_date"] > datetime(2000, 1, 1))
    ]

    print("Historical UST Auctions:")
    print(historical_auctions_df)

    off_the_run_maturities = {0.5 + i for i in range(30)} | {x for x in range(1, 31)}
    on_the_runs_maturities = {0.5, 1, 2, 3, 5, 7, 10, 20, 30}
    maturities_to_search = off_the_run_maturities - on_the_runs_maturities
    maturities_to_search = list(maturities_to_search)
    maturities_to_search.sort()
    off_the_run_usts_info_df = find_closest_dates(
        maturities_to_search, df=historical_auctions_df, date_key="maturity_date"
    )
    print("Off the run USTs info to search for:")
    print(off_the_run_usts_info_df)

    on_the_run_cusips = get_on_the_run_cusips(return_dict=True)
    off_the_run_cusips: Dict[str, str] = dict(
        zip(
            off_the_run_usts_info_df["target_tenor"],
            off_the_run_usts_info_df["cusip"],
        )
    )
    cusips_to_search: Dict[str, str] = on_the_run_cusips | off_the_run_cusips
    print("Cusips to search: ")
    print(json.dumps(cusips_to_search, indent=4))

    UST_Searcher = Schwab_UST_Seacher()
    UST_Searcher.login(
        username=os.getenv("SCHWAB_USERNAME"),
        password=os.getenv("SCHWAB_PASSWORD"),
        totp_secret=os.getenv("SCHWAB_TOTP_SECRET"),
    )
    usts = UST_Searcher.schwab_treasury_cusip_search(
        cusips=cusips_to_search,
    )
    usts_df = pd.DataFrame(usts)
    usts_df.to_excel("market_observed_treasuries.xlsx", index=False)
    print(usts_df)

    print(f"runtime: {time.time() - t1} seconds")
