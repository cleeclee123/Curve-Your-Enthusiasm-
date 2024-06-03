import asyncio
from datetime import datetime
from typing import Dict, List, Optional

import httpx
import pandas as pd
import requests

from fetch_treasuries import get_historical_treasury_auctions


def get_cme_quikstrike_headers(tabid: Optional[str] = None):
    tabid_id_actual = tabid or "CurveWatch"
    return {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Dnt": "1",
        "Host": "cmegroup-tools.quikstrike.net",
        "Origin": "https://cmegroup-tools.quikstrike.net",
        "Referer": f"https://cmegroup-tools.quikstrike.net/User/QuikStrikeView.aspx?viewitemid=IntegratedStrikeAsYield&tabid={tabid_id_actual}&userId=UR000552832&jobRole=Student&company=University%20of%20Illinois%20at%20Urbana-Champaign&companyType=University/Education&insid=126854949&qsid=7dc4cd6f-5273-41fc-975f-6cef3a8e4824",
        "Sec-Ch-Ua": '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "X-Microsoftajax": "Delta=true",
        "X-Requested-With": "XMLHttpRequest",
    }


def get_cme_ctd_data():
    headers = get_cme_quikstrike_headers(tabid="Deliverables")
    url = "https://cmegroup-tools.quikstrike.net/User/QuikStrikeView.aspx?viewitemid=IntegratedStrikeAsYield&tabid=Deliverables&insid=126856832&qsid=4239e622-9037-467c-bd40-1733ccafafd3"
    data = {
        "aac_nid": "2905",
        "ctl00$smPublic": "ctl00$upMain|ctl00$MainContent$ucViewControl_IntegratedStrikeAsYield$lbSummary",
        "__EVENTTARGET": "ctl00$MainContent$ucViewControl_IntegratedStrikeAsYield$lbSummary",
        "__VIEWSTATEGENERATOR": "7E260167",
        "__ASYNCPOST": "true",
    }
    res = requests.post(url, headers=headers, data=data, timeout=25)
    if res.ok:
        tables = pd.read_html(res.content)
        df = tables[1]
        df.drop(df.index[0])
        cols = [
            (
                list(df.columns.get_level_values(0))[i]
                + " "
                + list(df.columns.get_level_values(1))[i]
            )
            for i, _ in enumerate(list(df.columns.get_level_values(0)))
        ]
        df.columns = cols
        return df


def get_cme_delivery_basket(bond_info_to_dict=False):
    headers = get_cme_quikstrike_headers()
    url = "https://cmegroup-tools.quikstrike.net/User/QuikStrikeView.aspx?viewitemid=IntegratedStrikeAsYield&tabid=CurveWatch&insid=126856832&qsid=4239e622-9037-467c-bd40-1733ccafafd3"
    event_targets = {
        "2 Yr": "ctl00$MainContent$ucViewControl_IntegratedStrikeAsYield$ucTenorPicker$lvFutures$ctrl0$lbTenor",
        "3 Yr": "ctl00$MainContent$ucViewControl_IntegratedStrikeAsYield$ucTenorPicker$lvFutures$ctrl1$lbTenor",
        "5 Yr": "ctl00$MainContent$ucViewControl_IntegratedStrikeAsYield$ucTenorPicker$lvFutures$ctrl2$lbTenor",
        "10 Yr": "ctl00$MainContent$ucViewControl_IntegratedStrikeAsYield$ucTenorPicker$lvFutures$ctrl3$lbTenor",
        "Ultra 10 Yr": "ctl00$MainContent$ucViewControl_IntegratedStrikeAsYield$ucTenorPicker$lvFutures$ctrl4$lbTenor",
        "T-Bond": "ctl00$MainContent$ucViewControl_IntegratedStrikeAsYield$ucTenorPicker$lvFutures$ctrl5$lbTenor",
        "20 Yr": "ctl00$MainContent$ucViewControl_IntegratedStrikeAsYield$ucTenorPicker$lvFutures$ctrl6$lbTenor",
        "Ultra T_Bond": "ctl00$MainContent$ucViewControl_IntegratedStrikeAsYield$ucTenorPicker$lvFutures$ctrl7$lbTenor",
    }

    def build_cme_delivery_basket_payload(event_target: str):
        return {
            "aac_nid": "2905",
            "ctl00$smPublic": f"ctl00$upMain|{event_target}",
            "__EVENTTARGET": event_target,
            "__VIEWSTATEGENERATOR": "7E260167",
            "__ASYNCPOST": "true",
        }

    async def fetch_cme_delivery_basket_data(
        client: httpx.AsyncClient, tenor: str, event_target: str
    ) -> Dict:
        payload = build_cme_delivery_basket_payload(event_target=event_target)
        try:
            response = await client.post(url, data=payload, follow_redirects=True)
            response.raise_for_status()
            tables = pd.read_html(response.content)

            df_ctd_info = tables[1]
            df_ctd_info.columns = df_ctd_info.iloc[0]
            df_ctd_info = df_ctd_info.drop(df_ctd_info.index[0])
            df_otr_info = tables[2]
            df_otr_info.columns = df_otr_info.iloc[0]
            df_otr_info = df_otr_info.drop(df_ctd_info.index[0])

            delivery_basket_dict = {
                "ctd": (
                    df_ctd_info.to_dict("records")[0]
                    if bond_info_to_dict
                    else df_ctd_info
                ),
                "otr": (
                    df_otr_info.to_dict("records")[0]
                    if bond_info_to_dict
                    else df_otr_info
                ),
            }

            # no Options for 3s, 20s
            if tenor == "3 Yr" or tenor == "20 Yr":
                df_deliverables = tables[3]
                df_deliverables.columns = df_deliverables.columns.get_level_values(1)
            else:
                df_strike_as_yield = tables[3]
                df_strike_as_yield.rename(columns={"Unnamed: 0": " "}, inplace=True)
                df_strike_as_yield.set_index(" ", inplace=True)
                delivery_basket_dict["strike_as_yield"] = df_strike_as_yield

                df_deliverables = tables[4]
                df_deliverables.columns = df_deliverables.columns.get_level_values(1)

            delivery_basket_dict["deliverables"] = df_deliverables
            return tenor, delivery_basket_dict

        except httpx.HTTPStatusError as e:
            print(f"{tenor} HTTP error status: {e.response.status_code}")
            return tenor, {}
        except Exception as e:
            print(f"{tenor} An error occurred: {e}")
            return tenor, {}

    timeout = httpx.Timeout(10)

    async def run_fetch_all(event_targets: Dict[str, str]) -> List[Dict]:
        async with httpx.AsyncClient(
            headers=headers,
            timeout=timeout,
        ) as client:
            tasks = [
                fetch_cme_delivery_basket_data(
                    client=client, tenor=tenor, event_target=event_target
                )
                for tenor, event_target in event_targets.items()
            ]
            results = await asyncio.gather(*tasks)
            return results

    basket_of_delivery_baskets = asyncio.run(run_fetch_all(event_targets=event_targets))
    return dict(basket_of_delivery_baskets)


def get_cusip_from_bond_dates(
    issue_date: datetime = None, maturity_date: datetime = None
) -> pd.DataFrame:
    auctions_json = get_historical_treasury_auctions()
    auctions_df = pd.DataFrame(auctions_json)
    auctions_df["issue_date"] = pd.to_datetime(auctions_df["issue_date"])
    auctions_df["maturity_date"] = pd.to_datetime(auctions_df["maturity_date"])
    auctions_df = auctions_df[
        (auctions_df["issue_date"] == issue_date)
        & (auctions_df["maturity_date"] == maturity_date)
    ]
    return auctions_df
