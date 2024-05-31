import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
from typing import List


def convert_tenor_to_years(tenor):
    if "Mo" in tenor:
        return int(tenor.split(" ")[0]) / 12
    elif "Yr" in tenor:
        return int(tenor.split(" ")[0])
    else:
        raise ValueError("Unexpected tenor format")


# def plot_yield_curves(df: pd.DataFrame, dates: List[datetime], title: str = None):
#     df = df.copy()
#     plt.figure(figsize=(17, 6))

#     for date in dates:
#         row = df[df["Date"] == date]
#         if row.empty:
#             print(f"Date {date.date()} not found in the data")
#             continue

#         row = row.drop(columns="Date")
#         row = row.T
#         row.columns = ["Yield"]
#         print(row.index)
#         # maturities = [convert_tenor_to_years(tenor) for tenor in row.index]
#         # print(maturities)
#         # print(row["Yield"])
#         # plt.plot(maturities, row["Yield"], marker="o", label=date.date())
#         plt.plot(row.index, row["Yield"], marker="o", label=date.date())

#     plt.xlabel("Maturity")
#     plt.ylabel("Yield (%)")
#     plt.title(title or "Yield Curves")
#     plt.legend(title="Dates")
#     plt.grid(True)
#     plt.show()


def plot_yield_curves(df: pd.DataFrame, dates: List[datetime], title: str = None):
    df = df.copy()
    plt.figure(figsize=(17, 6))

    for date in dates:
        row = df[df["Date"] == date]
        if row.empty:
            print(f"Date {date.date()} not found in the data")
            continue

        row = row.drop(columns="Date")
        row = row.T
        row.columns = ["Yield"]
        maturities = [convert_tenor_to_years(tenor) for tenor in row.index]
        # print(maturities)
        plt.plot(maturities, row["Yield"], marker="o", label=date.date())

    plt.xlabel("Maturity")
    plt.ylabel("Yield (%)")
    plt.title(title or "Yield Curves")
    plt.legend(title="Dates")
    plt.grid(True)
    plt.show()
