from __future__ import division, print_function

from datetime import datetime, timedelta
from typing import List, Optional, Annotated, Literal

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

sns.set_style("whitegrid")

from scipy import signal as sig
from scipy.interpolate import CubicSpline
from scipy.optimize import fmin
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.linear_model import LinearRegression


def convert_tenor_to_years(tenor):
    if "Mo" in tenor:
        return int(tenor.split(" ")[0]) / 12
    elif "Yr" in tenor:
        return int(tenor.split(" ")[0])
    else:
        raise ValueError("Unexpected tenor format")


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


def split_dates_into_ranges(dates: List[datetime]) -> List[List[datetime]]:
    if not dates:
        return []

    sorted_dates = sorted(dates)
    ranges = []
    current_range = [sorted_dates[0]]

    for i in range(1, len(sorted_dates)):
        if sorted_dates[i] - sorted_dates[i - 1] == timedelta(days=1):
            current_range.append(sorted_dates[i])
        else:
            ranges.append(current_range)
            current_range = [sorted_dates[i]]

    if current_range:
        ranges.append(current_range)

    return ranges


def identify_yield_curve_movements(
    pc1: pd.Series, pc2: pd.Series, date_index: pd.DatetimeIndex, resample: Optional[Literal["W", "M", "Y"]] = None
) -> Annotated[List[List[datetime]], 4]:
    """
    0 => Bull Steepening => lime
    1 => Bear Steepening => skyblue
    2 => Bull Flattening => coral
    4 => Beat Flattening => khaki
    """
    movements = [[], [], [], []]

    if resample:
        df = pd.DataFrame({"PC1": pc1, "PC2": pc2}, index=date_index)
        resample_df = df.resample(resample).last()
        resampled_changes = resample_df.diff()
        for i in range(1, len(resampled_changes)):
            if resampled_changes["PC1"].iloc[i] < 0 and resampled_changes["PC2"].iloc[i] > 0:
                movements[0].append(resampled_changes.index[i])
            elif (
                resampled_changes["PC1"].iloc[i] > 0 and resampled_changes["PC2"].iloc[i] > 0
            ):
                movements[1].append(resampled_changes.index[i])
            elif (
                resampled_changes["PC1"].iloc[i] < 0 and resampled_changes["PC2"].iloc[i] < 0
            ):
                movements[2].append(resampled_changes.index[i])
            elif (
                resampled_changes["PC1"].iloc[i] > 0 and resampled_changes["PC2"].iloc[i] < 0
            ):
                movements[3].append(resampled_changes.index[i])
    else:
        for i in range(1, len(pc1)):
            if pc1[i] < pc1[i - 1] and pc2[i] > pc2[i - 1]:
                movements[0].append(date_index[i])
            elif pc1[i] > pc1[i - 1] and pc2[i] > pc2[i - 1]:
                movements[1].append(date_index[i])
            elif pc1[i] < pc1[i - 1] and pc2[i] < pc2[i - 1]:
                movements[2].append(date_index[i])
            elif pc1[i] > pc1[i - 1] and pc2[i] < pc2[i - 1]:
                movements[3].append(date_index[i])

    return movements


def run_pca_yield_curve(
    df: pd.DataFrame,
    date_subset_range: Annotated[List[datetime], 2] | None = None,
    show_cum_ex_var: Optional[bool] = False,
    show_most_recent: Optional[bool] = False,
    show_trend: Optional[bool] = False,
    window: Optional[int] = None,
    show_recessions: Optional[bool] = False,
    curve_analysis_resampling_window: Optional[Literal["W", "M", "Y"]] = None,
    show_bull_steepening_periods: Optional[bool] = False,
    show_bear_steepening_periods: Optional[bool] = False,
    show_bull_flattening_periods: Optional[bool] = False,
    show_bear_flattening_periods: Optional[bool] = False,
):
    df = df.copy()
    if date_subset_range:
        df = df.loc[date_subset_range[0] : date_subset_range[1]]

    scaler = StandardScaler()
    data_scaled = scaler.fit_transform(df)

    if show_cum_ex_var:
        pca = PCA()
        pca.fit(data_scaled)
        cumulative_explained_variance = np.cumsum(pca.explained_variance_ratio_)
        explained_variance = pca.explained_variance_ratio_
        _, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 10))
        ax1.plot(cumulative_explained_variance, marker="o")
        ax1.set_xlabel("Number of Components")
        ax1.set_ylabel("Cumulative Explained Variance")
        ax1.set_title("Cumulative Explained Variance by PCA Components")
        ax1.grid(True)
        ax2.bar(range(1, len(explained_variance) + 1), explained_variance, alpha=0.7)
        ax2.set_xlabel("Number of Components")
        ax2.set_ylabel("Individual Explained Variance")
        ax2.set_title("Individual Explained Variance by PCA Components")
        ax2.grid(True)
        plt.tight_layout()
        plt.show()

    pca_3 = PCA(n_components=3)
    data_pca_3 = pca_3.fit_transform(data_scaled)

    pc1 = data_pca_3[:, 0]
    pc2 = data_pca_3[:, 1]
    pc3 = data_pca_3[:, 2]

    if window:
        moving_avg_pc1 = pd.Series(pc1).rolling(window=window).mean()
        moving_avg_pc2 = pd.Series(pc2).rolling(window=window).mean()
        moving_avg_pc3 = pd.Series(pc3).rolling(window=window).mean()

    time_values = np.arange(len(df.index)).reshape(-1, 1)

    model_pc1 = LinearRegression()
    model_pc2 = LinearRegression()
    model_pc3 = LinearRegression()
    model_pc1.fit(time_values, pc1)
    model_pc2.fit(time_values, pc2)
    model_pc3.fit(time_values, pc3)
    trend_pc1 = model_pc1.predict(time_values)
    trend_pc2 = model_pc2.predict(time_values)
    trend_pc3 = model_pc3.predict(time_values)

    movements = identify_yield_curve_movements(pc1, pc2, df.index.to_list(), resample=curve_analysis_resampling_window)

    _, axes = plt.subplots(3, 1, figsize=(15, 20))

    pca_container = [
        {
            "label": "PC1 - Level",
            "pc": pc1,
            "model": model_pc1,
            "trend": trend_pc1,
            "ma": moving_avg_pc1 if window else None,
            "color": "blue",
        },
        {
            "label": "PC2 - Slope",
            "pc": pc2,
            "model": model_pc2,
            "trend": trend_pc2,
            "ma": moving_avg_pc2 if window else None,
            "color": "green",
        },
        {
            "label": "PC3 - Curvature",
            "pc": pc3,
            "model": model_pc3,
            "trend": trend_pc3,
            "ma": moving_avg_pc3 if window else None,
            "color": "red",
        },
    ]

    for i in [0, 1, 2]:
        pca_container[i]["label"]
        axes[i].plot(
            df.index,
            pca_container[i]["pc"],
            color=pca_container[i]["color"],
            label=pca_container[i]["label"],
        )
        if show_most_recent:
            axes[i].axhline(
                pca_container[i]["pc"][0],
                color="grey",
                linestyle="--",
                label=f"Recent ({pca_container[i]["trend"][-1]:.2f})",
            )
        if show_trend:
            axes[i].plot(
                df.index,
                pca_container[i]["trend"],
                color="black",
                linestyle="--",
                label="Trend",
            )
        if window:
            axes[i].plot(
                df.index,
                pca_container[i]["ma"].shift(-(window - 1)),
                color="plum",
                linestyle="--",
                label=f"{window} Day Moving Average: {round(pca_container[i]["ma"].tail(1).item(), 3)}",
            )

        if show_recessions:
            recessions_list = [
                [datetime(1990, 7, 1), datetime(1991, 3, 1)],
                [datetime(2001, 3, 1), datetime(2001, 11, 1)],
                [datetime(2007, 12, 1), datetime(2009, 6, 1)],
                [datetime(2020, 2, 1), datetime(2020, 4, 1)],
            ]
            if date_subset_range:
                start_plot_range, end_plot_range = min(date_subset_range), max(
                    date_subset_range
                )
            else:
                start_plot_range, end_plot_range = min(df.index), max(df.index)

            for start, end in recessions_list:
                if start <= end_plot_range and end >= start_plot_range:
                    axes[i].axvspan(start, end, color="lightcoral", alpha=0.3)

        if show_bull_steepening_periods:
            bs_date_ranges = split_dates_into_ranges(movements[0])
            print(bs_date_ranges)
            for date_ranges in bs_date_ranges:
                if len(date_ranges) == 1 and curve_analysis_resampling_window == "W":
                    axes[i].axvspan(date_ranges[0] - pd.offsets.BDay(5), date_ranges[0], color="lime", alpha=0.2)
                elif len(date_ranges) == 1 and curve_analysis_resampling_window == "M":
                    axes[i].axvspan(date_ranges[0] - pd.offsets.BDay(20), date_ranges[0], color="lime", alpha=0.2)
                elif len(date_ranges) == 1 and curve_analysis_resampling_window == "Y":
                    axes[i].axvspan(date_ranges[0] - pd.offsets.BDay(240), date_ranges[0], color="lime", alpha=0.2) 
                else:
                    start, end = min(date_ranges), max(date_ranges)
                    axes[i].axvspan(start, end, color="lime", alpha=0.2)

        axes[i].set_title(f"PC{i+1} over Time")
        axes[i].set_ylabel(f"PC{i+1} Values")
        axes[i].legend()
        axes[i].grid(True)

    plt.tight_layout()
    plt.show()


# Stolen from https://github.com/antdvid/MonotonicCubicInterpolation/blob/master/monospline.py
class MonoSpline:
    def __init__(self, x, y):
        self.x = np.array(x)
        self.y = np.array(y)
        self.n = self.y.size
        self.h = self.x[1:] - self.x[:-1]
        self.m = (self.y[1:] - self.y[:-1]) / self.h
        self.a = self.y[:]
        self.b = self.compute_b(self.x, self.y)
        self.c = (3 * self.m - self.b[1:] - 2 * self.b[:-1]) / self.h
        self.d = (self.b[1:] + self.b[:-1] - 2 * self.m) / (self.h * self.h)

    def compute_b(self, t, r):
        b = np.empty(self.n)
        for i in range(1, self.n - 1):
            is_mono = self.m[i - 1] * self.m[i] > 0
            if is_mono:
                b[i] = (
                    3
                    * self.m[i - 1]
                    * self.m[i]
                    / (
                        max(self.m[i - 1], self.m[i])
                        + 2 * min(self.m[i - 1], self.m[i])
                    )
                )
            else:
                b[i] = 0
            if is_mono and self.m[i] > 0:
                b[i] = min(max(0, b[i]), 3 * min(self.m[i - 1], self.m[i]))
            elif is_mono and self.m[i] < 0:
                b[i] = max(min(0, b[i]), 3 * max(self.m[i - 1], self.m[i]))

        b[0] = ((2 * self.h[0] + self.h[1]) * self.m[0] - self.h[0] * self.m[1]) / (
            self.h[0] + self.h[1]
        )
        b[self.n - 1] = (
            (2 * self.h[self.n - 2] + self.h[self.n - 3]) * self.m[self.n - 2]
            - self.h[self.n - 2] * self.m[self.n - 3]
        ) / (self.h[self.n - 2] + self.h[self.n - 3])
        return b

    def evaluate(self, t_intrp):
        ans = []
        for tau in t_intrp:
            i = np.where(tau >= self.x)[0]
            if i.size == 0:
                i = 0
            else:
                i = i[-1]
            i = min(i, self.n - 2)
            res = (
                self.a[i]
                + self.b[i] * (tau - self.x[i])
                + self.c[i] * np.power(tau - self.x[i], 2.0)
                + self.d[i] * np.power(tau - self.x[i], 3.0)
            )  # original curve r(t)
            ans.append(res)
        return ans

    def evaluate_derivative(self, t_intrp):
        ans = []
        if not hasattr(t_intrp, "__len__"):
            t_intrp = np.array([t_intrp])
        for tau in t_intrp:
            i = np.where(tau >= self.x)[0]
            if i.size == 0:
                i = 0
            else:
                i = i[-1]
            i = min(i, self.n - 2)
            res = (
                self.b[i]
                + 2 * self.c[i] * (tau - self.x[i])
                + 3 * self.d[i] * np.power(tau - self.x[i], 2.0)
            )
            ans.append(res)
        if len(ans) == 1:
            return ans[0]
        else:
            return ans

    def evaluate_forward(self, t_intrp):
        ans = []
        for tau in t_intrp:
            i = np.where(tau >= self.x)[0]
            if i.size == 0:
                i = 0
            else:
                i = i[-1]
            i = min(i, self.n - 2)
            res = (
                self.a[i]
                + self.b[i] * (2 * tau - self.x[i])
                + self.c[i] * (tau - self.x[i]) * (3 * tau - self.x[i])
                + self.d[i] * np.power(tau - self.x[i], 2.0) * (4 * tau - self.x[i])
            )  # d(xy)/dx
            ans.append(res)
        return ans
