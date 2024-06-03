from __future__ import division, print_function

from datetime import datetime
from typing import List

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

sns.set_style("whitegrid")
from scipy import signal as sig
from scipy.interpolate import CubicSpline


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
