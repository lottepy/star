# -- coding: utf-8 --
"""
The code is for performing the calculation customized back-testing results, and parameter optimization of AQUMON Algo.

Previous Version:
    SG_v5_2_2_algo.py
    ** 2018-08-08：算法确定版本 **

Technique improvement:
    1. 债券组合独立于股票组合
    2. 使用ytm作为债券组合的预期收益率，历史价格波动率作为其波动率，duration作为constraint进行优化求解
    3. 使用FTEC代替VNQ
    4. 使用VB代替VBR
    5. 股债比小于等于20时，不能选风险偏好，且减少股票个数


Authors:
    Lance, Li
    Ryan, Liu

Owner:
    Magnum Research Ltd.
"""

from algorithm import addpath
import sys
import math
import numpy as np
import cvxopt
from cvxopt import solvers
import cvxpy as cp
import pandas as pd
from numpy import array, zeros, sqrt, dot, append, mean, eye, arange
from datetime import datetime, timezone, timedelta
import numba as nb
from numba.typed import Dict, List
from numba import types
from numba.experimental import jitclass
from quantcycle.app.signal_generator.base_strategy import BaseStrategy
from quantcycle.app.pms_manager.portfolio_manager import PorfolioManager
import os
import quantcycle.engine.backtest_engine


try:
    pms_type = PorfolioManager.class_type.instance_type
except:
    pms_type = "PMS_type"

WINDOW_SIZE = 52


def assets_meanvar(daily_returns, prices_df):
    """
    Calculate expected returns and covariance matrix

    :param daily_returns: 2-d array
        Daily return time series of the portfolio

    :return:
    expected_returns: 1-d array
        The expected return vector
    covariance: 2-d array
        The covariance matrix
    """
    expected_returns = np.array([])
    (rows, cols) = daily_returns.shape
    for r in range(rows):
        expected_returns = np.append(expected_returns, np.mean(daily_returns[r]))

    # covariance = shrinkage_est(daily_returns.T)
    # covariance = shrinkage_est(daily_returns.T)
    covariance = compute_realized_cov(daily_returns, prices_df)

    return expected_returns, covariance

def compute_realized_cov(data, prices_df):
    tmp = data.copy()
    # log_price = pd.np.log(tmp)
    ret = tmp.T

    cov = pd.DataFrame(None, index=prices_df.columns, columns=prices_df.columns)
    tickers = prices_df.columns.tolist()
    for x in prices_df.columns:
        for y in prices_df.columns:
            cov.loc[x, y] = pd.np.dot(ret[tickers.index(x)], ret[tickers.index(y)])
    cov = cov / len(data) * 245
    return cov.values.astype(float)

def shrinkage_est(obs):
    """
    Generate the covariance matrix of the sample using shrinkage technique.

    :param obs: 2-d array
        The time series data of close prices

    :return: 2-d array
        The covariance matrix of the sample data
    """
    m = obs.shape[0]  # number of observations for each r.v.
    n = obs.shape[1]  # number of random variables

    cov = np.cov(obs.T)  # sample covariance matrix
    mean = np.mean(obs, axis=0)
    wij_bar = cov.dot(float(m - 1) / m)
    sq_cov = np.power(cov, 2.0)
    np.fill_diagonal(sq_cov, 0.0)
    beta = np.sum(sq_cov)

    mean_removed = obs - np.ones((m, n)).dot(np.diag(mean))

    alpha = 0
    for i in range(n):
        for j in range(i + 1, n):
            for t in range(m):
                alpha += (mean_removed[t - 1, i - 1] * mean_removed[t - 1, j - 1] - wij_bar[i - 1, j - 1]) ** 2
    alpha *= (float(m) / (m - 1) ** 3)

    # alpha: sum_{j>i} Var_hat(s_ij)
    # beta/2: sum_{j>i} s_ij^2
    # TODO [joseph 20180409]: it seems that lambda should be 'alpha / (alpha + beta/2)' according to the document?
    lam = alpha / (beta / 2)
    cov_shrinkage = cov.dot(1.0 - lam) + np.diag(np.diag(cov)).dot(lam)

    return cov_shrinkage


def risk_parity(cov, lb, ub, c):
    n = len(cov)
    x = cp.Variable(shape=(n, 1))
    # objective = cp.Minimize(cp.quad_form(x, cov) / 2 - cp.sum(cp.log(x)) / n)
    objective = cp.Minimize(cp.quad_form(x, cov) / 2 - cp.sum(cp.log(x)) / n / 150)
    # objective = cp.Minimize(cp.quad_form(x, cov) / 2)
    constraints = [
        x >= lb,
        x <= ub,
        x[-1] == 0.175,
        x[1] <= 0.05,
        x[2] >= 0.25,
        cp.sum(x) == 1
    ]

    prob = cp.Problem(objective, constraints)
    result = prob.solve()
    weight_array = np.array(x.value)
    return weight_array


@jitclass({
    'portfolio_manager': pms_type,
    'ccy_matrix': nb.float64[:, :],
    'strategy_name': types.unicode_type,
    'strategy_param': nb.float64[:],
    'symbol_batch': types.ListType(types.unicode_type),
    'ccy_list': types.ListType(types.unicode_type),
    'symbol_type': types.ListType(types.unicode_type),
    'status': types.DictType(types.unicode_type, nb.float64),
    'current_fx_data': nb.float64[:],
    'current_data': nb.float64[:],
    'is_tradable': nb.boolean[:],
    'trade_time': nb.int64[:],
    'trade_price': nb.float64[:]
})
class bond_risk_parity(BaseStrategy):

    def init(self):
        self.selected_stock_path = os.path.join(addpath.data_path, "Ashare", "selected_stocks", "semiyear")

        self.rebalance_count = 0

        formation_dates = os.listdir(self.selected_stock_path)
        self.formation_dates = [datetime.strptime(formation_date[:-4], "%Y-%m-%d") for formation_date in formation_dates]
        self.formation_dates = [formation_date for formation_date in self.formation_dates if formation_date.month == 4]
        self.holding_shares = np.zeros(len(self.symbol_batch)).reshape(1, -1)

        self.tau = 0.025

    def on_data(self, data_dict: dict, time_dict: dict, ref_data_dict: dict, ref_time_dict: dict):
        current_date = datetime(time_dict['main'][-1, 3], time_dict['main'][-1, 4], time_dict['main'][-1, 5], 0, 0, 0)

        if self.rebalance_count < len(self.formation_dates):
            if current_date >= self.formation_dates[self.rebalance_count]:

                close_list = [rec[:, -1] for rec in data_dict['main']]
                close_time = []
                for idx in range(len(close_list)):
                    close_time.append(
                        datetime(time_dict['main'][idx, 3], time_dict['main'][idx, 4], time_dict['main'][idx, 5], 0, 0, 0))
                close_df = pd.DataFrame(close_list, index=close_time, columns=self.symbol_batch)

                prices_df = close_df.iloc[-WINDOW_SIZE * 5:, :]
                prices_df_2 = close_df.iloc[-WINDOW_SIZE * 5 * 2:, :]

                # Drop missing values and transpose matrix
                daily_returns = prices_df.pct_change().dropna().values.T
                expreturns, covars = assets_meanvar(daily_returns, prices_df)

                daily_return_2 = prices_df_2.pct_change().dropna()
                daily_return_2 = daily_return_2[daily_return_2['pv_ashare_qtr_mth'] < -0.005]
                cov_neg = compute_realized_cov(daily_return_2.values, prices_df)

                cov_neg = pd.DataFrame(cov_neg, index=prices_df.columns, columns=prices_df.columns)
                cov_all = pd.DataFrame(covars, index=prices_df.columns, columns=prices_df.columns)

                self.cov = (1 + self.tau) * (cov_all * 0.7 + cov_neg * 0.3)

                lb = 0.03
                ub = 0.3

                weight_array = risk_parity(self.cov, lb, ub, -1e7).flatten()
                weight = pd.DataFrame(weight_array.T, index=self.cov.index, columns=['weight'])

                weight_path = os.path.join(addpath.data_path, "Combine", "weights")
                if os.path.exists(weight_path):
                    pass
                else:
                    os.makedirs(weight_path)

                weight.to_csv(
                    os.path.join(weight_path, self.formation_dates[self.rebalance_count].strftime('%Y-%m-%d') + ".csv"),
                    encoding='UTF-8-sig')

                trading_shares_list_tmp = pd.Series()
                for stock in weight.index:
                    trading_shares_list_tmp[stock] = int(
                        self.portfolio_manager.pv * weight.loc[stock, 'weight'] / close_df.loc[current_date, stock])
                trading_shares_list = pd.DataFrame(index=self.symbol_batch)
                trading_shares_list['trading_shares'] = trading_shares_list_tmp
                trading_shares_list['trading_shares'] = trading_shares_list['trading_shares'].fillna(0)
                holding_shares = trading_shares_list['trading_shares'].values.reshape(1, -1)
                trading_shares = holding_shares - self.holding_shares
                self.holding_shares = holding_shares
                self.rebalance_count += 1
            else:
                trading_shares = np.zeros(len(self.symbol_batch)).reshape(1, -1)
        else:
            trading_shares = np.zeros(len(self.symbol_batch)).reshape(1, -1)

        return trading_shares

    def load_status(self, pickle_dict):
        strategy_status = pickle_dict
        self.status = strategy_status

    def save_status(self):
        '''可以把需要的status存进strategy_status
           不存任何状态会存一个空的'''
        strategy_status = Dict.empty(types.unicode_type, nb.float64)
        self.status = strategy_status


@jitclass({
    'portfolio_manager': pms_type,
    'ccy_matrix': nb.float64[:, :],
    'strategy_name': types.unicode_type,
    'strategy_param': nb.float64[:],
    'symbol_batch': types.ListType(types.unicode_type),
    'ccy_list': types.ListType(types.unicode_type),
    'symbol_type': types.ListType(types.unicode_type),
    'status': types.DictType(types.unicode_type, nb.float64),
    'current_fx_data': nb.float64[:],
    'current_data': nb.float64[:],
    'is_tradable': nb.boolean[:],
    'trade_time': nb.int64[:],
    'trade_price': nb.float64[:]
})
class fix_weight(BaseStrategy):

    def init(self):
        self.selected_stock_path = os.path.join(addpath.data_path, "Ashare", "selected_stocks", "semiyear")

        self.rebalance_count = 0

        formation_dates = os.listdir(self.selected_stock_path)
        self.formation_dates = [datetime.strptime(formation_date[:-4], "%Y-%m-%d") for formation_date in formation_dates]
        self.formation_dates = [formation_date for formation_date in self.formation_dates if formation_date.month == 4]
        self.holding_shares = np.zeros(len(self.symbol_batch)).reshape(1, -1)

        self.tau = 0.025

    def on_data(self, data_dict: dict, time_dict: dict, ref_data_dict: dict, ref_time_dict: dict):
        current_date = datetime(time_dict['main'][-1, 3], time_dict['main'][-1, 4], time_dict['main'][-1, 5], 0, 0, 0)

        if self.rebalance_count < len(self.formation_dates):
            if current_date >= self.formation_dates[self.rebalance_count]:

                close_list = [rec[:, -1] for rec in data_dict['main']]
                close_time = []
                for idx in range(len(close_list)):
                    close_time.append(
                        datetime(time_dict['main'][idx, 3], time_dict['main'][idx, 4], time_dict['main'][idx, 5], 0, 0, 0))
                close_df = pd.DataFrame(close_list, index=close_time, columns=self.symbol_batch)

                weight = {}
                weight['161119.OF'] = 0.12
                weight['006491.OF'] = 0.12
                weight['007366.OF'] = 0.09
                weight['003376.OF'] = 0.06
                weight['511010.SH'] = 0.1
                weight['511260.SH'] = 0.08
                weight['006662.OF'] = 0.09
                weight['511360.SH'] = 0.09
                weight['519718.OF'] = 0.05

                trading_shares_list_tmp = pd.Series()
                for stock in weight.keys():
                    trading_shares_list_tmp[stock] = int(self.portfolio_manager.pv * weight[stock] / close_df.loc[current_date, stock])
                trading_shares_list = pd.DataFrame(index=self.symbol_batch)
                trading_shares_list['trading_shares'] = trading_shares_list_tmp
                trading_shares_list['trading_shares'] = trading_shares_list['trading_shares'].fillna(0)
                holding_shares = trading_shares_list['trading_shares'].values.reshape(1, -1)
                trading_shares = holding_shares - self.holding_shares
                self.holding_shares = holding_shares
                self.rebalance_count += 1
            else:
                trading_shares = np.zeros(len(self.symbol_batch)).reshape(1, -1)
        else:
            trading_shares = np.zeros(len(self.symbol_batch)).reshape(1, -1)

        return trading_shares

    def load_status(self, pickle_dict):
        strategy_status = pickle_dict
        self.status = strategy_status

    def save_status(self):
        '''可以把需要的status存进strategy_status
           不存任何状态会存一个空的'''
        strategy_status = Dict.empty(types.unicode_type, nb.float64)
        self.status = strategy_status