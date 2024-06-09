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
import pandas as pd
from numpy import array, zeros, sqrt, dot, append, mean, eye, arange
from datetime import datetime
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
class ew_ashare(BaseStrategy):

    def init(self):
        self.rebalance_count = 0
        self.selected_stock_path = os.path.join(addpath.data_path, "Ashare", "selected_stocks")
        formation_dates = os.listdir(self.selected_stock_path)
        self.formation_dates = [datetime.strptime(formation_date[:-4], "%Y-%m-%d") for formation_date in formation_dates]
        self.formation_dates = [formation_date for formation_date in self.formation_dates if formation_date.month != 5 and formation_date.month != 8]
        self.holding_shares = np.zeros(len(self.symbol_batch)).reshape(1, -1)

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
                close_df = close_df.iloc[-WINDOW_SIZE * 5:, :]
                stock_list = pd.read_csv(os.path.join(self.selected_stock_path, self.formation_dates[self.rebalance_count].strftime("%Y-%m-%d") + ".csv"))['Stkcd'].tolist()

                trading_shares_list_tmp = pd.Series()
                for stock in stock_list:
                    trading_shares_list_tmp[stock] = int(self.portfolio_manager.pv / len(stock_list) / close_df.loc[current_date, stock])
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
class score_wt_ashare(BaseStrategy):

    def init(self):
        self.rebalance_count = 0
        self.rank_path = os.path.join(addpath.data_path, "Ashare", "monthly_prior", "monthly_stock_rank")
        formation_dates = os.listdir(self.rank_path)
        self.formation_dates = [datetime.strptime(formation_date[:-4], "%Y-%m-%d") for formation_date in formation_dates]
        self.holding_shares = np.zeros(len(self.symbol_batch)).reshape(1, -1)

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
                close_df = close_df.iloc[-WINDOW_SIZE * 5:, :]
                score = pd.read_csv(os.path.join(self.rank_path, self.formation_dates[self.rebalance_count].strftime("%Y-%m-%d") + ".csv"), index_col=0)
                score['weight'] = score['score'] / score['score'].sum()

                holding_shares_list_tmp = pd.Series()
                for stock in score.index:
                    holding_shares_list_tmp[stock] = int(self.portfolio_manager.pv * score.loc[stock, 'weight'] / close_df.loc[current_date, stock])
                holding_shares_list = pd.DataFrame(index=self.symbol_batch)
                holding_shares_list['trading_shares'] = holding_shares_list_tmp
                holding_shares_list['trading_shares'] = holding_shares_list['trading_shares'].fillna(0)
                holding_shares = holding_shares_list['trading_shares'].values.reshape(1, -1)
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