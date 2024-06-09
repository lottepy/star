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
from cvxopt import solvers
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
        freq_n = self.strategy_param[0]
        factor_rotation_n = self.strategy_param[1]
        industry_level_n = self.strategy_param[2]

        if freq_n == 6.0:
            self.freq = "semiyear"
        elif freq_n == 3.0:
            self.freq = "qtr"

        if industry_level_n == 1.0:
            self.industry_level = "SW1"
        elif industry_level_n == 2.0:
            self.industry_level = "SW2"
        else:
            self.industry_level = "AQM_Category"

        if factor_rotation_n == 1.0:
            self.factor_rotation = True
            self.selected_stock_path = os.path.join(addpath.data_path, "Ashare", "selected_stocks", self.industry_level, self.freq)
        else:
            self.factor_rotation = False
            self.selected_stock_path = os.path.join(addpath.data_path, "Ashare", "selected_stocks")

        self.rebalance_count = 0

        formation_dates = os.listdir(self.selected_stock_path)
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
                score = pd.read_csv(os.path.join(self.selected_stock_path,
                                                 self.formation_dates[self.rebalance_count].strftime(
                                                     "%Y-%m-%d") + ".csv"), index_col='Stkcd')

                weight_path = os.path.join(addpath.data_path, "Ashare", "weights", "ew_ashare", self.industry_level, self.freq, str(self.factor_rotation.real))
                if os.path.exists(weight_path):
                    pass
                else:
                    os.makedirs(weight_path)

                symbol_list = pd.read_csv(os.path.join(addpath.config_path, "ashare_symbol_list.csv"), index_col=0)
                ranks = score[['rank']]
                ranks['SW1'] = symbol_list['SW1']
                ranks['prior_weight'] = 1 / ranks.shape[0]

                if self.industry_level != "SW1" and self.industry_level != "SW2":
                    ranks['SW1'] = ranks['SW1'].replace("建筑建材", "建筑材料")

                    SW1 = ranks[['SW1']].drop_duplicates().set_index('SW1')

                    ub_industry = pd.DataFrame(index=ranks.index)
                    for i in range(SW1.shape[0]):
                        ub_industry[str(i) + '_ub_industry'] = np.where(ranks['SW1'] == SW1.index[i], 1, 0)

                    P = np.eye(ranks.shape[0])
                    q = -2 * np.array(ranks['prior_weight'].tolist()).reshape(ranks.shape[0], 1)

                    G = np.vstack([np.eye(ranks.shape[0]), -np.eye(ranks.shape[0]), ub_industry.transpose().values])
                    h = np.vstack([np.ones(ranks.shape[0]).reshape(ranks.shape[0], 1) * 0.03,
                                   -np.ones(ranks.shape[0]).reshape(ranks.shape[0], 1) * 0.001,
                                   np.ones(SW1.shape[0]).reshape(SW1.shape[0], 1) * 0.25
                                   ])
                    A = np.vstack([np.ones(ranks.shape[0]).reshape(1, ranks.shape[0])])
                    b = np.vstack([np.ones(1).reshape(1, 1)])

                    solvers.options['show_progress'] = False
                    solvers.options['abstol'] = 1e-21
                    optimized = solvers.qp(
                        cvxopt.matrix(P),
                        cvxopt.matrix(q),
                        cvxopt.matrix(G),
                        cvxopt.matrix(h),
                        cvxopt.matrix(A),
                        cvxopt.matrix(b)
                    )

                    ranks['weight'] = optimized['x']
                else:
                    ranks['weight'] = ranks['prior_weight']
                weight = ranks[['weight']]
                weight.to_csv(
                    os.path.join(weight_path, self.formation_dates[self.rebalance_count].strftime('%Y-%m-%d') + ".csv"),
                    encoding='UTF-8-sig')

                trading_shares_list_tmp = pd.Series()
                for stock in weight.index:
                    trading_shares_list_tmp[stock] = int(self.portfolio_manager.pv * weight.loc[stock, 'weight'] / close_df.loc[current_date, stock])
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
class qtr_sw_capped_ashare(BaseStrategy):

    def init(self):
        self.rebalance_count = 0
        self.selected_stock_path = os.path.join(addpath.data_path, "Ashare", "selected_stocks")
        formation_dates = os.listdir(self.selected_stock_path)
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
                score = pd.read_csv(os.path.join(self.selected_stock_path, self.formation_dates[self.rebalance_count].strftime("%Y-%m-%d") + ".csv"), index_col='Stkcd')

                weight_path = os.path.join(addpath.data_path, "Ashare", "weights")
                if os.path.exists(weight_path):
                    pass
                else:
                    os.makedirs(weight_path)

                symbol_list = pd.read_csv(os.path.join(addpath.config_path, "ashare_symbol_list.csv"), index_col=0)
                ranks = score[['score']]
                ranks['SW1'] = symbol_list['SW1']
                ranks['pos_score'] = ranks['score'].map(lambda x: x + 1 if x >= 0 else 1 / (1 - x))
                ranks['prior_weight'] = ranks['pos_score'] / ranks['pos_score'].sum()
                ranks['SW1'] = ranks['SW1'].replace("建筑建材", "建筑材料")

                SW1 = ranks[['SW1']].drop_duplicates().set_index('SW1')

                ub_industry = pd.DataFrame(index=ranks.index)
                for i in range(SW1.shape[0]):
                    ub_industry[str(i) + '_ub_industry'] = np.where(ranks['SW1'] == SW1.index[i], 1, 0)

                P = np.eye(ranks.shape[0])
                q = -2 * np.array(ranks['prior_weight'].tolist()).reshape(ranks.shape[0], 1)

                G = np.vstack([np.eye(ranks.shape[0]), -np.eye(ranks.shape[0]), ub_industry.transpose().values])
                h = np.vstack([np.ones(ranks.shape[0]).reshape(ranks.shape[0], 1) * 0.03,
                               -np.ones(ranks.shape[0]).reshape(ranks.shape[0], 1) * 0.001,
                               np.ones(SW1.shape[0]).reshape(SW1.shape[0], 1) * 0.25
                               ])
                A = np.vstack([np.ones(ranks.shape[0]).reshape(1, ranks.shape[0])])
                b = np.vstack([np.ones(1).reshape(1, 1)])

                solvers.options['show_progress'] = False
                solvers.options['abstol'] = 1e-21
                optimized = solvers.qp(
                    cvxopt.matrix(P),
                    cvxopt.matrix(q),
                    cvxopt.matrix(G),
                    cvxopt.matrix(h),
                    cvxopt.matrix(A),
                    cvxopt.matrix(b)
                )

                ranks['weight'] = optimized['x']
                weight = ranks[['weight']]
                weight.to_csv(os.path.join(weight_path, self.formation_dates[self.rebalance_count].strftime('%Y-%m-%d') + ".csv"),
                             encoding='UTF-8-sig')

                trading_shares_list_tmp = pd.Series()
                for stock in weight.index:
                    trading_shares_list_tmp[stock] = int(self.portfolio_manager.pv * weight.loc[stock, 'weight'] / close_df.loc[current_date, stock])

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
        freq_n = self.strategy_param[0]
        factor_rotation_n = self.strategy_param[1]
        industry_level_n = self.strategy_param[2]

        if freq_n == 6.0:
            self.freq = "semiyear"
        elif freq_n == 3.0:
            self.freq = "qtr"

        if industry_level_n == 1.0:
            self.industry_level = "SW1"
        elif industry_level_n == 2.0:
            self.industry_level = "SW2"
        else:
            self.industry_level = "AQM_Category"

        if factor_rotation_n == 1.0:
            self.factor_rotation = True
            self.rank_path = os.path.join(addpath.data_path, "Ashare", "monthly_prior", "monthly_stock_rank", self.industry_level, self.freq)
        else:
            self.factor_rotation = False
            self.rank_path = os.path.join(addpath.data_path, "Ashare", "selected_stocks")

        self.rebalance_count = 0

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
                weight_path = os.path.join(addpath.data_path, "Ashare", "weights", "score_wt_ashare", self.industry_level, self.freq, str(self.factor_rotation.real))
                if os.path.exists(weight_path):
                    pass
                else:
                    os.makedirs(weight_path)
                score.to_csv(os.path.join(weight_path, self.formation_dates[self.rebalance_count].strftime('%Y-%m-%d') + ".csv"),
                             encoding='UTF-8-sig')

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
class score_wt_timed_ashare(BaseStrategy):

    def init(self):
        freq_n = self.strategy_param[0]
        factor_rotation_n = self.strategy_param[1]
        industry_level_n = self.strategy_param[2]

        if freq_n == 6.0:
            self.freq = "semiyear"
        elif freq_n == 3.0:
            self.freq = "qtr"

        if industry_level_n == 1.0:
            self.industry_level = "SW1"
        elif industry_level_n == 2.0:
            self.industry_level = "SW2"
        else:
            self.industry_level = "AQM_Category"

        if factor_rotation_n == 1.0:
            self.factor_rotation = True
            self.rank_path = os.path.join(addpath.data_path, "Ashare", "monthly_prior", "monthly_stock_rank",
                                          self.industry_level, self.freq)
        else:
            self.factor_rotation = False
            self.rank_path = os.path.join(addpath.data_path, "Ashare", "selected_stocks")

        self.rebalance_count = 0

        formation_dates = os.listdir(self.rank_path)
        self.formation_dates = [datetime.strptime(formation_date[:-4], "%Y-%m-%d") for formation_date in
                                formation_dates]
        self.holding_shares = np.zeros(len(self.symbol_batch)).reshape(1, -1)
        self.signal = pd.read_csv(os.path.join(addpath.data_path, "Ashare", "market_timing", "signal", "mon_end_300.csv"), index_col='date', parse_dates=['date'])
        self.signal = self.signal.resample('1M').last().ffill()

    def on_data(self, data_dict: dict, time_dict: dict, ref_data_dict: dict, ref_time_dict: dict):
        current_date = datetime(time_dict['main'][-1, 3], time_dict['main'][-1, 4], time_dict['main'][-1, 5], 0, 0, 0)

        if self.rebalance_count < len(self.formation_dates):
            if current_date >= self.formation_dates[self.rebalance_count]:
                close_list = [rec[:, -1] for rec in data_dict['main']]
                close_time = []
                for idx in range(len(close_list)):
                    close_time.append(
                        datetime(time_dict['main'][idx, 3], time_dict['main'][idx, 4], time_dict['main'][idx, 5], 0, 0,
                                 0))
                close_df = pd.DataFrame(close_list, index=close_time, columns=self.symbol_batch)
                close_df = close_df.iloc[-WINDOW_SIZE * 5:, :]
                score = pd.read_csv(os.path.join(self.rank_path, self.formation_dates[self.rebalance_count].strftime(
                    "%Y-%m-%d") + ".csv"), index_col=0)
                score['weight'] = score['score'] / score['score'].sum() * (self.signal.loc[self.formation_dates[self.rebalance_count], 'adj'] + 1) / 2
                weight_path = os.path.join(addpath.data_path, "Ashare", "weights", "score_wt_timed_ashare", self.industry_level, self.freq)
                if os.path.exists(weight_path):
                    pass
                else:
                    os.makedirs(weight_path)
                score.to_csv(
                    os.path.join(weight_path, self.formation_dates[self.rebalance_count].strftime('%Y-%m-%d') + ".csv"),
                    encoding='UTF-8-sig')

                holding_shares_list_tmp = pd.Series()
                for stock in score.index:
                    holding_shares_list_tmp[stock] = int(
                        self.portfolio_manager.pv * score.loc[stock, 'weight'] / close_df.loc[current_date, stock])
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