# -- coding: utf-8 --
from algorithm import addpath
import os
import numpy as np
import pandas as pd
from datetime import datetime, timezone, timedelta
import numba as nb
from numba.typed import Dict, List
from numba import types
from numba.experimental import jitclass
from quantcycle.app.signal_generator.base_strategy import BaseStrategy
from quantcycle.app.pms_manager.portfolio_manager import PorfolioManager
import quantcycle.engine.backtest_engine

try:
    pms_type = PorfolioManager.class_type.instance_type
except:
    pms_type = "PMS_type"
WINDOW_SIZE = 52

def get_buy_share_number(init_cap, opt_weight, price, board_lot, buffer):
    # input:
    # init_cap: initial capital; opt_weight: optimal weights; price: share prices; borad_lot: board lot; buffer: buffer.
    # output:
    # number of share to buy; drift.
    init_cap *= (1 - buffer)
    entry_price = price * board_lot
    opt_entry_num = opt_weight * init_cap / entry_price
    buy_entry_num = np.floor(opt_entry_num)
    entry_num_error = opt_entry_num - buy_entry_num

    # map_index = sorted(range(len(entry_num_error)), key=lambda k: entry_num_error[k], reverse=True)
    #
    # cost = sum(buy_entry_num * entry_price)
    # balance = init_cap - cost
    #
    # for iter in range(len(price)):
    #     if balance - entry_price[map_index[iter]] > buffer * init_cap:
    #         balance -= entry_price[map_index[iter]]
    #         buy_entry_num[map_index[iter]] += 1
    #     else:
    #         break

    buy_stock_num = buy_entry_num * board_lot
    buy_weight = buy_stock_num * price / init_cap
    error_weight = np.abs(buy_weight - opt_weight)
    drift = sum(error_weight) / 2
    return (buy_stock_num, drift)


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
class smartxray_jd(BaseStrategy):
    def init(self):
        # decode model parameters
        start_year = self.strategy_param[0]
        start_month = self.strategy_param[1]
        start_day = self.strategy_param[2]
        end_year = self.strategy_param[3]
        end_month = self.strategy_param[4]
        end_day = self.strategy_param[5]
        sector_preference = str(int(self.strategy_param[6]))
        invest_length = str(int(self.strategy_param[7]))
        risk_ratio = str(int(self.strategy_param[8]))

        self.bt_start = datetime(int(start_year), int(start_month), int(start_day))
        self.bt_end = datetime(int(end_year), int(end_month), int(end_day))
        # self.formation_dates = pd.date_range(self.bt_start, self.bt_end, freq='1M')
        self.rebalance_idx = 0
        self.lot_size = 1
        self.trading_buffer = 0.006


        dates = pd.read_csv(os.path.join(addpath.config_path, "JD","rebalance_dates.csv"), parse_dates=['rebalance_dates'])
        self.formation_dates = dates['rebalance_dates'].tolist()
        self.info = risk_ratio + '-A' + sector_preference + '-B' + invest_length

        trading_calendar = pd.read_csv(os.path.join(addpath.bundle_path, "Trading_Calendar.csv"), parse_dates=[0])
        self.trading_calendar = trading_calendar['data'].tolist()

    def on_data(self, data_dict: dict, time_dict: dict, ref_data_dict: dict, ref_time_dict: dict):
        current_date = datetime(time_dict['main'][-1, 3], time_dict['main'][-1, 4], time_dict['main'][-1, 5], 0, 0, 0)
        print("[ %s ] Portfolio Value = %s " % (current_date, str(self.portfolio_manager.pv)))
        if self.rebalance_idx < len(self.formation_dates):
            if current_date >= self.formation_dates[self.rebalance_idx] and current_date in self.trading_calendar:
                # if self.base_strategy != 1:
                current_date_str = self.formation_dates[self.rebalance_idx].strftime('%Y-%m-%d')
                close_list = [rec[:, -2] for rec in data_dict['main']]
                close_time = []
                for idx in range(len(close_list)):
                    close_time.append(
                        datetime(time_dict['main'][idx, 3], time_dict['main'][idx, 4], time_dict['main'][idx, 5], 0, 0, 0))
                close_df = pd.DataFrame(close_list, index=close_time, columns=self.metadata['main']['symbols'])
                close_df = close_df.iloc[-WINDOW_SIZE * 5:, :]

                # sr_path = os.path.join(addpath.data_path, "smart_rotation")
                portfolio_path = os.path.join(addpath.data_path, "bl_weights", "JD", self.info)

                portfolio = pd.read_csv(os.path.join(portfolio_path, self.formation_dates[self.rebalance_idx].strftime('%Y-%m-%d') + ".csv"), index_col=0)
                portfolio.index = portfolio.index.map(lambda x: x + ".CN")
                portfolio['close'] = close_df.iloc[-1].loc[portfolio.index]

                sharesToBuy = get_buy_share_number(
                    self.portfolio_manager.pv,
                    portfolio[current_date_str],
                    portfolio['close'].values,
                    self.lot_size,
                    self.trading_buffer
                )

                tradings = pd.DataFrame(index=self.metadata['main']['symbols'])
                tradings['current_position'] = self.portfolio_manager.current_holding
                tradings['new_position'] = sharesToBuy[0]
                tradings = tradings.fillna(0)
                tradings['trading_shares'] = tradings['new_position'] - tradings['current_position']
                trading_shares = tradings['trading_shares'].values.reshape(1, -1)

                self.rebalance_idx += 1
            else:
                trading_shares = np.zeros(len(self.metadata['main']['symbols'])).reshape(1, -1)
        else:
            trading_shares = np.zeros(len(self.metadata['main']['symbols'])).reshape(1, -1)


        return trading_shares

    def load_status(self, pickle_dict):
        strategy_status = pickle_dict
        self.status = strategy_status

    def save_status(self):
        '''可以把需要的status存进strategy_status
           不存任何状态会存一个空的'''
        strategy_status = Dict.empty(types.unicode_type, nb.float64)
        self.status = strategy_status
