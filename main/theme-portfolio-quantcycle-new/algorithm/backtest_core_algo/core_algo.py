import numpy as np
from numpy import array, zeros, sqrt, dot, append, mean, eye, arange
import math
import cvxopt
from os.path import join
from datetime import datetime
import pandas as pd
from cvxopt import solvers
from dateutil.relativedelta import relativedelta
from algorithm import addpath
from algorithm.backtest_engine.algorithm.strategy_utility.strategy_utility import StrategyUtility
from algorithm.backtest_engine.order.order_manager import OrderManagerBase as OrderManager
from algorithm.utils.helpers import cal_rebalancing_dates
from constants import *
from tabulate import tabulate
import numba as nb
from numba.typed import Dict, List
from numba import types
from numba.experimental import jitclass
from quantcycle.app.signal_generator.base_strategy import BaseStrategy
from quantcycle.app.pms_manager.portfolio_manager import PorfolioManager


try:
    pms_type = PorfolioManager.class_type.instance_type
except:
    pms_type = "PMS_type"


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
class aqumon_theme(BaseStrategy):
    def init(self):

        REB_FREQ = {
        "ANNUALLY": "12M",
        "SEMIANNUALLY": "6M",
        "QUARTERLY": "3M",
        "BIMONTHLY": "2M",
        "MONTHLY": "1M",
        "BIWEEKLY": "2W",
        "WEEKLY": "1W",
        "DAILY": "1D"
        }

        self.rebalance_freq=list(REB_FREQ.keys())[int(self.strategy_param[0])]
        self.start_year=self.strategy_param[1]
        self.start_month=self.strategy_param[2]
        self.start_date=self.strategy_param[3]
        self.end_year=self.strategy_param[4]
        self.end_month=self.strategy_param[5]
        self.end_date=self.strategy_param[6]
        self.strategy_code=self.strategy_param[7]
        self.task_code=self.strategy_param[8]

        if self.task_code==0:
            start = datetime(int(self.start_year), int(self.start_month), int(self.start_date))
            end = datetime(int(self.end_year), int(self.end_month), int(self.end_date))
            self.rebalancing_dates = cal_rebalancing_dates(start, end, self.rebalance_freq)
        else:
            portfolio_path=os.path.join(addpath.data_path,"tracking_weight",list(STRATEGY_CODE.keys())[int(self.strategy_code)],"portfolios")
            file_names=os.listdir(portfolio_path)
            if '.DS_Store' in file_names:
                file_names.remove('.DS_Store')
            rebalancing_dates=[datetime.strptime(file_name[:-4],'%Y-%m-%d') for file_name in file_names]
            self.rebalancing_dates=pd.DataFrame(rebalancing_dates)

    # @profile
    def on_data(self, data_dict: dict, time_dict: dict, ref_data_dict: dict, ref_time_dict: dict):

        current_date = datetime(time_dict['main'][-1, 3], time_dict['main'][-1, 4], time_dict['main'][-1, 5], 0, 0, 0)
        last_date=datetime(time_dict['main'][-2, 3], time_dict['main'][-2, 4], time_dict['main'][-2, 5], 0, 0, 0)
        if self.task_code==0:
            portfolio_path=list(STRATEGY_CODE.values())[int(self.strategy_code)]
        else:
            portfolio_path=os.path.join(addpath.data_path,"tracking_weight",list(STRATEGY_CODE.keys())[int(self.strategy_code)],"portfolios")
        rebalance_date=pd.DataFrame(self.rebalancing_dates)
        rebalance_date=rebalance_date[(rebalance_date>last_date)&(rebalance_date<=current_date)].dropna()

        # close_list = [rec[:, -1] for rec in data_dict['main']]
        # close_time = []
        # for idx in range(len(close_list)):
        #     close_time.append(
        #         datetime(time_dict['main'][idx, 3], time_dict['main'][idx, 4], time_dict['main'][idx, 5], 0, 0, 0))
        # close_df = pd.DataFrame(close_list, index=close_time, columns=self.symbol_batch)
        # WINDOW_SIZE=52
        # close_df = close_df.iloc[-WINDOW_SIZE * 5:, :]

        if len(rebalance_date)>0:
            rebalance_date=rebalance_date.iloc[0,0]
            print(rebalance_date)
            # symbol_batch=[]
            # for symbol in self.metadata['main']['symbols'].tolist():
            #     symbol_batch.append(symbol[:-3])
            portfolio_file_path = join(portfolio_path, rebalance_date.strftime("%Y-%m-%d") + ".csv")
            portfolio_file = pd.read_csv(portfolio_file_path, index_col=0)
            # ref_aum = self.order_manager.ref_aum
            # cum_pnl = sum(self.order_manager.pnl)
            weight = pd.DataFrame(index=self.metadata['main']['symbols'].tolist())
            weight['weight']=portfolio_file['weight']
            adj_close = pd.DataFrame(data_dict['main'][-1, :][:, 3], columns=['CLOSE'], index=self.metadata['main']['symbols'].tolist())
            weight['CLOSE']=portfolio_file['CLOSE']
            weight['ADJ_CLOSE']=adj_close
            self.scaling_weight=weight['weight'].fillna(0).values

            if list(STRATEGY_CODE.keys())[int(self.strategy_code)][:2]=='CN':
                lot_sizes=pd.read_csv(os.path.join(addpath.data_path,'cn_data','symbol_list.csv'),index_col=0)
            elif list(STRATEGY_CODE.keys())[int(self.strategy_code)][:2] == 'HK':
                lot_sizes = pd.read_csv(os.path.join(addpath.data_path, 'hk_data', 'symbol_list.csv'), index_col=0)
            else:
                lot_sizes = pd.read_csv(os.path.join(addpath.data_path, 'us_data', 'symbol_list.csv'), index_col=0)

            lot_sizes_df=lot_sizes.loc[self.metadata['main']['symbols'].tolist(),['LOTSIZE']]
            lot_sizes=lot_sizes_df['LOTSIZE'].tolist()
            weight['lotsize']=lot_sizes_df
            weight['target_position']=self.portfolio_manager.pv*weight['weight']/weight['CLOSE']/weight['lotsize']
            weight['target_position']=weight['target_position'].fillna(0).apply(int)
            weight['target_position']=weight['target_position']*weight['lotsize']*weight['CLOSE']/weight['ADJ_CLOSE']/weight['lotsize']
            #position表示手数   shares表示股数
            weight=weight.fillna(0)
            weight['target_position']=[shares for shares in weight['target_position']]
            weight['target_shares']=weight['target_position']*weight['lotsize']

            self.target_shares = weight['target_shares'].fillna(0).values
            sharesToBuy=self.target_shares-self.portfolio_manager.current_holding
            trading_shares_list = list(sharesToBuy)

            trading_shares_list_tmp = pd.Series([shares for shares in trading_shares_list], index=self.metadata['main']['symbols'].tolist())
            trading_shares_list = pd.DataFrame(index=self.metadata['main']['symbols'].tolist())
            trading_shares_list['trading_shares'] = trading_shares_list_tmp
            trading_shares_list['trading_shares'] = trading_shares_list['trading_shares'].fillna(0)
            trading_shares = trading_shares_list['trading_shares'].values.reshape(1, -1)
        else:
            trading_shares = np.zeros(len(self.metadata['main']['symbols'].tolist())).reshape(1, -1)
        return trading_shares
