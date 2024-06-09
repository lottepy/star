
from algorithm import addpath
from algorithm.utils.quantcycle_helper import get_buy_share_number
import sys
import math
import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
import pytz
from tabulate import tabulate
from os.path import dirname, abspath, join, exists
from os import makedirs, listdir
from datetime import datetime
import numba as nb
from numba.typed import Dict, List
from numba import types
from numba.experimental import jitclass
from quantcycle.app.signal_generator.base_strategy import defaultjitclass, BaseStrategy
from quantcycle.app.pms_manager.portfolio_manager import PorfolioManager
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import quantcycle.engine.backtest_engine
try:
    pms_type = PorfolioManager.class_type.instance_type
except:
    pms_type = "PMS_type"

@defaultjitclass()
class benchmark(BaseStrategy):
    def init(self):
        self.ratio = self.strategy_param[0]
        self.rebalance_dates = pd.read_csv(join(addpath.config_path, 'rebalance_dates.csv'), parse_dates=[0]).iloc[:,0].astype(str).tolist()
        
        # ['LG13TRUU Index', 'LGTRTRUU Index', 'MXWD Index']
        self.weight = np.array([1-self.ratio, 0, self.ratio]) # new
        # self.weight = np.array([0, 1-self.ratio, self.ratio]) # old
        self.lot_size = 1.0
        self.trading_buffer = 0
        
        
    def on_data(self, data_dict: dict, time_dict: dict, ref_data_dict: dict, ref_time_dict: dict):
        current_date = datetime(time_dict['main'][-1, 3], time_dict['main'][-1, 4], time_dict['main'][-1, 5], 0, 0, 0)
        self.current_date = current_date
        close_list = [rec[:, -1] for rec in data_dict['main']]
        close_time = []
        for idx in range(len(close_list)):
            close_time.append(datetime(time_dict['main'][idx, 3], time_dict['main'][idx, 4], time_dict['main'][idx, 5], 0, 0, 0))
        close_df = pd.DataFrame(close_list, index=close_time, columns=self.metadata['main']['symbols'])
        
        if str(current_date)[:10] in self.rebalance_dates:
            sharesToBuy = get_buy_share_number(
                self.portfolio_manager.pv,
                self.weight,
                close_df.iloc[-1].values,
                self.lot_size,
                self.trading_buffer
            )
            
            trading_shares = sharesToBuy[0]
            current_holding = self.portfolio_manager.current_holding
            trading_shares = trading_shares - current_holding
            trading_shares = trading_shares.reshape(1,-1).astype('int64')
        else:
            trading_shares = np.zeros(len(self.metadata['main']['symbols'])).reshape(1, -1)
                                      
        return trading_shares
        
        
        