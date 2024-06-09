import numpy as np
from os.path import join
from datetime import datetime
import pandas as pd
from algorithm import addpath
import numba as nb
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
class bond_backtest_core(BaseStrategy):
    def init(self):

        # self.rebalance_freq=list(REB_FREQ.keys())[int(self.strategy_param[0])]
        self.start_year=self.strategy_param[0]
        self.start_month=self.strategy_param[1]
        self.start_date=self.strategy_param[2]
        self.end_year=self.strategy_param[3]
        self.end_month=self.strategy_param[4]
        self.end_date=self.strategy_param[5]
        self.symbol_batch = self.metadata['main']['symbols']
        # self.strategy_code=self.strategy_param[7]

        self.rebalancing_dates = pd.read_csv(join(addpath.config_path,'formation_date_bond.csv'),parse_dates=[0])['formation_date']
        self.rebalancing_dates=self.rebalancing_dates[self.rebalancing_dates<datetime.today()]


    # @profile
    def on_data(self, data_dict: dict, time_dict: dict, ref_data_dict: dict, ref_time_dict: dict):

        current_date = datetime(time_dict['main'][-1, 3], time_dict['main'][-1, 4], time_dict['main'][-1, 5], 0, 0, 0)
        last_date=datetime(time_dict['main'][-2, 3], time_dict['main'][-2, 4], time_dict['main'][-2, 5], 0, 0, 0)
        portfolio_path=join(addpath.data_path,'bond','portfolio')
        rebalance_date=pd.DataFrame(self.rebalancing_dates)
        rebalance_date=rebalance_date[(rebalance_date>last_date)&(rebalance_date<=current_date)].dropna()

        close_list = [rec[:, -1] for rec in data_dict['main']]
        close_time = []
        for idx in range(len(close_list)):
            close_time.append(
                datetime(time_dict['main'][idx, 3], time_dict['main'][idx, 4], time_dict['main'][idx, 5], 0, 0, 0))
        close_df = pd.DataFrame(close_list, index=close_time, columns=self.symbol_batch)
        WINDOW_SIZE=52
        close_df = close_df.iloc[-WINDOW_SIZE * 5:, :]


        if len(rebalance_date)>0:
            rebalance_date=rebalance_date.iloc[0,0]
            portfolio_file_path = join(portfolio_path, rebalance_date.strftime("%Y-%m-%d") + ".csv")
            portfolio_file = pd.read_csv(portfolio_file_path, index_col=0)

            weight = pd.DataFrame(index=self.symbol_batch)
            weight['weight']=0
            weight.loc[portfolio_file.index,'weight']=portfolio_file['weight']
            self.scaling_weight=weight['weight'].fillna(0).values
            weight['CLOSE'] = close_df.iloc[-1, :]

            weight['target_shares']=self.portfolio_manager.pv*weight['weight']/weight['CLOSE']
            self.target_shares = weight['target_shares'].fillna(0).values
            sharesToBuy=self.target_shares-self.portfolio_manager.current_holding
            trading_shares_list = list(sharesToBuy)

            trading_shares_list_tmp = pd.Series([shares for shares in trading_shares_list], index=self.symbol_batch)
            trading_shares_list = pd.DataFrame(index=self.symbol_batch)
            trading_shares_list['trading_shares'] = trading_shares_list_tmp
            trading_shares_list['trading_shares'] = trading_shares_list['trading_shares'].fillna(0)
            trading_shares = trading_shares_list['trading_shares'].values.reshape(1, -1)
        else:
            trading_shares = np.zeros(len(self.symbol_batch)).reshape(1, -1)
        return trading_shares
