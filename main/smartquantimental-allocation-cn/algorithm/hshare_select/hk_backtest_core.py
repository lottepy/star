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
class hk_backtest_core(BaseStrategy):
    def init(self):
        # self.order_manager = order_manager
        # self.strategy_utility = strategy_utility
        # self.symbol_batch = self.strategy_utility.symbol_batch
        # self.strategy_param = self.strategy_utility.strategy_param
        # self.n_security = len(self.strategy_utility.symbol_batch)
        # self.portfolio_name = self.strategy_param[0]

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

        # self.rebalance_freq=list(REB_FREQ.keys())[int(self.strategy_param[0])]
        self.start_year=self.strategy_param[0]
        self.start_month=self.strategy_param[1]
        self.start_date=self.strategy_param[2]
        self.end_year=self.strategy_param[3]
        self.end_month=self.strategy_param[4]
        self.end_date=self.strategy_param[5]
        # self.strategy_code=self.strategy_param[7]

        self.rebalancing_dates = pd.read_csv(join(addpath.config_path,'formation_date_hk.csv'),parse_dates=[0])['formation_date']
        self.rebalancing_dates=self.rebalancing_dates[self.rebalancing_dates<datetime.today()]


    # @profile
    def on_data(self, data_dict: dict, time_dict: dict, ref_data_dict: dict, ref_time_dict: dict):

        current_date = datetime(time_dict['main'][-1, 3], time_dict['main'][-1, 4], time_dict['main'][-1, 5], 0, 0, 0)
        last_date=datetime(time_dict['main'][-2, 3], time_dict['main'][-2, 4], time_dict['main'][-2, 5], 0, 0, 0)
        portfolio_path=join(addpath.data_path,'Hshare','portfolio')
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
        if current_date==datetime(2019,10,1):
            print(current_date)

        if len(rebalance_date)>0:
            symbol_batch=[]
            self.symbol_batch=self.metadata['main']['symbols']
            for symbol in self.symbol_batch:
                symbol_batch.append(symbol[:-3])
            rebalance_date=rebalance_date.iloc[0,0]
            portfolio_file_path = join(portfolio_path, rebalance_date.strftime("%Y-%m-%d") + ".csv")
            portfolio_file = pd.read_csv(portfolio_file_path, index_col=0)
            # ref_aum = self.order_manager.ref_aum
            # cum_pnl = sum(self.order_manager.pnl)
            weight = pd.DataFrame(index=symbol_batch)
            weight['weight']=portfolio_file['weight']
            adj_close = pd.DataFrame(data_dict['main'][-1, :][:, 3], columns=['CLOSE'], index=symbol_batch)
            weight['CLOSE']=portfolio_file['CLOSE']
            weight['ADJ_CLOSE']=adj_close
            self.scaling_weight=weight['weight'].fillna(0).values

            lot_sizes = pd.read_csv(join(addpath.config_path, 'Hshare_symbol_list.csv'), index_col=0)

            lot_sizes_df=lot_sizes.loc[symbol_batch,['LOTSIZE']]
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

            trading_shares_list_tmp = pd.Series([shares for shares in trading_shares_list], index=symbol_batch)
            trading_shares_list = pd.DataFrame(index=symbol_batch)
            trading_shares_list['trading_shares'] = trading_shares_list_tmp
            trading_shares_list['trading_shares'] = trading_shares_list['trading_shares'].fillna(0)
            trading_shares = trading_shares_list['trading_shares'].values.reshape(1, -1)
        else:
            trading_shares = np.zeros(len(self.symbol_batch)).reshape(1, -1)
        return trading_shares
