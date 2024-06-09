import numpy as np
import pandas as pd
from datetime import datetime
from backtest_engine.utils.constants import Time
from backtest_engine.algorithm.strategy_utility.strategy_utility import StrategyUtility
from backtest_engine.order.order_manager_default import OrderManager


##===========================================================================##
class Weight_strategy:
    def __init__(self, order_manager: OrderManager, strategy_utility: StrategyUtility):
        self.order_manager = order_manager
        self.strategy_utility = strategy_utility
        self.weight_df = pd.read_csv(r"D:\work\backtesting\test_sample\v21\v21_daily\fx_daily_weight_v21.csv",index_col=0,parse_dates=True)
        n_security = len(self.strategy_utility.symbol_batch)

    # @profile
    def on_data(self, window_data, time_data, ref_window_data, ref_window_data_time, ref_window_factor, ref_window_factor_time):
        
        index = datetime(time_data[-1,Time.YEAR.value], time_data[-1,Time.MONTH.value], time_data[-1,Time.DAY.value])

        weight = self.weight_df.loc[index, self.strategy_utility.symbol_batch]

        ref_aum = self.order_manager.ref_aum
        self.strategy_utility.place_reserve_target_base_ccy(weight * ref_aum)
        #remark = {"var": var, "target_exp": weight * ref_aum, "window_data": list(np.around(window_data[-1, :, -1], decimals=3))}  # remark = {}
        #remark["pnl"] = self.order_manager.pnl
        self.strategy_utility.place_signal_remark({})


##===========================================================================##

