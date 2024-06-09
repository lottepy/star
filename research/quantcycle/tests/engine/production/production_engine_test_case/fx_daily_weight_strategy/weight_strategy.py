import numpy as np
import pandas as pd
from quantcycle.app.signal_generator.base_strategy import defaultjitclass, BaseStrategy
from datetime import datetime,timedelta
from quantcycle.utils.production_constant import Time
##===========================================================================##
class Weight_strategy(BaseStrategy):
    def init(self):
        self.weight_df = pd.read_csv('tests/engine/production/production_engine_test_case/fx_daily_weight_strategy/fx_daily_weight_v21.csv',index_col=0,parse_dates=True)
        self.weight_df.index = pd.to_datetime(self.weight_df.index, format='%Y-%m-%d', utc=True)
        self.symbol_batch = self.metadata["main"]["symbols"]

    # @profile
    def on_data(self, data_dict: dict, time_dict: dict, ref_data_dict: dict, ref_time_dict: dict):

        init_cash = self.portfolio_manager.init_cash

        time_data=time_dict['main'][-1]
        index = datetime(int(time_data[3]), int(time_data[4]), int(time_data[5]))

        symbol_batch = list(map(lambda x: x.split(".")[0], self.symbol_batch))
        weight = self.weight_df.loc[index, symbol_batch]
        target = list(weight * init_cash)
        return self.return_reserve_target_base_ccy(target).reshape(1,-1)


##===========================================================================##



