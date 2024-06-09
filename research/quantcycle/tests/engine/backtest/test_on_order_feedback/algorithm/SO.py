import numpy as np
import numba as nb
from numba.typed import Dict, List
from numba import types
from numba.experimental import jitclass
from quantcycle.app.signal_generator.base_strategy import defaultjitclass, BaseStrategy
from quantcycle.app.pms_manager.portfolio_manager import PorfolioManager

try:
    pms_type = PorfolioManager.class_type.instance_type
except:
    pms_type = "PMS_type"


@defaultjitclass()
class SeparateOrder(BaseStrategy):
    # This is a strategy for testing on_order_feedback function
    # The strategy will only order CN stocks via on_data loop while order HK stocks via on_order_feedback loop
    def on_data(self, data_dict: dict, time_dict: dict, ref_data_dict: dict, ref_time_dict: dict):
        init_cash = self.portfolio_manager.init_cash / 10
        symbol_batch = self.metadata["main"]["symbols"]
        target = np.ones(len(symbol_batch)) * init_cash / len(symbol_batch)
        if time_dict['main'][0, -3] == 6 and time_dict['main'][0, -2] == 57:
            return self.return_reserve_target_base_ccy(target).reshape(1, -1)
        else:
            return None

    def on_order_feedback(self, trade_array, data_dict: dict, time_dict: dict, ref_data_dict: dict,
                          ref_time_dict: dict):
        init_cash = self.portfolio_manager.init_cash / 10
        symbol_batch = self.metadata["main"]["symbols"]
        target = np.ones(len(symbol_batch)) * init_cash / len(symbol_batch)
        if time_dict['main'][0, -3] == 8 and time_dict['main'][0, -2] == 0:
            return self.return_reserve_target_base_ccy(target).reshape(1, -1)
        else:
            return None

    def load_status(self, pickle_dict):
        strategy_status = pickle_dict
        self.status = strategy_status

    def save_status(self):
        '''可以把需要的status存进strategy_status
           不存任何状态会存一个空的'''
        strategy_status = Dict.empty(types.unicode_type, nb.float64)
        self.status = strategy_status
