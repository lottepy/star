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
class EW_strategy(BaseStrategy):

    def init(self):
        self.index = 0

    def on_data(self, data_dict: dict, time_dict: dict, ref_data_dict: dict, ref_time_dict: dict):
        symbol_batch = self.metadata["main"]["symbols"]
        max_exp = 800000 if self.index%2 else 1600000
        target = np.zeros(len(symbol_batch))
        if self.index == 0 and not (self.portfolio_manager.current_holding == np.zeros(len(symbol_batch))).all():
            pass
        else:
            #pv = self.portfolio_manager.pv / ((self.index%2+1) * 100)
            target = np.ones(len(symbol_batch)) * max_exp / len(symbol_batch)
        self.index += 1
        self.update_backup_symbols(["HK_10_1"])
        order = self.return_reserve_target_base_ccy(target)
        if not np.isnan(order[0]):
            order[0] = int(order[0])
        return order.reshape(1, -1)

    def load_status(self, pickle_dict):
        strategy_status = pickle_dict
        self.status = strategy_status

    def save_status(self):
        '''可以把需要的status存进strategy_status
           不存任何状态会存一个空的'''
        strategy_status = Dict.empty(types.unicode_type, nb.float64)
        self.status = strategy_status
