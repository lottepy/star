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
class EqualWeighting(BaseStrategy):

    def on_data(self, data_dict: dict, time_dict: dict, ref_data_dict: dict, ref_time_dict: dict):
        n_security = len(self.metadata['main']['symbols'])
        if (self.portfolio_manager.current_holding != np.zeros(n_security)).all():
            random_weight = np.zeros(n_security)
        else:
            random_weight = np.ones(n_security) / n_security * 0.5
        ref_aum = self.portfolio_manager.init_cash
        ref_aum = min(ref_aum,100000)
        order = self.return_reserve_target(ref_aum * random_weight).astype(int)
        return order.reshape(1, -1)
        """ target_position = (ref_aum * random_weight) / data_dict['main'][-1, :, 3]
        return self.return_target(target_position).reshape(1, -1) """

    def load_status(self, pickle_dict):
        strategy_status = pickle_dict
        self.status = strategy_status

    def save_status(self):
        '''可以把需要的status存进strategy_status
           不存任何状态会存一个空的'''
        strategy_status = Dict.empty(types.unicode_type, nb.float64)
        self.status = strategy_status
