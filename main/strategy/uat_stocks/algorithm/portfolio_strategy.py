import numba as nb
import numpy as np
from numba import types
from numba.typed import Dict
from quantcycle.app.pms_manager.portfolio_manager import PorfolioManager
from quantcycle.app.signal_generator.base_strategy import BaseStrategy

try:
    pms_type = PorfolioManager.class_type.instance_type
except AttributeError:
    pms_type = "PMS_type"


class PortfolioStrategy(BaseStrategy):

    def init(self):
        pass

    def on_data(self, data_dict, time_dict, ref_data_dict, ref_time_dict):
        current_time = time_dict['main'][-1, 0]
        keys = [i for i in data_dict.keys() if i not in ['main', 'fxrate', 'int']]
        self.symbol_batch = self.metadata["main"]["symbols"]
        n_security = len(self.symbol_batch)
        position_list = []
        for k in keys:
            if time_dict[k][-1, 0] == current_time:
                position_list.append(data_dict[k][-1, 0, :].tolist())
            else:
                position_list.append(np.zeros(n_security).tolist())
        total_position = np.array(position_list)
        final_position = np.mean(total_position, axis=0)
        order = self.return_target(final_position)
        return order.reshape(1, -1)

    def load_status(self, pickle_dict):
        strategy_status = pickle_dict
        self.status = strategy_status

    def save_status(self):
        '''可以把需要的status存进strategy_status
           不存任何状态会存一个空的'''
        strategy_status = Dict.empty(types.unicode_type, nb.float64)
        self.status = strategy_status
