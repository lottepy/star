import numpy as np
import pandas as pd
import numba as nb
from numba.typed import Dict, List
from numba import types
from numba.experimental import jitclass
from quantcycle.app.signal_generator.base_strategy import defaultjitclass, BaseStrategy
from quantcycle.app.pms_manager.portfolio_manager import PorfolioManager

import quantcycle.engine.backtest_engine

try:
    pms_type = PorfolioManager.class_type.instance_type
except:
    pms_type = "PMS_type"

@defaultjitclass()
class fake_strategy(BaseStrategy):

    def init(self):
        self.no_meaning_variable = True

    def on_data(self, data_dict: dict, time_dict: dict, ref_data_dict: dict, ref_time_dict: dict):

        return np.zeros(len(self.metadata['main']['symbols'])).reshape(1, -1)

    def load_status(self, pickle_dict):
        strategy_status = pickle_dict
        self.status = strategy_status

    def save_status(self):
        '''可以把需要的status存进strategy_status
           不存任何状态会存一个空的'''
        strategy_status = Dict.empty(types.unicode_type, nb.float64)
        self.status = strategy_status