import numpy as np
import numba as nb
from numba.typed import Dict, List
from numba import types
from numba.experimental import jitclass
from quantcycle.utils.indicator import RSI,Moving_series, moving_series_type
from quantcycle.app.signal_generator.base_strategy import defaultjitclass, BaseStrategy
from quantcycle.app.pms_manager.portfolio_manager import PorfolioManager

try:
    pms_type = PorfolioManager.class_type.instance_type
except:
    pms_type = "PMS_type"
##===========================================================================##


@defaultjitclass({
    'portfolio_manager': pms_type,
    'ccy_matrix': nb.float64[:, :],
    'strategy_name': types.unicode_type,
    'strategy_param': nb.float64[:],
    'symbol_batch': types.ListType(types.unicode_type),
    'ccy_list': types.ListType(types.unicode_type),
    'symbol_type': types.ListType(types.unicode_type),
    'current_fx_data': nb.float64[:],
    'current_data': nb.float64[:],
    'is_tradable': nb.boolean[:],
    'status':types.DictType(types.unicode_type, nb.float64),

})
class Test_Status_Record_Strategy(BaseStrategy):
#class RSI_strategy():
    # open long:   rsi(t-1)< 50 - break_threshold, rsi(t)>50 - break_threshold
    # open short:  rsi(t-1)> 50 + break_threshold, rsi(t)<50 + break_threshold
    # close long:   rsi(t-1)< 50 + break_threshold, rsi(t)>50 + break_threshold
    # close short:  rsi(t-1)> 50 - break_threshold, rsi(t)<50 - break_threshold
    def init(self):
        n_security = self.ccy_matrix.shape[1]
        length = np.int64(self.strategy_param[0])

    def save_status(self):
        '''可以把需要的status存进strategy_status
           不存任何状态会存一个空的'''
        strategy_status = Dict.empty(types.unicode_type, nb.float64)
        strategy_status['rsi'] = self.strategy_param[0]
        self.status = strategy_status

    def load_status(self, pickle_dict):
        self.status = pickle_dict

    def on_data(self, data_dict: dict, time_dict: dict, ref_data_dict: dict, ref_time_dict: dict):
        window_data = data_dict['main']
        time_data = time_dict['main'][:,:]
        current_data = window_data[-1,:,3]

        n_security = self.ccy_matrix.shape[1]

        ccp_target = self.portfolio_manager.current_holding.copy()
        ccp_target[ccp_target > 0] = 1
        ccp_target[ccp_target < 0] = -1

        ref_aum = self.portfolio_manager.init_cash
        weight = ccp_target / n_security
        order = self.return_reserve_target_base_ccy(weight * ref_aum)
        return order.reshape(1, -1)
