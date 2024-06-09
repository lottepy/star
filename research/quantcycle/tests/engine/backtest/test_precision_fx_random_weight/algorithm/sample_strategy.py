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
    'status': types.DictType(types.unicode_type, nb.float64),
    'current_fx_data': nb.float64[:],
    'current_data': nb.float64[:],
    'is_tradable': nb.boolean[:],
})
class Fx_Random_Weight_Strategy(BaseStrategy):

    def init(self):
        pass
        
    def on_data(self, data_dict: dict, time_dict: dict, ref_data_dict: dict, ref_time_dict: dict):

        current_weight = data_dict["ref"][0, :, 0]
        ref_aum = self.portfolio_manager.init_cash
        order = self.return_reserve_target_base_ccy(current_weight * ref_aum)

        return order.reshape(1, -1)