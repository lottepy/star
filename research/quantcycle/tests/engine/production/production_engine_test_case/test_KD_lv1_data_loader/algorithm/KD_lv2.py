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

int64_2d_array_type = nb.int64[:, :]
@defaultjitclass({
    'portfolio_manager': pms_type,
    'ccy_matrix': nb.float64[:, :],
    'strategy_name': types.unicode_type,
    'strategy_param': nb.float64[:],
    'symbol_batch': types.ListType(types.unicode_type),
    'ccy_list': types.ListType(types.unicode_type),
    'symbol_type': types.ListType(types.unicode_type),
    'status':types.DictType(types.unicode_type, nb.float64),
    'current_fx_data': nb.float64[:],
    'current_data': nb.float64[:],
    'is_tradable': nb.boolean[:],
    'ID_symbol_dict': types.DictType(types.unicode_type, int64_2d_array_type),
    'category_names': types.ListType(types.unicode_type),
    'select_strategy_ids': nb.int64[:],
})
class Allocation_strategy(BaseStrategy):

    def __init__(self, portfolio_manager: PorfolioManager,strategy_name, strategy_param, symbol_batch, ccy_list, ccy_matrix):
        self.init(portfolio_manager, strategy_name, strategy_param, symbol_batch, ccy_list, ccy_matrix)
        n_security = self.ccy_matrix.shape[1]
        self.ID_symbol_dict = {}
        self.category_names = []
        self.own_static_init = False
    
    def _load_info_dict(self):
        # 找到position对应的symbol(hardcore: 暂时认为每个ID的symbol数目一致)
        symbols = list(self.metadata['main']['symbols'])
        for strategy_name in self.category_names:
            n_id = len(self.id_mapping)
            n_symbol = len(self.metadata[str(int(0))][strategy_name])
            id_symbol_array = np.zeros((n_id, n_symbol), dtype=np.int64)
            for id in range(len(self.id_mapping)):
                id_symbol_array[id] = [symbols.index(s) for s in self.metadata[str(int(id))][strategy_name]]
            self.ID_symbol_dict[strategy_name] = id_symbol_array.copy()
    def save_status(self):
        '''可以把需要的status存进strategy_status
           不存任何状态会存一个空的'''
        strategy_status = Dict.empty(types.unicode_type, nb.float64)
        self.status = strategy_status

    def load_status(self, pickle_dict):
        strategy_status = pickle_dict
        self.status = strategy_status

    def on_data(self, data_dict: dict, time_dict: dict, ref_data_dict: dict, ref_time_dict: dict):
        if not self.own_static_init:
            self.category_names = list(self.metadata['0'].keys())
            self._load_info_dict()
            self.own_static_init = True

        category_name = self.category_names[0]
        position_name = category_name + '_position'
        short_metrics_name = category_name + '_metrics_61'
        long_metrics_name = category_name + '_metrics_252'
        n_security = self.ccy_matrix.shape[1]
        ccp_current = self.portfolio_manager.current_holding
        ccp_target = np.zeros(n_security)
        strategy_id_position = data_dict[position_name][-1]
        #有12个月数据时，可以开始allocation
        if long_metrics_name in time_dict:
            # 更新12个月数据时，重新allocation
            if time_dict[long_metrics_name][-1,0,0] == \
                time_dict['main'][-1,0,0]:
                metrics_long = data_dict[long_metrics_name]
                metrics_short = data_dict[short_metrics_name]

                return_long = metrics_long[0,:,0]
                return_short = metrics_short[0,:,0]

                self.select_strategy_ids = np.where((return_long>0)&(return_short>0))[0]
            # equal weight to this strategy
            n_select_id = len(self.select_strategy_ids)
            for strategy_id in self.select_strategy_ids:
                strategy_id_symbols = self.ID_symbol_dict[category_name][strategy_id]
                for symbol_i in nb.prange(strategy_id_symbols.shape[0]):
                    position = strategy_id_position[strategy_id, symbol_i]
                    symbol_id = strategy_id_symbols[symbol_i]
                    ccp_target[symbol_id] += position
            ccp_target = ccp_target/n_select_id
        ccp_order = ccp_target - ccp_current

        return ccp_order.reshape(1, -1)

