import numpy as np
import numba as nb
from numba.typed import Dict, List
from numba import types
from quantcycle.app.signal_generator.base_strategy import defaultjitclass, BaseStrategy
from quantcycle.app.pms_manager.portfolio_manager import PorfolioManager

try:
    pms_type = PorfolioManager.class_type.instance_type
except:
    pms_type = "PMS_type"
##===========================================================================##

int64_2d_array_type = nb.int64[:, :]
@defaultjitclass({
    'ID_symbol_dict': types.DictType(types.unicode_type, int64_2d_array_type),
    'category_names': types.ListType(types.unicode_type),
    'select_strategy_ids': nb.int64[:],
    'own_static_init': nb.boolean
})
class Combination_strategy(BaseStrategy):

    def init(self):
        n_security = self.ccy_matrix.shape[1]
        self.ID_symbol_dict = Dict.empty(nb.types.unicode_type, int64_2d_array_type)
        self.category_names = List.empty_list(nb.types.unicode_type)
        self.own_static_init = False  # indicate the initialization of ID_symbol_dict & category_names

    def _load_info_dict(self):
        # 找到position对应的symbol(hardcore: 暂时认为每个ID的symbol数目一致)
        symbols = list(self.metadata['main']['symbols'])
        for strategy_name in self.category_names:
            n_id = len(self.id_mapping)
            n_symbol = len(self.metadata['0'][strategy_name])
            id_symbol_array = np.zeros((n_id, n_symbol), dtype=np.int64)
            for id in range(len(self.id_mapping)):
                id_str = self.metadata['masked_secondary_ids'][strategy_name][id]
                id_symbol_array[id] = [symbols.index(s) for s in self.metadata[id_str][strategy_name]]
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
            self.category_names.extend(self.metadata['0'].keys())
            self._load_info_dict()
            self.own_static_init = True
        n_security = self.ccy_matrix.shape[1]
        n_category = len(self.category_names)
        ccp_current = self.portfolio_manager.current_holding
        ccp_target = np.zeros(n_security)
        # category 等权
        weight = 1/(n_category) * np.ones(n_category)
        for i in range(n_category):
            category_name = self.category_names[i]
            position_name = category_name + '_position'
            strategy_id_position = data_dict[position_name][-1] #取当前时刻position

            strategy_id_symbols = self.ID_symbol_dict[category_name][0]
            for symbol_i in nb.prange(strategy_id_symbols.shape[0]):
                position = strategy_id_position[0, symbol_i]
                symbol_id = strategy_id_symbols[symbol_i]
                ccp_target[symbol_id] += position * weight[i]
        ccp_order = ccp_target - ccp_current

        return ccp_order.reshape(1, -1)
