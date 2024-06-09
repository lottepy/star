import numpy as np
import numba as nb
from numba.experimental import jitclass
from numba.typed import Dict, List
from numba import types
from quantcycle.app.pms_manager.portfolio_manager import PorfolioManager

try:
    pms_type = PorfolioManager.class_type.instance_type
except:
    pms_type = "PMS_type"


def defaultjitclass(spec={}):
    default_spec = {
        'portfolio_manager': pms_type,
        'strategy_name': types.unicode_type,
        'backup_symbols': types.ListType(types.unicode_type),
        'symbols_list': types.ListType(types.unicode_type),
        'strategy_param': nb.float64[:],
        'metadata': types.DictType(types.unicode_type,
                                   types.DictType(types.unicode_type, types.ListType(types.unicode_type))),
        'ccy_matrix': nb.float64[:, :],
        'id_mapping': types.DictType(nb.int64, nb.int64),
        'current_fx_data': nb.float64[:],
        'current_data': nb.float64[:],
        'status': types.DictType(types.unicode_type, nb.float64),
        'signal_remark': types.DictType(nb.int64, types.DictType(types.unicode_type, nb.float64)),
        'remark_array': nb.float64[:, :, :],
        'timestamp': nb.int64,
        'sid': nb.int64,
        'trade_array': nb.float64[:, :]
    }
    default_spec.update(spec)
    return jitclass(default_spec)


class BaseStrategy:

    def __init__(self, portfolio_manager: PorfolioManager, strategy_name, strategy_param, metadata,
                 ccy_matrix, id_mapping):

        self.portfolio_manager = portfolio_manager
        self.strategy_name = strategy_name
        self.strategy_param = strategy_param
        self.metadata = self.__copy_metadata(metadata)
        self.ccy_matrix = ccy_matrix
        self.id_mapping = id_mapping
        self.status = Dict.empty(types.unicode_type, nb.float64)
        self.signal_remark = Dict.empty(nb.int64, Dict.empty(types.unicode_type, nb.float64))
        self.timestamp = 0
        self.backup_symbols = List.empty_list(types.unicode_type)


    def init(self):
        pass


    def save_status(self):
        pass


    def load_status(self, pickle_dict: dict):
        pass


    def load_info(self, info_dict):
        pass


    def on_data(self, data_dict: dict, time_dict: dict, ref_data_dict: dict, ref_time_dict: dict):
        # fire order
        # self.save_signal_remark(np.zeros((5,5)))
        return np.zeros((1, self.ccy_matrix.shape[1]))

    def on_order_feedback(self, trade_array, data_dict: dict, time_dict: dict, ref_data_dict: dict, ref_time_dict: dict):
        # by default, return a zero-array of shape(1, symbol_size)
        return np.zeros((1, self.ccy_matrix.shape[1]))

    def save_signal_remark(self, signal_remark: dict):
        if signal_remark:
            self.signal_remark[self.timestamp] = signal_remark

    def on_pms_feedback(self, order, pms_status):
        return np.copy(order)


    def on_risk_feedback(self, order, risk_array):
        return np.copy(order)


    def save_current_data(self, current_fx_data, current_data, current_time):
        self.current_fx_data = current_fx_data
        self.current_data = current_data
        self.timestamp = current_time


    def fx_convert(self, order):
        return order * (1 / self.current_fx_data)
    
    
    def get_remark(self, remark_array, sid):
        """
        Retrieve correct remarks given a list of post-flattened strategy IDs.
        :param remark_array: The signal remarks' 3D-array
        :param sid: The post-flattened strategy ID (int32)
        :return: A 2D remark array of the selected strategy ID. The 1st-dim represents
                 the time window; the 2nd-dim represents remark fields.
        """
        if not self.id_mapping:
            return remark_array[:, sid, :]
        else:
            return remark_array[:, self.id_mapping[sid], :]


    def return_reserve_order_base_ccy(self, base_ccy_order):
        local_ccy_converted_order = self.fx_convert(base_ccy_order)
        return self.return_reserve_order(local_ccy_converted_order)


    def return_reserve_target_base_ccy(self, base_ccy_target):
        local_ccy_converted_target = self.fx_convert(base_ccy_target)
        return self.return_reserve_target(local_ccy_converted_target)


    def return_reserve_order(self, order):
        non_tradable_ticker = self.current_data < 0.01
        temp_order = (order / self.current_data)
        temp_order[non_tradable_ticker] = 0
        return temp_order


    def return_reserve_target(self, target):
        non_tradable_ticker = self.current_data < 0.01
        temp_target = (target / self.current_data) * (
            ~non_tradable_ticker) + self.portfolio_manager.current_holding * non_tradable_ticker
        return self.return_target(temp_target)


    def return_target(self, target):
        return target - self.portfolio_manager.current_holding


    def clear_signal_remark(self):
        self.signal_remark = Dict.empty(nb.int64, Dict.empty(types.unicode_type, nb.float64))

    def update_backup_symbols(self, symbols_list):
        self.backup_symbols = symbols_list.copy()

    def remove_backup_symbols(self):
        self.backup_symbols.clear()
    


    @staticmethod
    def __copy_metadata(metadata):
        metadata2 = Dict()
        for k1, v1 in metadata.items():
            dict2 = Dict()
            for k2, v2 in v1.items():
                dict2[k2] = v2.copy()
            metadata2[k1] = dict2
        return metadata2


# @defaultjitclass({
#     'portfolio_manager': pms_type,
#     'ccy_matrix': nb.float64[:, :],
#     'strategy_name': types.unicode_type,
#     'strategy_param': nb.float64[:],
#     'symbol_batch': types.ListType(types.unicode_type),
#     'ccy_list': types.ListType(types.unicode_type),
#     'current_fx_data': nb.float64[:],
#     'current_data': nb.float64[:],
#     'is_tradable': nb.boolean[:],
# })
# class TestStrategy(BaseStrategy):
#
#     def __init__(self, portfolio_manager: PorfolioManager, strategy_name, strategy_param, symbol_batch, ccy_list,
#                  ccy_matrix):
#         self.init(portfolio_manager, strategy_name, strategy_param, symbol_batch, ccy_list, ccy_matrix)
#
#     def on_data(self, window_data, time_data, is_close, data_dict: dict):
#         current_weight = data_dict["ref"][0, :, 0]
#         current_data = window_data[-1, :, 3]
#         current_fx_data = data_dict["fx"][-1, :, 0]
#         ref_aum = self.portfolio_manager.init_cash
#         target_holding = current_weight * ref_aum / (current_data * current_fx_data)
#         current_holding = self.portfolio_manager.current_holding
#         return (target_holding - current_holding).reshape(1, -1)


# import numpy as np
#
# from quantcycle.app.pms_manager.portfolio_manager import PorfolioManager
#
#
# class BaseStrategy:
#
#     def init(self, portfolio_manager: PorfolioManager,strategy_name, strategy_param, symbol_batch, ccy_list, ccy_matrix):
#         self.portfolio_manager = portfolio_manager
#         self.ccy_matrix = ccy_matrix
#         self.strategy_name = strategy_name
#         self.strategy_param = strategy_param
#         self.symbol_batch = symbol_batch
#         self.ccy_list = ccy_list
#
#     def save_status(self):
#         return {}
#
#
#     def load_status(self, pickle_dict : dict):
#         pass
#
#     def on_data(self, window_data, time_data,is_close, data_dict : dict):
#         # fire order
#         return None
#         # return return_reserve_order_base_ccy(weight from base ccy)
#         # not fire order
#         #return [0,0,0,0,0,0]
#
#
#
#     def on_pms_feedback(self,order,pms_status):
#         return np.copy(order)
#
#     def on_risk_feedback(self,order,risk_array):
#         return np.copy(order)
#
#     def save_current_data(self, current_fx_data, current_data,is_tradable):
#         self.current_fx_data = current_fx_data
#         self.current_data = current_data
#         self.is_tradable = is_tradable
#
#     def fx_convert(self, order):
#         return order * (1 / self.current_fx_data)
#
#
#     def return_reserve_order_base_ccy(self, base_ccy_order):
#         local_ccy_converted_order = self.fx_convert(base_ccy_order)
#         return self.return_reserve_order(local_ccy_converted_order)
#
#     def return_reserve_target_base_ccy(self, base_ccy_target):
#         local_ccy_converted_target = self.fx_convert(base_ccy_target)
#         return self.return_reserve_target(local_ccy_converted_target)
#
#
#     def return_reserve_order(self, order):
#         non_tradable_ticker = self.current_data < 0.01
#         temp_order = (order / self.current_data)
#         temp_order[non_tradable_ticker] = 0
#         return temp_order
#
#
#     def return_reserve_target(self, target):
#         non_tradable_ticker = self.current_data < 0.01
#         temp_target = (target / self.current_data) * (~non_tradable_ticker) + self.portfolio_manager.current_holding * non_tradable_ticker
#         return self.return_target(temp_target)
#
#
#     def return_target(self, target):
#         return target - self.portfolio_manager.current_holding
#
