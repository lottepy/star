import numpy as np
from numba.typed import Dict
from numba.types import string

"""
FX_utility_spec = [
    ('strategy_name', string),
    ('strategy_start', int64),
    ('strategy_end', int64),
    ('strategy_param', ListType(float64)),
    ('symbol_batch', ListType(string)),
    ('ccy_list', ListType(string)),
    ('ccy_matrix', float64[:, :]),
    ('ccp_target', float64[:]),
    ('signal_remark', DictType(string, string)),

    ('window_past_data', float64[:, :, :]),
    ('window_past_fx_data', float64[:, :]),
    ('window_rate_data', float64[:, :, :]),
    ('window_rate_time_data', uint64[:, :]),
]"""


class StrategyUtility(object):
    def __init__(self, strategy_name, strategy_start, strategy_end, strategy_param, lot_size, symbol_batch, ccy_list, ccy_matrix):
        self.strategy_name = strategy_name
        self.strategy_start = strategy_start
        self.strategy_end = strategy_end
        self.strategy_param = strategy_param
        self.symbol_batch = symbol_batch
        self.ccy_list = ccy_list
        self.ccy_matrix = np.copy(ccy_matrix)
        self.target_order = None
        self.current_holding = np.zeros(len(symbol_batch))
        self.signal_remark = {}

        self.window_past_data = np.empty((1, 1, 1))
        self.window_past_fx_data = np.empty((1, 1))
        self.window_rate_data = np.empty((1, 1, 1))
        self.window_rate_time_data = np.ones((1, 1), dtype=np.uint64)
        self.current_fx_data = np.empty(len(ccy_list))
        self.current_data = np.empty(len(symbol_batch))
        self.current_rate = np.empty(len(symbol_batch))
        self.is_tradable = np.array([True for symbol in symbol_batch])
        self.lot_size = lot_size
        self.signal_remark_list = []
        self.signal_remark_ts = []

    def save_window_data(self, window_past_data, window_past_fx_data, window_rate_data, window_rate_time_data):
        self.window_past_data = window_past_data
        self.window_past_fx_data = window_past_fx_data
        self.window_rate_data = window_rate_data
        self.window_rate_time_data = window_rate_time_data

    def save_current_data(self, current_fx_data, current_data, current_rate,is_tradable):
        self.current_fx_data = current_fx_data
        self.current_data = current_data
        self.current_rate = current_rate
        self.is_tradable = is_tradable

    def fx_convert(self, target):
        return target * (self.ccy_matrix.T @ (1 / self.current_fx_data))

    def place_reserve_target_base_ccy(self, bace_ccy_target):
        local_ccy_converted_target = self.fx_convert(bace_ccy_target)
        self.place_reserve_target(local_ccy_converted_target)

    def place_reserve_target(self, target):
        non_tradable_ticker = self.current_data < 0.01
        temp_target = (target / self.current_data)
        temp_target[non_tradable_ticker] = self.current_holding[non_tradable_ticker]
        self.place_target(temp_target)

    def place_reserve_order(self, order):
        non_tradable_ticker = self.current_data < 0.01
        temp_order = order / self.current_data
        temp_order[non_tradable_ticker] = 0
        self.place_order(temp_order)

    def place_target(self, target):
        self.place_order(target - self.current_holding)

    def place_order(self, order):
        self.target_order = np.copy(order)
        self.target_order[~ self.is_tradable] = 0

    def fire_order(self):
        """ consider lot size """
        if self.target_order is None:
            return None
        order = np.copy(self.target_order)

        """ for i in range(len(order)):
            if self.lot_size[i] is not None and not np.isnan(self.lot_size[i]):
                order[i] = int(order[i] / self.lot_size[i]) * self.lot_size[i] """
        return order

    def fire_target(self):
        order = self.fire_order()
        return order + self.current_holding

    def rec_feedback(self, order_feedback):
        prev_current_holding = np.copy(self.current_holding)
        if order_feedback is not None:
            self.current_holding = np.array(order_feedback["position"])
        #self.signal_remark["target_order"] = str(list(self.current_holding - prev_current_holding))
        self.target_order = None

    def place_signal_remark(self, signal_remark):
        for k, v in signal_remark.items():
            signal_remark[k] = str(v)
        self.signal_remark = signal_remark

    def fire_signal_remark(self):
        signal_remark = self.signal_remark.copy()
        self.signal_remark = {}
        return signal_remark

    def capture(self, timestamp):
        signal_remark = self.fire_signal_remark()
        self.signal_remark_list.append(signal_remark)
        self.signal_remark_ts.append(int(timestamp))
