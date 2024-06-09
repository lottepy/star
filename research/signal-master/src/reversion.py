import numba as nb
import numpy as np
from numba import types
from numba.experimental import jitclass
from numba.typed import Dict
from quantcycle.app.pms_manager.portfolio_manager import PorfolioManager
from quantcycle.app.signal_generator.base_strategy import BaseStrategy

try:
    pms_type = PorfolioManager.class_type.instance_type
except AttributeError:
    pms_type = "PMS_type"

float_array_2d = nb.float64[:, :]


@jitclass({
    'portfolio_manager': pms_type,
    'ccy_matrix': nb.float64[:, :],
    'strategy_name': types.unicode_type,
    'strategy_param': nb.float64[:],
    'symbol_batch': types.ListType(types.unicode_type),
    'ccy_list': types.ListType(types.unicode_type),
    'symbol_type': types.ListType(types.unicode_type),
    'status': types.DictType(types.unicode_type, nb.float64[:, :]),
    'current_fx_data': nb.float64[:],
    'current_data': nb.float64[:],
    'is_tradable': nb.boolean[:],
    'trade_time': nb.int64[:],
    'trade_price': nb.float64[:],
    'last_order': nb.float64[:],
    'symbol_transformer': nb.float64[:],
    'predict_return': nb.float64[:, :],
    'time': nb.float64[:, :],
    'prepared': nb.boolean
})
class signalReversion(BaseStrategy):

    def init(self):
        self.symbol_transformer = np.zeros(len(self.symbol_batch))
        self.symbol_transform()
        self.prepared = False  # indicate whether status is created
        self.status = Dict.empty(types.unicode_type, float_array_2d)

    def symbol_transform(self):
        for i in nb.prange(len(self.symbol_batch)):
            assert 'USD' in self.symbol_batch[i]
            if self.symbol_batch[i][:3] == 'USD':
                self.symbol_transformer[i] = -1.0
            else:
                self.symbol_transformer[i] = 1.0

    def save_status(self):
        '''可以把需要的status存进strategy_status
           不存任何状态会存一个空的'''
        # strategy_status = Dict.empty(types.unicode_type, nb.float64[:,:])
        self.status['predict_return'] = self.predict_return
        self.status['time'] = self.time

    def load_status(self, pickle_dict):
        # strategy_status = pickle_dict
        pass

    def on_data(self, data_dict: dict, time_dict: dict):
        window_data = data_dict["main"]
        time_data = time_dict["main"][:, :]

        n_security = self.ccy_matrix.shape[1]

        # 获取参数列表
        alpha = np.float64(self.strategy_param[0])
        w = np.int64(self.strategy_param[1])

        # 获取历史数据
        prices = window_data[-w:, :, 3]

        # 计算预期收益
        predict_return = np.ones(n_security)
        for i in range(0, w - 1):
            x_t = prices[i + 1] / prices[i]
            predict_return = self.compute_ema(alpha, x_t, predict_return)

        predict_return = predict_return ** self.symbol_transformer

        if self.prepared:
            self.predict_return = np.concatenate(
                (self.predict_return, predict_return.copy().reshape(1, -1)))
            self.time = np.concatenate(
                (self.time, np.array([time_data[-1, 0]]).reshape(1, -1)))
        else:
            self.predict_return = predict_return.copy().reshape(1, -1)
            self.time = np.array([np.float64(time_data[-1, 0])]).reshape(1, -1)
            self.prepared = True

        return np.zeros(n_security).reshape(1, -1)

    def compute_ema(self, alpha, x_t, xt_head):
        return alpha + (1 - alpha) * np.divide(xt_head, x_t)
