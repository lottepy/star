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
    'mom_record': nb.float64[:, :],
    'mom_rank': nb.float64[:, :],
    'time': nb.float64[:, :],
    'prepared': nb.boolean
})
class signalMomentum(BaseStrategy):

    def init(self):
        rank_period = np.int64(self.strategy_param[0])
        n_security = self.ccy_matrix.shape[1]
        self.mom_record = np.ones((rank_period, n_security)) * np.nan
        self.prepared = False  # indicate whether status is created
        self.status = Dict.empty(types.unicode_type, float_array_2d)

    def save_status(self):
        '''可以把需要的status存进strategy_status
           不存任何状态会存一个空的'''
        # strategy_status = Dict.empty(types.unicode_type, nb.float64[:,:])
        self.status['mom_rank'] = self.mom_rank
        self.status['time'] = self.time

    def load_status(self, pickle_dict):
        # strategy_status = pickle_dict
        pass

    def on_data(self, data_dict: dict, time_dict: dict):
        window_data = data_dict["main"]
        time_data = time_dict["main"][:, :]

        n_security = self.ccy_matrix.shape[1]

        # 获取参数列表
        mom_window = np.int64(self.strategy_param[1])

        # 获取历史数据
        prices = window_data[:, :, 3]
        best_window = mom_window

        mom = prices[-1] / prices[-best_window] - 1.

        self.mom_record[:-1, :] = self.mom_record[1:, :]
        self.mom_record[-1, :] = mom

        current_level = np.ones(n_security) * np.nan
        if not np.isnan(self.mom_record).any():
            mom_rank = np.zeros_like(self.mom_record)
            for i in nb.prange(n_security):
                mom_rank[:, i] = self.mom_record[:, i].argsort().argsort()
            current_level = mom_rank[-1] / (mom_rank.shape[0] - 1.)

        if self.prepared:
            self.mom_rank = np.concatenate(
                (self.mom_rank, current_level.copy().reshape(1, -1)))
            self.time = np.concatenate(
                (self.time, np.array([time_data[-1, 0]]).reshape(1, -1)))
        else:
            self.mom_rank = current_level.copy().reshape(1, -1)
            self.time = np.array([np.float64(time_data[-1, 0])]).reshape(1, -1)
            self.prepared = True

        return np.zeros(n_security).reshape(1, -1)
