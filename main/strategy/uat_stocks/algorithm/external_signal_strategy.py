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


class ExternalSignalStrategy(BaseStrategy):

    def init(self):
        self.set_parameters()
        self.executed_signal_time = 0

    def set_parameters(self):
        self.signal_method = 1

    def on_data(self, data_dict, time_dict, ref_data_dict, ref_time_dict):
        try:
            current_signal_time = ref_time_dict['signal'][-1, 0]
        except:
            current_signal_time = -1
        close_price = data_dict['main'][:, :, 3]
        self.symbol_batch = self.metadata["main"]["symbols"]
        n_security = len(self.symbol_batch)
        if current_signal_time > self.executed_signal_time:
            signal = ref_data_dict['signal'][-1, 0, :]
            self.executed_signal_time = current_signal_time
            known_filter = ref_data_dict['filter'][-1, 0, :]
            signal = self.apply_known_filter(signal, known_filter)
            weight = self.signal2weight(signal, close_price)
            target = weight * self.portfolio_manager.pv
            target = target[:n_security]
            order = self.return_reserve_target_base_ccy(target)
            order = order[:n_security]
            return order.reshape(1, -1)
        else:
            return np.zeros(n_security).reshape(1, -1)

    def apply_known_filter(self, signal, known_filter):
        filtered_signal = np.zeros_like(signal)
        known_filter = known_filter.astype(bool)
        filtered_signal[known_filter] = signal[known_filter]
        return filtered_signal

    def signal2weight(self, signal, close_price):
        if np.isnan(signal).all():
            return np.zeros_like(signal)
        else:
            # method 1
            # equal weight
            if self.signal_method == 1:
                return np.sign(signal) / np.nansum(np.abs(np.sign(signal)))

            # method 2
            # risk parity
            if self.signal_method == 2:
                vol = np.nanstd(close_price[1:, :] / close_price[:-1, :] - 1, axis=0)
                weight = np.sign(signal) / vol
                return weight / np.nansum(np.abs(weight))

            # method 3
            # cap
            if self.signal_method == 3:
                if np.nansum(np.abs(signal)) > 1:
                    return signal / np.nansum(np.abs(signal))
                else:
                    return signal

    def load_status(self, pickle_dict):
        strategy_status = pickle_dict
        self.status = strategy_status

    def save_status(self):
        '''可以把需要的status存进strategy_status
           不存任何状态会存一个空的'''
        strategy_status = Dict.empty(types.unicode_type, nb.float64)
        self.status = strategy_status


class ANALYST_COVERAGE(ExternalSignalStrategy):
    pass


class NORTHBOUND_HOLD(ExternalSignalStrategy):
    def set_parameters(self):
        self.signal_method = 3


class EPIND_FSCORE(ExternalSignalStrategy):
    pass


class QUALITY_REVERSION(ExternalSignalStrategy):
    pass


class NORTHBOUND_HOLD_MARKET_CAP_WEIGHT(ExternalSignalStrategy):
    def set_parameters(self):
        self.signal_method = 3


class TEST(ExternalSignalStrategy):
    pass


class QUALITY_AND_REVERSION(ExternalSignalStrategy):
    def set_parameters(self):
        self.signal_method = 3