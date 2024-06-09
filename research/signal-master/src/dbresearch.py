import numba as nb
import numpy as np
from numba import types
from numba.experimental import jitclass
from numba.typed import Dict
from quantcycle.app.pms_manager.portfolio_manager import PorfolioManager
from quantcycle.app.signal_generator.base_strategy import BaseStrategy
from quantcycle.utils.indicator import MA

try:
    pms_type = PorfolioManager.class_type.instance_type
    ma_type = MA.class_type.instance_type
except AttributeError:
    pms_type = "PMS_type"
    ma_type = 'MA_type'

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
    'symbol_transformer': nb.float64[:],
    'mValue': nb.float64[:, :],
    'mVolatility': nb.float64[:, :],
    'mSignal': nb.float64[:, :],
    'time': nb.float64[:, :],
    'prepared': nb.boolean
})
class signalMomentum(BaseStrategy):
    def init(self):
        self.prepared = False  # indicate whether status is created
        self.status = Dict.empty(types.unicode_type, float_array_2d)
        self.symbol_transformer = np.zeros(len(self.symbol_batch))
        self.symbol_transform()

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
        self.status['momentumValue'] = self.mValue
        self.status['momentumVolatility'] = self.mVolatility
        self.status['momentumSignal'] = self.mSignal
        self.status['time'] = self.time

    def load_status(self, pickle_dict):
        # strategy_status = pickle_dict
        pass

    def on_data(self, data_dict: dict, time_dict: dict):
        window_data = data_dict["main"]
        time_data = time_dict["main"][:, :]

        close_price = window_data[-1, :, 3]
        hist_close = window_data[-253:-22, :, 3]
        n_security = self.ccy_matrix.shape[1]

        rtn_matrix = (close_price - hist_close) / hist_close \
            * self.symbol_transformer
        sgnl_matrix = np.sign(rtn_matrix)
        mValue = np.zeros(n_security)
        mVolatility = np.zeros(n_security)
        mSignal = np.zeros(n_security)
        for i in nb.prange(n_security):
            sgnl_series = sgnl_matrix[:, i]
            mValue[i] = np.mean(sgnl_series)
            mVolatility[i] = np.std(sgnl_series)
            mSignal[i] = np.sign(mValue[i])

        if self.prepared:
            self.mValue = np.concatenate(
                (self.mValue, mValue.copy().reshape(1, -1)))
            self.mVolatility = np.concatenate(
                (self.mVolatility, mVolatility.copy().reshape(1, -1)))
            self.mSignal = np.concatenate(
                (self.mSignal, mSignal.copy().reshape(1, -1)))
            self.time = np.concatenate(
                (self.time, np.array([time_data[-1, 0]]).reshape(1, -1)))
        else:
            self.mValue = mValue.copy().reshape(1, -1)
            self.mVolatility = mVolatility.copy().reshape(1, -1)
            self.mSignal = mSignal.copy().reshape(1, -1)
            self.time = np.array([np.float64(time_data[-1, 0])]).reshape(1, -1)
            self.prepared = True

        return np.zeros(n_security).reshape(1, -1)


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
    'symbol_transformer': nb.float64[:],
    'Carry': nb.float64[:, :],
    'time': nb.float64[:, :],
    'prepared': nb.boolean
})
class signalCarry(BaseStrategy):
    def init(self):
        self.prepared = False  # indicate whether status is created
        self.status = Dict.empty(types.unicode_type, float_array_2d)
        self.symbol_transformer = np.zeros(len(self.symbol_batch))
        self.symbol_transform()

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
        self.status['Carry'] = self.Carry
        self.status['time'] = self.time

    def load_status(self, pickle_dict):
        # strategy_status = pickle_dict
        pass

    def on_data(self, data_dict: dict, time_dict: dict):
        window_data = data_dict["main"]
        time_data = time_dict["main"][:, :]

        close_price = window_data[:, :, 3]
        n_security = self.ccy_matrix.shape[1]

        N = np.int64(self.strategy_param[0])

        fp_data = data_dict["FP"][:, :, 0]
        forward_price = close_price + fp_data/100

        c = (close_price ** self.symbol_transformer
             - forward_price ** self.symbol_transformer) / \
            forward_price**self.symbol_transformer
        signal = np.zeros(n_security)
        for i in range(n_security):
            rtn = close_price[-252:, i]/close_price[-253:-1, i]-1
            signal[i] = np.mean(c[-N:, i]) / np.std(rtn *
                                                    self.symbol_transformer[i])

        if self.prepared:
            self.Carry = np.concatenate(
                (self.Carry, signal.copy().reshape(1, -1)))
            self.time = np.concatenate(
                (self.time, np.array([time_data[-1, 0]]).reshape(1, -1)))
        else:
            self.Carry = signal.copy().reshape(1, -1)
            self.time = np.array([np.float64(time_data[-1, 0])]).reshape(1, -1)
            self.prepared = True

        return np.zeros(n_security).reshape(1, -1)


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
    'symbol_transformer': nb.float64[:],
    'CFTCMomentum': nb.float64[:, :],
    'time': nb.float64[:, :],
    'prepared': nb.boolean
})
class signalCFTCMomentum(BaseStrategy):
    def init(self):
        self.prepared = False  # indicate whether status is created
        self.status = Dict.empty(types.unicode_type, float_array_2d)
        self.symbol_transformer = np.zeros(len(self.symbol_batch))
        self.symbol_transform()

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
        self.status['CFTCMomentum'] = self.CFTCMomentum
        self.status['time'] = self.time

    def load_status(self, pickle_dict):
        # strategy_status = pickle_dict
        pass

    def on_data(self, data_dict: dict, time_dict: dict):
        # window_data = data_dict["main"]
        time_data = time_dict["main"][:, :]
        NCLong = data_dict['NCLong'][:, :, 0]
        NCShort = data_dict['NCShort'][:, :, 0]

        n_security = self.ccy_matrix.shape[1]

        if time_data[-1, 0] == time_dict["NCLong"][-1, 0]:
            L = np.zeros(n_security)
            S = np.zeros(n_security)
            signal = np.zeros(n_security)
            for i in range(n_security):
                L[i] = np.sum(NCLong[-4:, i])
                S[i] = np.sum(NCShort[-4:, i])
                signal[i] = (L[i]-S[i])/(L[i]+S[i])

            if self.prepared:
                self.CFTCMomentum = np.concatenate(
                    (self.CFTCMomentum, signal.copy().reshape(1, -1)))
                self.time = np.concatenate(
                    (self.time, np.array([time_data[-1, 0]]).reshape(1, -1)))
            else:
                self.CFTCMomentum = signal.copy().reshape(1, -1)
                self.time = np.array(
                    [np.float64(time_data[-1, 0])]).reshape(1, -1)
                self.prepared = True

        return np.zeros(n_security).reshape(1, -1)


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
    'symbol_transformer': nb.float64[:],
    'CFTCReversal': nb.float64[:, :],
    'time': nb.float64[:, :],
    'prepared': nb.boolean
})
class signalCFTCReversal(BaseStrategy):
    def init(self):
        self.prepared = False  # indicate whether status is created
        self.status = Dict.empty(types.unicode_type, float_array_2d)
        self.symbol_transformer = np.zeros(len(self.symbol_batch))
        self.symbol_transform()

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
        self.status['CFTCReversal'] = self.CFTCReversal
        self.status['time'] = self.time

    def load_status(self, pickle_dict):
        # strategy_status = pickle_dict
        pass

    def on_data(self, data_dict: dict, time_dict: dict):
        window_data = data_dict["main"]
        time_data = time_dict["main"][:, :]
        NCLong = data_dict['NCLong'][:, :, 0]
        NCShort = data_dict['NCShort'][:, :, 0]

        close_price = window_data[:, :, 3]

        n_security = self.ccy_matrix.shape[1]

        if time_data[-1, 0] == time_dict["NCLong"][-1, 0]:
            signal = np.zeros(n_security)
            for i in range(n_security):
                net = NCLong[-12:, i] - NCShort[-12:, i]
                z_1 = (net[-1] - np.mean(net[-4:])) / np.std(net[-4:])
                z_2 = (net[-1] - np.mean(net[-8:])) / np.std(net[-8:])
                z_3 = (net[-1] - np.mean(net[-12:])) / np.std(net[-12:])
                z_mean = (z_1+z_2+z_3)/3
                rtn = close_price[-126:, i]/close_price[-127:-1, i]-1
                signal[i] = z_mean / np.std(rtn) * (-1)

            if self.prepared:
                self.CFTCReversal = np.concatenate(
                    (self.CFTCReversal, signal.copy().reshape(1, -1)))
                self.time = np.concatenate(
                    (self.time, np.array([time_data[-1, 0]]).reshape(1, -1)))
            else:
                self.CFTCReversal = signal.copy().reshape(1, -1)
                self.time = np.array(
                    [np.float64(time_data[-1, 0])]).reshape(1, -1)
                self.prepared = True

        return np.zeros(n_security).reshape(1, -1)


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
    'symbol_transformer': nb.float64[:],
    'signal': ma_type,
    'MSO': nb.float64[:, :],
    'time': nb.float64[:, :],
    'prepared': nb.boolean
})
class signalMomentumSpillOver(BaseStrategy):
    def init(self):
        self.prepared = False  # indicate whether status is created
        self.status = Dict.empty(types.unicode_type, float_array_2d)
        self.symbol_transformer = np.zeros(len(self.symbol_batch))
        self.symbol_transform()
        n_security = self.ccy_matrix.shape[1]
        self.signal = MA(21, n_security)

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
        self.status['MSO'] = self.MSO
        self.status['time'] = self.time

    def load_status(self, pickle_dict):
        # strategy_status = pickle_dict
        pass

    def on_data(self, data_dict: dict, time_dict: dict):
        window_data = data_dict["main"]
        time_data = time_dict["main"][:, :]

        close_price = window_data[:, :, 3]
        n_security = self.ccy_matrix.shape[1]

        fp_data = data_dict["FP"][:, :, 0]
        forward_price = close_price + fp_data/100

        price_ = close_price ** self.symbol_transformer
        forward_price_ = forward_price ** self.symbol_transformer

        interest_rate_diff = 1 - forward_price_ / price_

        value = np.zeros(n_security)
        for i in range(n_security):
            diff_1 = (interest_rate_diff[-1, i] -
                      interest_rate_diff[-22, i]) * 252 / 21
            diff_2 = (interest_rate_diff[-1, i] -
                      interest_rate_diff[-43, i]) * 252 / 42
            diff_3 = (interest_rate_diff[-1, i] -
                      interest_rate_diff[-64, i]) * 252 / 64
            vol_1 = np.std(interest_rate_diff[-21:, i] -
                           interest_rate_diff[-22:-1, i]) * np.sqrt(252)
            vol_2 = np.std(interest_rate_diff[-42:, i] -
                           interest_rate_diff[-43:-1, i]) * np.sqrt(252)
            vol_3 = np.std(interest_rate_diff[-63:, i] -
                           interest_rate_diff[-64:-1, i]) * np.sqrt(252)
            value[i] = (diff_1 / vol_1 + diff_2/vol_2+diff_3/vol_3)/2
        self.signal.on_data(value)

        if self.signal.ready:
            if self.prepared:
                self.MSO = np.concatenate(
                    (self.MSO, self.signal.value.copy().reshape(1, -1)))
                self.time = np.concatenate(
                    (self.time, np.array([time_data[-1, 0]]).reshape(1, -1)))
            else:
                self.MSO = self.signal.value.copy().reshape(1, -1)
                self.time = np.array(
                    [np.float64(time_data[-1, 0])]).reshape(1, -1)
                self.prepared = True

        return np.zeros(n_security).reshape(1, -1)
