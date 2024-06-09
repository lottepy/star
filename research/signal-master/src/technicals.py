import numba as nb
import numpy as np
from numba import types
from numba.experimental import jitclass
from numba.typed import Dict
from quantcycle.app.pms_manager.portfolio_manager import PorfolioManager
from quantcycle.app.signal_generator.base_strategy import BaseStrategy
from quantcycle.utils.indicator import (CCI, EMA, KD, MA, MACD_P, RSI,
                                        Bollinger_Bands, Donchian)

try:
    pms_type = PorfolioManager.class_type.instance_type
    rsi_type = RSI.class_type.instance_type
    bollinger_type = Bollinger_Bands.class_type.instance_type
    donchian_type = Donchian.class_type.instance_type
    macd_type = MACD_P.class_type.instance_type
    cci_type = CCI.class_type.instance_type
    kd_type = KD.class_type.instance_type
    ma_type = MA.class_type.instance_type
    ema_type = EMA.class_type.instance_type
except AttributeError:
    pms_type = "PMS_type"
    rsi_type = "RSI_type"
    bollinger_type = "bollinger_type"
    donchian_type = "donchian_type"
    macd_type = "macd_type"
    cci_type = "CCI_type"
    kd_type = "KD_type"
    ma_type = "MA_type"
    ema_type = "EMA_type"

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
    'kd': kd_type,
    'K': nb.float64[:, :],
    'D': nb.float64[:, :],
    'time': nb.float64[:, :],
    'prepared': nb.boolean
})
class signalKD(BaseStrategy):
    def init(self):
        n_security = self.ccy_matrix.shape[1]
        length1 = np.int64(self.strategy_param[0])
        length2 = np.int64(self.strategy_param[1])
        length3 = np.int64(self.strategy_param[2])
        self.kd = KD(length1, length2, length3, n_security)
        self.prepared = False  # indicate whether status is created
        self.status = Dict.empty(types.unicode_type, float_array_2d)

    def save_status(self):
        '''可以把需要的status存进strategy_status
           不存任何状态会存一个空的'''
        # strategy_status = Dict.empty(types.unicode_type, nb.float64[:,:])
        self.status['K'] = self.K
        self.status['D'] = self.D
        self.status['time'] = self.time

    def load_status(self, pickle_dict):
        # strategy_status = pickle_dict
        pass

    def on_data(self, data_dict: dict, time_dict: dict):
        window_data = data_dict["main"]
        time_data = time_dict["main"][:, :]

        high_price = window_data[-1, :, 1]
        low_price = window_data[-1, :, 2]
        close_price = window_data[-1, :, 3]
        n_security = self.ccy_matrix.shape[1]

        self.kd.on_data(high_price, low_price, close_price)

        if self.kd.ready:
            curr_K_value = self.kd.fast_d.value
            curr_D_value = self.kd.slow_d.value
        else:
            curr_K_value = np.ones(n_security) * np.nan
            curr_D_value = np.ones(n_security) * np.nan

        if self.prepared:
            self.K = np.concatenate(
                (self.K, curr_K_value.copy().reshape(1, -1)))
            self.D = np.concatenate(
                (self.D, curr_D_value.copy().reshape(1, -1)))
            self.time = np.concatenate(
                (self.time, np.array([time_data[-1, 0]]).reshape(1, -1)))
        else:
            self.K = curr_K_value.copy().reshape(1, -1)
            self.D = curr_D_value.copy().reshape(1, -1)
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
    'cci': cci_type,
    'CCI': nb.float64[:, :],
    'time': nb.float64[:, :],
    'prepared': nb.boolean
})
class signalCCI(BaseStrategy):
    def init(self):
        n_security = self.ccy_matrix.shape[1]
        length = np.int64(self.strategy_param[0])
        self.cci = CCI(length, n_security)
        self.prepared = False  # indicate whether status is created
        self.status = Dict.empty(types.unicode_type, float_array_2d)

    def save_status(self):
        '''可以把需要的status存进strategy_status
           不存任何状态会存一个空的'''
        # strategy_status = Dict.empty(types.unicode_type, nb.float64[:,:])
        self.status['CCI'] = self.CCI
        self.status['time'] = self.time

    def load_status(self, pickle_dict):
        # strategy_status = pickle_dict
        pass

    def on_data(self, data_dict: dict, time_dict: dict):
        window_data = data_dict["main"]
        time_data = time_dict["main"][:, :]

        # open_price = window_data[-1, :, 0]
        high_price = window_data[-1, :, 1]
        low_price = window_data[-1, :, 2]
        close_price = window_data[-1, :, 3]
        n_security = self.ccy_matrix.shape[1]

        self.cci.on_data(high_price, low_price, close_price)

        if self.cci.ready:
            curr_CCI_value = self.cci.value
        else:
            curr_CCI_value = np.ones(n_security) * np.nan

        if self.prepared:
            self.CCI = np.concatenate(
                (self.CCI, curr_CCI_value.copy().reshape(1, -1)))
            self.time = np.concatenate(
                (self.time, np.array([time_data[-1, 0]]).reshape(1, -1)))
        else:
            self.CCI = curr_CCI_value.copy().reshape(1, -1)
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
    'bollingerband': bollinger_type,
    'BollingerBandUpper': nb.float64[:, :],
    'BollingerBandMiddle': nb.float64[:, :],
    'BollingerBandLower': nb.float64[:, :],
    'time': nb.float64[:, :],
    'prepared': nb.boolean
})
class signalBollingerBand(BaseStrategy):
    def init(self):
        n_security = self.ccy_matrix.shape[1]
        length = np.int64(self.strategy_param[0])
        width = np.float64(self.strategy_param[1])
        self.bollingerband = Bollinger_Bands(length, n_security, width)
        self.prepared = False  # indicate whether status is created
        self.status = Dict.empty(types.unicode_type, float_array_2d)

    def save_status(self):
        '''可以把需要的status存进strategy_status
           不存任何状态会存一个空的'''
        # strategy_status = Dict.empty(types.unicode_type, nb.float64[:,:])
        self.status['BollingerBandUpper'] = self.BollingerBandUpper
        self.status['BollingerBandMiddle'] = self.BollingerBandMiddle
        self.status['BollingerBandLower'] = self.BollingerBandLower
        self.status['time'] = self.time

    def load_status(self, pickle_dict):
        # strategy_status = pickle_dict
        pass

    def on_data(self, data_dict: dict, time_dict: dict):
        window_data = data_dict["main"]
        time_data = time_dict["main"][:, :]

        # open_price = window_data[-1, :, 0]
        # high_price = window_data[-1, :, 1]
        # low_price = window_data[-1, :, 2]
        close_price = window_data[-1, :, 3]
        n_security = self.ccy_matrix.shape[1]

        self.bollingerband.on_data(close_price)

        if self.bollingerband.ready:
            curr_BB_upper = self.bollingerband.upper
            curr_BB_middle = self.bollingerband.middle
            curr_BB_lower = self.bollingerband.lower
        else:
            curr_BB_upper = np.ones(n_security) * np.nan
            curr_BB_middle = np.ones(n_security) * np.nan
            curr_BB_lower = np.ones(n_security) * np.nan

        if self.prepared:
            self.BollingerBandUpper = np.concatenate(
                (self.BollingerBandUpper, curr_BB_upper.copy().reshape(1, -1)))
            self.BollingerBandMiddle = np.concatenate(
                (self.BollingerBandMiddle,
                 curr_BB_middle.copy().reshape(1, -1))
            )
            self.BollingerBandLower = np.concatenate(
                (self.BollingerBandLower, curr_BB_lower.copy().reshape(1, -1)))
            self.time = np.concatenate(
                (self.time, np.array([time_data[-1, 0]]).reshape(1, -1)))
        else:
            self.BollingerBandUpper = curr_BB_upper.copy().reshape(1, -1)
            self.BollingerBandMiddle = curr_BB_middle.copy().reshape(1, -1)
            self.BollingerBandLower = curr_BB_lower.copy().reshape(1, -1)
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
    'macd': macd_type,
    'value': nb.float64[:, :],
    'diff': nb.float64[:, :],
    'time': nb.float64[:, :],
    'prepared': nb.boolean
})
class signalMACD(BaseStrategy):
    def init(self):
        n_security = self.ccy_matrix.shape[1]
        length1 = np.int64(self.strategy_param[0])
        length2 = np.int64(self.strategy_param[1])
        length3 = np.int64(self.strategy_param[2])
        self.macd = MACD_P(length1, length2, length3, n_security)
        self.prepared = False  # indicate whether status is created
        self.status = Dict.empty(types.unicode_type, float_array_2d)

    def save_status(self):
        '''可以把需要的status存进strategy_status
           不存任何状态会存一个空的'''
        # strategy_status = Dict.empty(types.unicode_type, nb.float64[:,:])
        self.status['MACDvalue'] = self.value
        self.status['MACDdiff'] = self.diff
        self.status['time'] = self.time

    def load_status(self, pickle_dict):
        # strategy_status = pickle_dict
        pass

    def on_data(self, data_dict: dict, time_dict: dict):
        window_data = data_dict["main"]
        time_data = time_dict["main"][:, :]

        # open_price = window_data[-1, :, 0]
        # high_price = window_data[-1, :, 1]
        # low_price = window_data[-1, :, 2]
        close_price = window_data[-1, :, 3]
        n_security = self.ccy_matrix.shape[1]

        self.macd.on_data(close_price)

        if self.macd.ready:
            curr_MACD_value = self.macd.value
            curr_MACD_diff = self.macd.dif
        else:
            curr_MACD_value = np.ones(n_security) * np.nan
            curr_MACD_diff = np.ones(n_security) * np.nan

        if self.prepared:
            self.value = np.concatenate(
                (self.value, curr_MACD_value.copy().reshape(1, -1)))
            self.diff = np.concatenate(
                (self.diff, curr_MACD_diff.copy().reshape(1, -1)))
            self.time = np.concatenate(
                (self.time, np.array([time_data[-1, 0]]).reshape(1, -1)))
        else:
            self.value = curr_MACD_value.copy().reshape(1, -1)
            self.diff = curr_MACD_diff.copy().reshape(1, -1)
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
    'rsi': rsi_type,
    'RSI': nb.float64[:, :],
    'time': nb.float64[:, :],
    'prepared': nb.boolean
})
class signalRSI(BaseStrategy):
    def init(self):
        n_security = self.ccy_matrix.shape[1]
        length = np.int64(self.strategy_param[0])
        self.rsi = RSI(length, n_security)
        self.prepared = False  # indicate whether status is created
        self.status = Dict.empty(types.unicode_type, float_array_2d)

    def save_status(self):
        '''可以把需要的status存进strategy_status
           不存任何状态会存一个空的'''
        # strategy_status = Dict.empty(types.unicode_type, nb.float64[:,:])
        self.status['RSI'] = self.RSI
        self.status['time'] = self.time

    def load_status(self, pickle_dict):
        # strategy_status = pickle_dict
        pass

    def on_data(self, data_dict: dict, time_dict: dict):
        window_data = data_dict["main"]
        time_data = time_dict["main"][:, :]

        # open_price = window_data[-1, :, 0]
        # high_price = window_data[-1, :, 1]
        # low_price = window_data[-1, :, 2]
        close_price = window_data[-1, :, 3]
        n_security = self.ccy_matrix.shape[1]

        self.rsi.on_data(close_price)

        if self.rsi.ready:
            curr_RSI_value = self.rsi.value
        else:
            curr_RSI_value = np.ones(n_security) * np.nan

        if self.prepared:
            self.RSI = np.concatenate(
                (self.RSI, curr_RSI_value.copy().reshape(1, -1)))
            self.time = np.concatenate(
                (self.time, np.array([time_data[-1, 0]]).reshape(1, -1)))
        else:
            self.RSI = curr_RSI_value.copy().reshape(1, -1)
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
    'DonchianChannelUpper': nb.float64[:, :],
    'DonchianChannelMiddle': nb.float64[:, :],
    'DonchianChannelLower': nb.float64[:, :],
    'time': nb.float64[:, :],
    'prepared': nb.boolean
})
class signalDonchianChannel(BaseStrategy):
    def init(self):
        self.prepared = False  # indicate whether status is created
        self.status = Dict.empty(types.unicode_type, float_array_2d)

    def save_status(self):
        '''可以把需要的status存进strategy_status
           不存任何状态会存一个空的'''
        # strategy_status = Dict.empty(types.unicode_type, nb.float64[:,:])
        self.status['DonchianChannelUpper'] = self.DonchianChannelUpper
        self.status['DonchianChannelMiddle'] = self.DonchianChannelMiddle
        self.status['DonchianChannelLower'] = self.DonchianChannelLower
        self.status['time'] = self.time

    def load_status(self, pickle_dict):
        # strategy_status = pickle_dict
        pass

    def on_data(self, data_dict: dict, time_dict: dict):
        window_data = data_dict["main"]
        time_data = time_dict["main"][:, :]

        # open_price = window_data[-1, :, 0]
        # high_price = window_data[-1, :, 1]
        # low_price = window_data[-1, :, 2]
        # close_price = window_data[-1, :, 3]
        n_security = self.ccy_matrix.shape[1]

        len_MA = np.int64(self.strategy_param[0])

        Donchian_upper = np.zeros(n_security).reshape(n_security)
        Donchian_lower = np.zeros(n_security).reshape(n_security)

        for i in range(n_security):
            Donchian_upper[i] = np.max(window_data[-len_MA: -1, i, 1])
            Donchian_lower[i] = np.min(window_data[-len_MA: -1, i, 2])

        Donchian_middle = (Donchian_upper + Donchian_lower) / 2

        if self.prepared:
            self.DonchianChannelUpper = np.concatenate(
                (self.DonchianChannelUpper,
                 Donchian_upper.copy().reshape(1, -1))
            )
            self.DonchianChannelMiddle = np.concatenate(
                (self.DonchianChannelMiddle,
                 Donchian_middle.copy().reshape(1, -1))
            )
            self.DonchianChannelLower = np.concatenate(
                (self.DonchianChannelLower,
                 Donchian_lower.copy().reshape(1, -1))
            )
            self.time = np.concatenate(
                (self.time, np.array([time_data[-1, 0]]).reshape(1, -1)))
        else:
            self.DonchianChannelUpper = Donchian_upper.copy().reshape(1, -1)
            self.DonchianChannelMiddle = Donchian_middle.copy().reshape(1, -1)
            self.DonchianChannelLower = Donchian_lower.copy().reshape(1, -1)
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
    'ma': ma_type,
    'MA': nb.float64[:, :],
    'time': nb.float64[:, :],
    'prepared': nb.boolean
})
class signalMA(BaseStrategy):
    def init(self):
        n_security = self.ccy_matrix.shape[1]
        len_MA = np.int64(self.strategy_param[0])
        self.ma = MA(len_MA, n_security)
        self.prepared = False  # indicate whether status is created
        self.status = Dict.empty(types.unicode_type, float_array_2d)

    def save_status(self):
        '''可以把需要的status存进strategy_status
           不存任何状态会存一个空的'''
        # strategy_status = Dict.empty(types.unicode_type, nb.float64[:,:])
        self.status['MA'] = self.MA
        self.status['time'] = self.time

    def load_status(self, pickle_dict):
        # strategy_status = pickle_dict
        pass

    def on_data(self, data_dict: dict, time_dict: dict):
        window_data = data_dict["main"]
        time_data = time_dict["main"][:, :]

        # open_price = window_data[-1, :, 0]
        # high_price = window_data[-1, :, 1]
        # low_price = window_data[-1, :, 2]
        close_price = window_data[-1, :, 3]
        n_security = self.ccy_matrix.shape[1]

        self.ma.on_data(close_price)

        if self.ma.ready:
            curr_MA_value = self.ma.value
        else:
            curr_MA_value = np.ones(n_security) * np.nan

        if self.prepared:
            self.MA = np.concatenate(
                (self.MA, curr_MA_value.copy().reshape(1, -1)))
            self.time = np.concatenate(
                (self.time, np.array([time_data[-1, 0]]).reshape(1, -1)))
        else:
            self.MA = curr_MA_value.copy().reshape(1, -1)
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
    'ema': ema_type,
    'EMA': nb.float64[:, :],
    'time': nb.float64[:, :],
    'prepared': nb.boolean
})
class signalEMA(BaseStrategy):
    def init(self):
        n_security = self.ccy_matrix.shape[1]
        len_MA = np.int64(self.strategy_param[0])
        self.ema = EMA(len_MA, n_security)
        self.prepared = False  # indicate whether status is created
        self.status = Dict.empty(types.unicode_type, float_array_2d)

    def save_status(self):
        '''可以把需要的status存进strategy_status
           不存任何状态会存一个空的'''
        # strategy_status = Dict.empty(types.unicode_type, nb.float64[:,:])
        self.status['EMA'] = self.EMA
        self.status['time'] = self.time

    def load_status(self, pickle_dict):
        # strategy_status = pickle_dict
        pass

    def on_data(self, data_dict: dict, time_dict: dict):
        window_data = data_dict["main"]
        time_data = time_dict["main"][:, :]

        # open_price = window_data[-1, :, 0]
        # high_price = window_data[-1, :, 1]
        # low_price = window_data[-1, :, 2]
        close_price = window_data[-1, :, 3]
        n_security = self.ccy_matrix.shape[1]

        self.ema.on_data(close_price)

        if self.ema.ready:
            curr_EMA_value = self.ema.value
        else:
            curr_EMA_value = np.ones(n_security) * np.nan

        if self.prepared:
            self.EMA = np.concatenate(
                (self.EMA, curr_EMA_value.copy().reshape(1, -1)))
            self.time = np.concatenate(
                (self.time, np.array([time_data[-1, 0]]).reshape(1, -1)))
        else:
            self.EMA = curr_EMA_value.copy().reshape(1, -1)
            self.time = np.array([np.float64(time_data[-1, 0])]).reshape(1, -1)
            self.prepared = True

        return np.zeros(n_security).reshape(1, -1)
