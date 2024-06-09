import numpy as np
import numba as nb
from numba.typed import Dict, List
from numba import types
from numba.experimental import jitclass
from quantcycle.utils.indicator import MA,EMA
from quantcycle.app.signal_generator.base_strategy import defaultjitclass, BaseStrategy
from quantcycle.app.pms_manager.portfolio_manager import PorfolioManager

try:
    pms_type = PorfolioManager.class_type.instance_type
    MA_list_type = MA.class_type.instance_type
    EMA_list_type = EMA.class_type.instance_type
    MA_short_type = MA.class_type.instance_type
    MA_long_type = MA.class_type.instance_type
except:
    pms_type = "PMS_type"
    MA_list_type = "MA_list_type"
    EMA_list_type = "EMA_list_type"
    MA_short_type = "MA_short_type"
    MA_long_type = "MA_long_type"


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
    'MA_list': MA_list_type,
    'trade_time': nb.int64[:],
    'trade_price': nb.float64[:]
})
class single_MA_strategy(BaseStrategy):

    def init(self):
        length = np.int64(self.strategy_param[3])
        n_security = self.ccy_matrix.shape[1]
        self.MA_list = MA(length, n_security)
        self.trade_time = np.zeros(n_security, dtype=np.int64)
        self.trade_price = np.zeros(n_security)

    def save_status(self):
        '''可以把需要的status存进strategy_status
           不存任何状态会存一个空的'''
        strategy_status = Dict.empty(types.unicode_type, nb.float64)
        strategy_status['MA_list'] = self.MA_list.value[0]
        self.status = strategy_status

    def load_status(self, pickle_dict):
        strategy_status = pickle_dict

    def on_data(self, data_dict: dict, time_dict: dict, ref_data_dict: dict, ref_time_dict: dict):
        window_data = data_dict['main']
        time_data = time_dict['main'][:,:]
        current_data = window_data[-1,:,3]

        n_security = self.ccy_matrix.shape[1]

        stop_profit = np.float64(self.strategy_param[0])
        stop_loss = np.float64(self.strategy_param[1])
        max_hold_minute = np.int64(self.strategy_param[2])

        curr_close      = window_data[-1, :, 3]
        prev_close = window_data[-2, :, 3]
        prev_MA_value        = np.copy(self.MA_list.value)

        self.MA_list.on_data(curr_close)

        curr_MA_value        = np.copy(self.MA_list.value)
        curr_MA_ready        = np.array([self.MA_list.ready])[0]
        ccp_target           = self.portfolio_manager.current_holding.copy()

        empty_position_condition = (ccp_target == 0.)
        long_position_condition = (ccp_target > 0.)
        short_position_condition = (ccp_target < 0.)

        stop_profit_condition = long_position_condition * (curr_close / self.trade_price > 1 + stop_profit) \
                                + short_position_condition * (self.trade_price / curr_close > 1 + stop_profit)

        stop_loss_condition = long_position_condition * (curr_close / self.trade_price < 1 - stop_loss) \
                              + short_position_condition * (self.trade_price / curr_close < 1 - stop_loss)

        hold_period_condition = (time_data[-1, 0] - self.trade_time) / 86400 > max_hold_minute
        mandatory_condition = stop_profit_condition | stop_loss_condition | hold_period_condition

        up_cross_condition = (prev_close < prev_MA_value) *  (curr_close > curr_MA_value)
        down_cross_condition = (prev_close > prev_MA_value) *  (curr_close < curr_MA_value)

        open_long_condition = empty_position_condition * up_cross_condition
        open_short_condition = empty_position_condition * down_cross_condition
        open_position_condition = open_long_condition + open_short_condition * (-1)

        close_long_condition = long_position_condition * (mandatory_condition | down_cross_condition)
        close_short_condition = short_position_condition * (mandatory_condition | up_cross_condition)
        close_position_condition = (close_long_condition | close_short_condition)

        if curr_MA_ready:
            for i in range(n_security):
                if empty_position_condition[i]:
                    ccp_target[i] = open_position_condition[i]
                    self.trade_time[i] = time_data[-1, 0]
                    self.trade_price[i] = curr_close[i]

                if not empty_position_condition[i] and close_position_condition[i]:
                    ccp_target[i] = 0.
                    self.trade_time[i] = 0.
                    self.trade_price[i] = 0.

        target_signal = np.sign(ccp_target)
        ref_aum = self.portfolio_manager.init_cash
        weight = target_signal / n_security
        order = self.return_reserve_target_base_ccy(weight * ref_aum)
        return order.reshape(1,-1)

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
    'EMA_list': EMA_list_type,
    'trade_time': nb.int64[:],
    'trade_price': nb.float64[:]
})
class single_EMA_strategy(BaseStrategy):

    def init(self):
        length = np.int64(self.strategy_param[3])
        n_security = self.ccy_matrix.shape[1]
        self.EMA_list = EMA(length, n_security)
        self.trade_time = np.zeros(n_security, dtype=np.int64)
        self.trade_price = np.zeros(n_security)

    def save_status(self):
        '''可以把需要的status存进strategy_status
           不存任何状态会存一个空的'''
        strategy_status = Dict.empty(types.unicode_type, nb.float64)
        strategy_status['EMA_list'] = self.EMA_list.value[0]
        self.status = strategy_status

    def load_status(self, pickle_dict):
        strategy_status = pickle_dict

    def on_data(self, data_dict: dict, time_dict: dict, ref_data_dict: dict, ref_time_dict: dict):
        window_data = data_dict['main']
        time_data = time_dict['main'][:,:]
        current_data = window_data[-1,:,3]

        n_security = self.ccy_matrix.shape[1]

        stop_profit = np.float64(self.strategy_param[0])
        stop_loss = np.float64(self.strategy_param[1])
        max_hold_minute = np.int64(self.strategy_param[2])

        curr_close      = window_data[-1, :, 3]
        prev_close = window_data[-2, :, 3]
        prev_MA_value        = np.copy(self.EMA_list.value)

        self.EMA_list.on_data(curr_close)

        curr_MA_value        = np.copy(self.EMA_list.value)
        curr_MA_ready        = np.array([self.EMA_list.ready])[0]
        ccp_target           = self.portfolio_manager.current_holding.copy()

        empty_position_condition = (ccp_target == 0.)
        long_position_condition = (ccp_target > 0.)
        short_position_condition = (ccp_target < 0.)

        stop_profit_condition = long_position_condition * (curr_close / self.trade_price > 1 + stop_profit) \
                                + short_position_condition * (self.trade_price / curr_close > 1 + stop_profit)

        stop_loss_condition = long_position_condition * (curr_close / self.trade_price < 1 - stop_loss) \
                              + short_position_condition * (self.trade_price / curr_close < 1 - stop_loss)

        hold_period_condition = (time_data[-1, 0] - self.trade_time) / 86400 > max_hold_minute
        mandatory_condition = stop_profit_condition | stop_loss_condition | hold_period_condition

        up_cross_condition = (prev_close < prev_MA_value) * (curr_close > curr_MA_value)
        down_cross_condition = (prev_close > prev_MA_value) * (curr_close < curr_MA_value)

        open_long_condition = empty_position_condition * up_cross_condition
        open_short_condition = empty_position_condition * down_cross_condition
        open_position_condition = open_long_condition + open_short_condition * (-1)

        close_long_condition = long_position_condition * (mandatory_condition | down_cross_condition)
        close_short_condition = short_position_condition * (mandatory_condition | up_cross_condition)
        close_position_condition = (close_long_condition | close_short_condition)

        if curr_MA_ready:
            for i in range(n_security):
                if empty_position_condition[i]:
                    ccp_target[i] = open_position_condition[i]
                    self.trade_time[i] = time_data[-1, 0]
                    self.trade_price[i] = curr_close[i]

                if not empty_position_condition[i] and close_position_condition[i]:
                    ccp_target[i] = 0.
                    self.trade_time[i] = 0.
                    self.trade_price[i] = 0.

        target_signal = np.sign(ccp_target)
        ref_aum = self.portfolio_manager.init_cash

        sum_ = np.sum(np.abs(target_signal))

        if sum_ != 0:
            ccp_target = target_signal / n_security * ref_aum

        target_signal = np.sign(ccp_target)
        ref_aum = self.portfolio_manager.init_cash
        weight = target_signal / n_security
        order = self.return_reserve_target_base_ccy(weight * ref_aum)
        return order.reshape(1,-1)

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
    'MA_short': MA_short_type,
    'MA_long': MA_long_type,
    'trade_time': nb.int64[:],
    'trade_price': nb.float64[:]
})
class double_MA_strategy(BaseStrategy):

    def init(self):
        short_len = np.int64(self.strategy_param[3])
        long_len = np.int64(self.strategy_param[4])
        n_security = self.ccy_matrix.shape[1]
        self.MA_short = MA(short_len, n_security)
        self.MA_long = MA(long_len, n_security)
        self.trade_time = np.zeros(n_security, dtype=np.int64)
        self.trade_price = np.zeros(n_security)

    def save_status(self):
        '''可以把需要的status存进strategy_status
           不存任何状态会存一个空的'''
        strategy_status = Dict.empty(types.unicode_type, nb.float64)
        strategy_status['MA_short_list'] = self.MA_short.value[0]
        strategy_status['MA_long_list'] = self.MA_long.value[0]
        self.status = strategy_status

    def load_status(self, pickle_dict):
        strategy_status = pickle_dict

    def on_data(self, data_dict: dict, time_dict: dict, ref_data_dict: dict, ref_time_dict: dict):
        window_data = data_dict['main']
        time_data = time_dict['main'][:,:]
        current_data = window_data[-1,:,3]

        stop_profit = np.float64(self.strategy_param[0])
        stop_loss = np.float64(self.strategy_param[1])
        max_hold_minute = np.int64(self.strategy_param[2])

        n_security = self.ccy_matrix.shape[1]
        curr_close = window_data[-1, :, 3]
        prev_short_value = np.copy(self.MA_short.value)
        prev_long_value = np.copy(self.MA_long.value)

        self.MA_short.on_data(curr_close)
        self.MA_long.on_data(curr_close)

        curr_short_value = np.copy(self.MA_short.value)
        curr_long_value = np.copy(self.MA_long.value)
        curr_long_ready = np.array([self.MA_long.ready])[0]
        ccp_target = self.portfolio_manager.current_holding.copy()

        empty_position_condition = (ccp_target == 0.)
        long_position_condition = (ccp_target > 0.)
        short_position_condition = (ccp_target < 0.)

        stop_profit_condition = long_position_condition * (curr_close / self.trade_price > 1 +stop_profit) \
                                + short_position_condition * (self.trade_price / curr_close > 1 + stop_profit)

        stop_loss_condition = long_position_condition * (curr_close / self.trade_price < 1 - stop_loss) \
                              + short_position_condition * (self.trade_price / curr_close < 1 - stop_loss)

        hold_period_condition = (time_data[-1, 0] - self.trade_time) / 86400 > max_hold_minute

        mandatory_condition = stop_profit_condition | stop_loss_condition | hold_period_condition

        up_cross_condition = (prev_short_value < prev_long_value) * (curr_short_value > curr_long_value)
        down_cross_condition = (prev_short_value > prev_long_value) * (curr_short_value < curr_long_value)

        open_long_condition = empty_position_condition * up_cross_condition
        open_short_condition = empty_position_condition * down_cross_condition
        open_position_condition = open_long_condition + open_short_condition * (-1)

        close_long_condition = long_position_condition * (mandatory_condition | down_cross_condition)
        close_short_condition = short_position_condition * (mandatory_condition | up_cross_condition)
        close_position_condition = (close_long_condition | close_short_condition)

        if curr_long_ready:
            for i in range(n_security):
                if empty_position_condition[i]:
                    ccp_target[i] = open_position_condition[i]
                    self.trade_time[i] = time_data[-1, 0]
                    self.trade_price[i] = curr_close[i]

                if not empty_position_condition[i] and close_position_condition[i]:
                    ccp_target[i] = 0.
                    self.trade_time[i] = 0.
                    self.trade_price[i] = 0.

        target_signal = np.sign(ccp_target)
        ref_aum = self.portfolio_manager.init_cash
        weight = target_signal / n_security
        order = self.return_reserve_target_base_ccy(weight * ref_aum)
        return order.reshape(1,-1)

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
    'trade_time': nb.int64[:],
    'trade_price': nb.float64[:],
    'stop_profit': nb.float64,
    'stop_loss': nb.float64,
    'max_hold_minute': nb.float64,
    'len_MA': nb.int64,
    'curr_close': nb.float64[:,:,:],
    'curr_high': nb.float64[:,:,:],
    'curr_low': nb.float64[:,:,:]
})
class Donchian_Channel_strategy(BaseStrategy):

    def init(self):
       
        self.stop_profit = np.float(self.strategy_param[0])
        self.stop_loss = np.float(self.strategy_param[1])
        self.max_hold_minute = np.float(self.strategy_param[2])
        self.len_MA = np.int(self.strategy_param[3])
       
        n_security = self.ccy_matrix.shape[1]
        self.trade_time = np.zeros(n_security, dtype=np.int64)
        self.trade_price = np.zeros(n_security)

    def save_status(self):
        '''可以把需要的status存进strategy_status
           不存任何状态会存一个空的'''
        strategy_status = Dict.empty(types.unicode_type, nb.float64)
        # TODO 
        self.status = strategy_status

    def load_status(self, pickle_dict):
        strategy_status = pickle_dict

    def on_data(self, data_dict: dict, time_dict: dict, ref_data_dict: dict, ref_time_dict: dict):
        window_data = data_dict['main']
        time_data = time_dict['main'][:,:]
        current_data = window_data[-1,:,3]

        n_security = self.ccy_matrix.shape[1]

        self.curr_close = window_data[-1, :, 3]

        self.curr_high = window_data[-1, :, 1]
        self.curr_low = window_data[-1, :, 2]
        
        # TODO fix np.max np.min for working numba
        donchian_upper = np.arange(n_security).reshape(n_security)
        donchian_lower = np.arange(n_security).reshape(n_security)
        for i in range(n_security):
            donchian_upper[i] = np.max(window_data[-self.len_MA: -1, i, 1])
            donchian_lower[i] = np.min(window_data[-self.len_MA: -1, i, 1])

        # donchian_upper = np.max(window_data[-self.len_MA: -1, :, 1], axis=0)
        # donchian_lower = np.min(window_data[-self.len_MA: -1, :, 2], axis=0)
        donchian_middle = (donchian_upper + donchian_lower) / 2
 
        # MA = np.mean(window_data[-self.len_MA: -1, :, 3], axis=0)
        ccp_target = self.portfolio_manager.current_holding.copy()

        empty_position_condition = (ccp_target == 0.)
        long_position_condition = (ccp_target > 0.)
        short_position_condition = (ccp_target < 0.)

        stop_profit_condition = long_position_condition * (self.curr_close / self.trade_price > 1 + self.stop_profit) \
                                + short_position_condition * (self.trade_price / self.curr_close > 1 + self.stop_profit)

        stop_loss_condition = long_position_condition * (self.curr_close / self.trade_price < 1 - self.stop_loss) \
                              + short_position_condition * (self.trade_price / self.curr_close < 1 - self.stop_loss)

        hold_period_condition = (time_data[-1, 0] - self.trade_time) / 86400 > self.max_hold_minute

        mandatory_condition = (stop_profit_condition | stop_loss_condition | hold_period_condition)

        upCross_upper_condition = (self.curr_close > donchian_upper)
        upCross_middle_condition = (self.curr_close > donchian_middle)

        downCross_middle_condition = (self.curr_close < donchian_middle)
        downCross_lower_condition = (self.curr_close < donchian_lower)

        open_long_condition = empty_position_condition * downCross_lower_condition
        open_short_condition = empty_position_condition * upCross_upper_condition
        open_position_condition = open_long_condition + open_short_condition * (-1)

        close_long_condition = long_position_condition * (mandatory_condition | upCross_middle_condition)
        close_short_condition = short_position_condition * (mandatory_condition | downCross_middle_condition)
        close_position_condition = (close_long_condition | close_short_condition)

        if True:
            for i in range(n_security):
                if empty_position_condition[i]:
                    ccp_target[i] = open_position_condition[i]
                    self.trade_time[i] = time_data[-1, 0]
                    self.trade_price[i] = self.curr_close[i]

                if not empty_position_condition[i] and close_position_condition[i]:
                    ccp_target[i] = 0.
                    self.trade_time[i] = 0.
                    self.trade_price[i] = 0.

        target_signal = np.sign(ccp_target)
        ref_aum = self.portfolio_manager.init_cash
        weight = target_signal / n_security
        order = self.return_reserve_target_base_ccy(weight * ref_aum)
        return order.reshape(1,-1)