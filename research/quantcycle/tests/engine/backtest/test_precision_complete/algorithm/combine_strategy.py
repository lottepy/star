import numpy as np
import numba as nb
from numba.typed import Dict, List
from numba import types
from numba.experimental import jitclass
from quantcycle.utils.indicator import RSI,Bollinger_Bands,MACD_P,Donchian
from quantcycle.app.signal_generator.base_strategy import defaultjitclass, BaseStrategy
from quantcycle.app.pms_manager.portfolio_manager import PorfolioManager

try:
    pms_type = PorfolioManager.class_type.instance_type
    rsi_type = RSI.class_type.instance_type
    bollinger_type = Bollinger_Bands.class_type.instance_type
    donchian_type = Donchian.class_type.instance_type
    macd_type = MACD_P.class_type.instance_type
except:
    pms_type = "PMS_type"
    rsi_type = "RSI_type"
    bollinger_type = "bollinger_type"
    donchian_type = "donchian_type"
    macd_type = "macd_type"

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
    'bollinger1': bollinger_type,
    'bollinger2': bollinger_type,
    'macd': macd_type,
    'macd_donchain': donchian_type,
    'macd_peak': nb.float64[:],
    'cross_low_init': nb.boolean[:],
    'cross_low': nb.boolean[:],
    'cross_low_exit': nb.boolean[:],
    'cross_up_init': nb.boolean[:],
    'cross_up': nb.boolean[:],
    'cross_up_exit': nb.boolean[:],
    'warn_long': nb.boolean[:],
    'warn_short': nb.boolean[:],
})

class Bollinger_two_macd_divergence_update_warning_confirm_strategy(BaseStrategy):

    def init(self):
        n_security = self.ccy_matrix.shape[1]
        length = np.int64(self.strategy_param[0])
        band_width1 = np.float64(self.strategy_param[1])
        band_width2 = np.float64(self.strategy_param[2])
        self.trade_time = np.zeros(n_security, dtype=np.int64)
        self.trade_price = np.zeros(n_security)

        self.bollinger1 = Bollinger_Bands(length, n_security, band_width1)
        self.bollinger2 = Bollinger_Bands(length, n_security, band_width2)
        macd_length1 = np.int64(self.strategy_param[3])
        macd_length2 = np.int64(self.strategy_param[4])
        macd_length3 = np.int64(self.strategy_param[5])
        self.macd = MACD_P(macd_length1,macd_length2,macd_length3, n_security)
        length_dochain = np.int64(self.strategy_param[6])
        self.macd_donchain = Donchian(length_dochain, n_security)
        self.macd_peak = np.zeros(n_security)
        self.cross_low_init = (np.zeros(n_security)!=np.zeros(n_security))
        self.cross_low = (np.zeros(n_security)!=np.zeros(n_security))
        self.cross_low_exit = (np.zeros(n_security)!=np.zeros(n_security))
        self.cross_up_init = (np.zeros(n_security)!=np.zeros(n_security))
        self.cross_up = (np.zeros(n_security)!=np.zeros(n_security))
        self.cross_up_exit = (np.zeros(n_security)!=np.zeros(n_security))
        self.warn_long = (np.zeros(n_security)!=np.zeros(n_security))
        self.warn_short = (np.zeros(n_security)!=np.zeros(n_security))

    def on_data(self, data_dict: dict, time_dict: dict, ref_data_dict: dict, ref_time_dict: dict):
        window_data = data_dict['main']
        time_data = time_dict['main'][:,:]
        current_data = window_data[-1,:,3]

        # get strategy param
        stop_profit = np.float64(self.strategy_param[7])
        stop_loss = np.float64(self.strategy_param[8])
        max_hold_days = np.int64(self.strategy_param[9])

        # get current price
        open_price = window_data[-1, :, 0]
        high_price = window_data[-1, :, 1]
        low_price = window_data[-1, :, 2]
        close_price = window_data[-1, :, 3]

        curr_close = window_data[-1, :, 3]
        prev_close = window_data[-2, :, 3]

        n_security = self.ccy_matrix.shape[1]

        # update indicator
        prev_upper2_value = np.copy(self.bollinger2.upper)
        prev_upper1_value = np.copy(self.bollinger1.upper)
        prev_middle_value = np.copy(self.bollinger1.middle)
        prev_lower1_value = np.copy(self.bollinger1.lower)
        prev_lower2_value = np.copy(self.bollinger2.lower)

        prev_bollinger1_ready = np.array([self.bollinger1.ready])[0]
        prev_bollinger2_ready = np.array([self.bollinger2.ready])[0]

        self.bollinger1.on_data(close_price)
        self.bollinger2.on_data(close_price)

        curr_upper2_value = np.copy(self.bollinger2.upper)
        curr_upper1_value = np.copy(self.bollinger1.upper)
        curr_middle_value = np.copy(self.bollinger1.middle)
        curr_lower1_value = np.copy(self.bollinger1.lower)
        curr_lower2_value = np.copy(self.bollinger2.lower)

        curr_bollinger1_ready = self.bollinger1.ready
        curr_bollinger2_ready = self.bollinger2.ready

        self.macd.on_data(close_price)
        curr_macd_value = self.macd.value
        curr_macd_ready = self.macd.ready

        self.macd_donchain.on_data(curr_macd_value, curr_macd_value)
        curr_macd_high = self.macd_donchain.upper
        curr_macd_low = self.macd_donchain.lower
        curr_macd_donchain_ready = self.macd_donchain.ready

        ccp_current = self.portfolio_manager.current_holding.copy()
        ccp_current[ccp_current > 0] = 1
        ccp_current[ccp_current < 0] = -1

        # signal_condition
        up_cross_lower2  = (prev_close < prev_lower2_value) & \
                           (curr_close > curr_lower2_value)
        up_cross_lower1  = (prev_close < prev_lower1_value) & \
                           (curr_close > curr_lower1_value)
        up_cross_middle = (prev_close < prev_middle_value) & \
                          (curr_close > curr_middle_value)
        up_cross_upper1  = (prev_close <  prev_upper1_value) & \
                           (curr_close > curr_upper1_value)
        up_cross_upper2  = (prev_close <  prev_upper2_value) & \
                           (curr_close > curr_upper2_value)
        down_cross_lower2 = (prev_close > prev_lower2_value) & \
                           (curr_close < curr_lower2_value)
        down_cross_lower1 = (prev_close > prev_lower1_value) & \
                           (curr_close < curr_lower1_value)
        down_cross_middle = (prev_close > prev_middle_value) & \
                            (curr_close < curr_middle_value)
        down_cross_upper1  = (prev_close > prev_upper1_value) & \
                             (curr_close < curr_upper1_value)
        down_cross_upper2  = (prev_close > prev_upper2_value) & \
                             (curr_close < curr_upper2_value)

        # 初次下穿底部
        self.cross_low_init = (down_cross_lower2 & \
                                prev_bollinger2_ready & curr_macd_ready & \
                                curr_macd_donchain_ready & (ccp_current==0))
        self.cross_up_init = ( up_cross_upper2 & \
                                prev_bollinger2_ready & curr_macd_ready & \
                                curr_macd_donchain_ready & (ccp_current==0))

        self.warn_long[self.cross_low_init] = 0
        self.warn_short[self.cross_up_init] = 0

        self.macd_peak[self.cross_low_init] = curr_macd_value[self.cross_low_init]
        self.macd_peak[self.cross_up_init] = curr_macd_value[self.cross_up_init]

        self.cross_low = self.cross_low_init|self.cross_low
        self.cross_up = self.cross_up_init|self.cross_up

        self.cross_low_init[self.cross_low] = 0
        self.cross_up_init[self.cross_up] = 0

        self.macd_peak[self.cross_low] = np.minimum(self.macd_peak[self.cross_low], \
                                                   curr_macd_value[self.cross_low])
        self.macd_peak[self.cross_up] = np.maximum(self.macd_peak[self.cross_up], \
                                                   curr_macd_value[self.cross_up])

        self.cross_low_exit = self.cross_low & up_cross_lower2
        self.cross_up_exit = self.cross_up & down_cross_upper2

        self.cross_low[self.cross_low_exit] = 0
        self.cross_up[self.cross_up_exit] = 0

        self.warn_long = ((self.cross_low_exit) & \
                         (self.macd_peak > curr_macd_low))|self.warn_long

        self.warn_short = ((self.cross_up_exit) & \
                         (self.macd_peak < curr_macd_high))|self.warn_short

        self.cross_low_exit[:] = 0
        self.cross_up_exit[:] = 0
        #### position ####

        open_long_condition = self.warn_long & up_cross_lower1 & \
                              (ccp_current == 0)
        self.warn_long = self.warn_long & (open_long_condition==0)

        open_short_condition = self.warn_short & down_cross_upper1 & \
                              (ccp_current == 0)
        self.warn_short = self.warn_short & (open_short_condition==0)

        close_long_condition = (ccp_current > 0.) * up_cross_upper2
        close_short_condition = (ccp_current < 0.) * down_cross_lower2

        #mandatory condition
        current_price_level = np.ones_like(close_price)
        current_price_level[self.trade_price != 0] = np.divide(close_price, self.trade_price)[self.trade_price != 0]
        current_pnl = current_price_level - 1
        stop_loss_condition = ((ccp_current > 0.) & (current_pnl < -stop_loss)) | \
                              ((ccp_current < 0.) & (current_pnl > stop_loss))
        stop_profit_condition = ((ccp_current > 0.) & (current_pnl > stop_profit)) | \
                                ((ccp_current < 0.) & (current_pnl < -stop_profit))
        hold_period_condition = (((time_data[-1, 0] - self.trade_time) / 86400) > max_hold_days) & \
                                (ccp_current != 0.)
        mandatory_condition = stop_loss_condition|stop_profit_condition|hold_period_condition
        close_condition = mandatory_condition|close_long_condition|close_short_condition

        #### update target position ####
        ccp_target = self.portfolio_manager.current_holding.copy()
        ccp_target[ccp_target > 0] = 1
        ccp_target[ccp_target < 0] = -1

        if prev_bollinger1_ready & prev_bollinger2_ready & \
           curr_macd_ready & curr_macd_donchain_ready:

            ccp_target[open_long_condition] = 1
            ccp_target[open_short_condition] = -1
            ccp_target[close_condition] = 0

        has_trade = (ccp_target != ccp_current)
        self.trade_time[has_trade] = time_data[-1][0]
        self.trade_price[has_trade] = close_price[has_trade]

        ref_aum = self.portfolio_manager.init_cash
        weight = ccp_target / n_security
        order = self.return_reserve_target_base_ccy(weight * ref_aum)
        return order.reshape(1,-1)
