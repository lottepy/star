import numpy as np
import numba as nb
from numba.typed import Dict, List
from numba import types
from numba.experimental import jitclass
from quantcycle.utils.indicator import RSI, MACD_P, KD, CCI, Moving_series, moving_series_type
from quantcycle.app.signal_generator.base_strategy import defaultjitclass, BaseStrategy
from quantcycle.app.pms_manager.portfolio_manager import PorfolioManager

try:
    pms_type = PorfolioManager.class_type.instance_type
    rsi_type = RSI.class_type.instance_type
    kd_type = KD.class_type.instance_type
    cci_type = CCI.class_type.instance_type
    macd_type = MACD_P.class_type.instance_type
except:
    pms_type = "PMS_type"
    rsi_type = "RSI_type"
    kd_type = "KD_type"
    cci_type = "CCI_type"
    macd_type = "MACD_type"
##===========================================================================##


@defaultjitclass({
    'rsi': rsi_type,
    'trade_time': nb.int64[:],
    'trade_price': nb.float64[:]
})
class RSI_strategy(BaseStrategy):
    # open long:   rsi(t-1)< 50 - break_threshold, rsi(t)>50 - break_threshold
    # open short:  rsi(t-1)> 50 + break_threshold, rsi(t)<50 + break_threshold
    # close long:   rsi(t-1)< 50 + break_threshold, rsi(t)>50 + break_threshold
    # close short:  rsi(t-1)> 50 - break_threshold, rsi(t)<50 - break_threshold
    def init(self):
        n_security = self.ccy_matrix.shape[1]
        length = np.int64(self.strategy_param[0])
        self.rsi = RSI(length,n_security)
        self.trade_time = np.zeros(n_security, dtype=np.int64)
        self.trade_price = np.zeros(n_security)

    def save_status(self):
        '''可以把需要的status存进strategy_status
           不存任何状态会存一个空的'''
        strategy_status = Dict.empty(types.unicode_type, nb.float64)
        strategy_status['rsi'] = self.rsi.value[0]
        self.status = strategy_status

    def load_status(self, pickle_dict):
        strategy_status = pickle_dict

    def on_data(self, data_dict: dict, time_dict: dict, ref_data_dict: dict, ref_time_dict: dict):
        window_data = data_dict['main']
        time_data = time_dict['main'][:,:]

        current_data = window_data[-1,:,3]

        break_threshold = np.float64(self.strategy_param[1])
        stop_profit = np.float64(self.strategy_param[2])
        stop_loss = np.float64(self.strategy_param[3])
        max_hold_days = np.int64(self.strategy_param[4])

        close_price = window_data[-1, :, 3]
        n_security = self.ccy_matrix.shape[1]

        prev_rsi_value = np.copy(self.rsi.value)
        prev_rsi_ready = np.array([self.rsi.ready])[0]
        self.rsi.on_data(close_price)
        curr_rsi_value = self.rsi.value

        ccp_current = self.portfolio_manager.current_holding.copy()
        ccp_current[ccp_current > 0] = 1
        ccp_current[ccp_current < 0] = -1

        #### signals ####

        up_cross_upper = (prev_rsi_value < 50 + break_threshold) & \
                         (curr_rsi_value > 50 + break_threshold)
        up_cross_lower = (prev_rsi_value < 50 - break_threshold) & \
                         (curr_rsi_value > 50 - break_threshold)
        down_cross_upper = (prev_rsi_value > 50 + break_threshold) & \
                           (curr_rsi_value < 50 + break_threshold)
        down_cross_lower = (prev_rsi_value > 50 - break_threshold) & \
                           (curr_rsi_value < 50 - break_threshold)

        #### position signal ####
        open_long_condition = (ccp_current == 0) & up_cross_lower
        open_short_condition = (ccp_current == 0) & down_cross_upper
        close_long_condition = (ccp_current > 0) & up_cross_upper
        close_short_condition = (ccp_current < 0) & down_cross_lower

        current_price_level = np.ones_like(close_price)
        current_price_level[self.trade_price != 0] = np.divide(close_price, self.trade_price)[self.trade_price != 0]
        current_pnl = current_price_level - 1
        stop_loss_condition = ((ccp_current > 0.) * (current_pnl < -stop_loss)) | \
                              ((ccp_current < 0.) * (current_pnl > stop_loss))
        stop_profit_condition = ((ccp_current > 0.) * (current_pnl > stop_profit)) | \
                                ((ccp_current < 0.) * (current_pnl < -stop_profit))
        hold_period_condition = (((time_data[-1, 0] - self.trade_time) / 86400) > max_hold_days) & \
                                (ccp_current != 0.)
        mandatory_condition = stop_loss_condition | stop_profit_condition | hold_period_condition
        close_condition = mandatory_condition | close_long_condition | close_short_condition

        ccp_target = self.portfolio_manager.current_holding.copy()
        ccp_target[ccp_target > 0] = 1
        ccp_target[ccp_target < 0] = -1

        if prev_rsi_ready:
            ccp_target[close_condition] = 0
            ccp_target[open_long_condition] = 1
            ccp_target[open_short_condition] = -1


        #### record trade has happened ####
        has_trade = (ccp_target != ccp_current)
        self.trade_time[has_trade] = time_data[-1][0]
        self.trade_price[has_trade] = close_price[has_trade]

        ref_aum = self.portfolio_manager.init_cash
        weight = ccp_target / n_security
        order = self.return_reserve_target_base_ccy(weight * ref_aum)
        return order.reshape(1,-1)
##===========================================================================##
@defaultjitclass({
    'macd': macd_type,
    'trade_time': nb.int64[:],
    'trade_price': nb.float64[:]
})
class MACD_P_strategy(BaseStrategy):
    # macd strategy
    # 开多:  dif < - break_threshold and macd金叉
    # 平多:  macd死叉 or mandatory condition
    # 开空:  dif > break_threshold and macd死叉
    # 平空:  macd金叉 or mandatory condition
    def init(self):
        n_security = self.ccy_matrix.shape[1]
        length1 = np.int64(self.strategy_param[0])
        length2 = np.int64(self.strategy_param[1])
        length3 = np.int64(self.strategy_param[2])
        self.macd = MACD_P(length1, length2, length3, n_security)
        self.trade_time = np.zeros(n_security, dtype=np.int64)
        self.trade_price = np.zeros(n_security)

    def save_status(self):
        '''可以把需要的status存进strategy_status
           不存任何状态会存一个空的'''
        strategy_status = Dict.empty(types.unicode_type, nb.float64)
        strategy_status['macd_p'] = self.macd.value[0]
        self.status = strategy_status

    def load_status(self, pickle_dict):
        strategy_status = pickle_dict

    def on_data(self, data_dict: dict, time_dict: dict, ref_data_dict: dict, ref_time_dict: dict):
        window_data = data_dict['main']
        time_data = time_dict['main'][:,:]

        current_data = window_data[-1,:,3]

        #### 策略参数 ####
        break_threshold = np.float64(self.strategy_param[3])
        stop_profit = np.float64(self.strategy_param[4])
        stop_loss = np.float64(self.strategy_param[5])
        max_hold_days = np.int64(self.strategy_param[6])
        n_security = self.ccy_matrix.shape[1]

        #### 更新指标值 ####
        close_price = window_data[-1, :, 0]

        prev_macd_value = np.copy(self.macd.value)
        prev_dif_value = np.copy(self.macd.dif)
        prev_macd_ready = np.array([self.macd.ready])[0]

        self.macd.on_data(close_price)

        curr_macd_value = self.macd.value
        curr_dif_value = self.macd.dif

        #### 得到目前仓位 ####
        ccp_current = self.portfolio_manager.current_holding.copy()
        ccp_current[ccp_current > 0] = 1
        ccp_current[ccp_current < 0] = -1



        #### signal ####
        #金叉死叉的位置也可以作为参数
        macd_up_cross_middle = (prev_macd_value < 0) & (curr_macd_value > 0)
        macd_down_cross_middle = (prev_macd_value > 0) & (curr_macd_value < 0)
        dif_upper_cross_middle = (prev_dif_value < 0) & (curr_dif_value > 0)
        dif_down_cross_middle = (prev_dif_value > 0) & (curr_dif_value < 0)

        oversold_condition = curr_dif_value < -break_threshold
        overbought_condition = curr_dif_value > break_threshold

        #### open/close condition ####
        open_long_condition = (ccp_current == 0.) & macd_up_cross_middle & oversold_condition
        open_short_condition = (ccp_current == 0.) & macd_down_cross_middle & overbought_condition

        close_long_condition = (ccp_current > 0.) & dif_down_cross_middle
        close_short_condition = (ccp_current < 0.) & dif_upper_cross_middle

        #### mandatory condition ####
        current_price_level = np.ones_like(close_price)
        current_price_level[self.trade_price != 0] = np.divide(close_price, self.trade_price)[self.trade_price != 0]
        current_pnl = current_price_level - 1
        stop_loss_condition = ((ccp_current > 0.) * (current_pnl < -stop_loss)) | \
                              ((ccp_current < 0.) * (current_pnl > stop_loss))
        stop_profit_condition = ((ccp_current > 0.) * (current_pnl > stop_profit)) | \
                                ((ccp_current < 0.) * (current_pnl < -stop_profit))
        hold_period_condition = (((time_data[-1, 0] - self.trade_time) / 86400) > max_hold_days) & \
                                (ccp_current != 0.)
        mandatory_condition = stop_loss_condition | stop_profit_condition | hold_period_condition
        close_condition = mandatory_condition | close_long_condition | close_short_condition

        #### get target position ####
        ccp_target = self.portfolio_manager.current_holding.copy()
        ccp_target[ccp_target > 0] = 1
        ccp_target[ccp_target < 0] = -1

        if prev_macd_ready:
            ccp_target[close_condition] = 0
            ccp_target[open_long_condition] = 1
            ccp_target[open_short_condition] = -1

        #### record trade has happened ####
        has_trade = (ccp_target != ccp_current)
        self.trade_time[has_trade] = time_data[-1][0]
        self.trade_price[has_trade] = close_price[has_trade]

        ref_aum = self.portfolio_manager.init_cash
        weight = ccp_target / n_security
        order = self.return_reserve_target_base_ccy(weight * ref_aum)
        return order.reshape(1,-1)
##===========================================================================##
@defaultjitclass({
    'kd': kd_type,
    'trade_time': nb.int64[:],
    'trade_price': nb.float64[:]
})
class KD_strategy(BaseStrategy):
    # if (Kt-1 < Dt-1 & Kt > Dt) & Dt < 50-break_threshold , open long position
    # if (Kt-1 > Dt-1 & Kt < Dt) & Dt > 50+break_threshold , open short position
    # if Dt > 50+break_threshold, close long position
    # if Dt < 50-break_threshold, close short position

    def init(self):
        n_security = self.ccy_matrix.shape[1]
        length1 = np.int64(self.strategy_param[0])
        length2 = np.int64(self.strategy_param[1])
        length3 = np.int64(self.strategy_param[2])
        self.kd = KD(length1, length2, length3, n_security)
        self.trade_time = np.zeros(n_security, dtype=np.int64)
        self.trade_price = np.zeros(n_security)

    def save_status(self):
        '''可以把需要的status存进strategy_status
           不存任何状态会存一个空的'''
        strategy_status = Dict.empty(types.unicode_type, nb.float64)
        strategy_status['kd'] = self.kd.value[0]
        self.status = strategy_status

    def load_status(self, pickle_dict):
        strategy_status = pickle_dict

    def on_data(self, data_dict: dict, time_dict: dict, ref_data_dict: dict, ref_time_dict: dict):
        window_data = data_dict['main']
        time_data = time_dict['main'][:,:]

        current_data = window_data[-1,:,3]

        #### 策略参数 ####
        break_threshold = np.float64(self.strategy_param[3])
        stop_profit = np.float64(self.strategy_param[4])
        stop_loss = np.float64(self.strategy_param[5])
        max_hold_days = np.int64(self.strategy_param[6])

        high_price = window_data[-1, :, 1]
        low_price = window_data[-1, :, 2]
        close_price = window_data[-1, :, 3]
        n_security = self.ccy_matrix.shape[1]

        #### 更新指标值 ####
        prev_K_value = np.copy(self.kd.fast_d.value)
        prev_D_value = np.copy(self.kd.slow_d.value)
        prev_kd_ready = np.array([self.kd.ready])[0]

        self.kd.on_data(high_price, low_price, close_price)

        curr_K_value = self.kd.fast_d.value
        curr_D_value = self.kd.slow_d.value
        #### signals ####
        up_cross_condition = (prev_K_value < prev_D_value) & \
                             (curr_K_value >= curr_D_value)
        down_cross_condition = (prev_K_value > prev_D_value) & \
                               (curr_K_value <= curr_D_value)
        overbought_condition = curr_D_value >= 50 + break_threshold
        oversold_condition = curr_D_value <= 50 - break_threshold

        #### position ####
        ccp_current = self.portfolio_manager.current_holding.copy()
        ccp_current[ccp_current > 0] = 1
        ccp_current[ccp_current < 0] = -1

        open_long_condition = (ccp_current == 0) & up_cross_condition & oversold_condition
        open_short_condition = (ccp_current == 0) & down_cross_condition & overbought_condition
        close_long_condition = (ccp_current > 0) & overbought_condition
        close_short_condition = (ccp_current < 0) & oversold_condition

        # stop loss
        current_price_level = np.ones_like(close_price)
        current_price_level[self.trade_price != 0] = np.divide(close_price, self.trade_price)[self.trade_price != 0]
        current_pnl = current_price_level - 1
        stop_loss_condition = ((ccp_current > 0.) * (current_pnl < -stop_loss)) | \
                              ((ccp_current < 0.) * (current_pnl > stop_loss))
        stop_profit_condition = ((ccp_current > 0.) * (current_pnl > stop_profit)) | \
                                ((ccp_current < 0.) * (current_pnl < -stop_profit))
        hold_period_condition = (((time_data[-1, 0] - self.trade_time) / 86400) > max_hold_days) & \
                                (ccp_current != 0.)
        mandatory_condition = stop_loss_condition|stop_profit_condition|hold_period_condition
        close_condition = mandatory_condition|close_long_condition|close_short_condition

        #### update target position ####
        ccp_target = self.portfolio_manager.current_holding.copy()
        ccp_target[ccp_target > 0] = 1
        ccp_target[ccp_target < 0] = -1

        if prev_kd_ready:
            ccp_target[open_long_condition] = 1
            ccp_target[open_short_condition] = -1
            ccp_target[close_condition] = 0

        #### record trade has happened ####
        has_trade = (ccp_target != ccp_current)
        self.trade_time[has_trade] = time_data[-1][0]
        self.trade_price[has_trade] = close_price[has_trade]

        ref_aum = self.portfolio_manager.init_cash
        weight = ccp_target / n_security
        order = self.return_reserve_target_base_ccy(weight * ref_aum)
        return order.reshape(1,-1)

##===========================================================================##
@defaultjitclass({
    'cci': cci_type,
    'trade_time': nb.int64[:],
    'trade_price': nb.float64[:],
    'warn_long': nb.boolean[:],
    'warn_short': nb.boolean[:]
})
class CCI_strategy(BaseStrategy):
    # learn from Jiaqi's Bollinger bands
    # warn long:  无仓 and 上穿 - break_threshold
    # open long:  warn long and 上穿 0
    # close long:  下穿 0
    # warn short:  无仓 and 下穿 break_threshold
    # open short:  warn short and 下穿 0
    # close short:  上穿 0

    def init(self):
        n_security = self.ccy_matrix.shape[1]
        length = np.int64(self.strategy_param[0])
        self.cci = CCI(length, n_security)
        self.trade_time = np.zeros(n_security, dtype=np.int64)
        self.trade_price = np.zeros(n_security)
        self.warn_long = (np.zeros(n_security)!=np.zeros(n_security))
        self.warn_short = (np.zeros(n_security)!=np.zeros(n_security))

    def save_status(self):
        '''可以把需要的status存进strategy_status
           不存任何状态会存一个空的'''
        strategy_status = Dict.empty(types.unicode_type, nb.float64)
        strategy_status['cci'] = self.cci.value[0]
        self.status = strategy_status

    def load_status(self, pickle_dict):
        strategy_status = pickle_dict

    def on_data(self, data_dict: dict, time_dict: dict, ref_data_dict: dict, ref_time_dict: dict):
        window_data = data_dict['main']
        time_data = time_dict['main'][:,:]

        current_data = window_data[-1,:,3]

        # get strategy param
        break_threshold = np.float64(self.strategy_param[1])
        stop_profit = np.float64(self.strategy_param[2])
        stop_loss = np.float64(self.strategy_param[3])
        max_hold_days = np.int64(self.strategy_param[4])

        # get current price
        open_price = window_data[-1, :, 0]
        high_price = window_data[-1, :, 1]
        low_price = window_data[-1, :, 2]
        close_price = window_data[-1, :, 3]

        n_security = self.ccy_matrix.shape[1]

        # update indicator
        prev_cci_value = np.copy(self.cci.value)
        prev_cci_ready = np.array([self.cci.ready])[0]
        self.cci.on_data(high_price, low_price, close_price)
        curr_cci_value = np.copy(self.cci.value)

        ccp_current = self.portfolio_manager.current_holding.copy()
        ccp_current[ccp_current > 0] = 1
        ccp_current[ccp_current < 0] = -1

        # signal_condition
        up_cross_lower  = (prev_cci_value <= -break_threshold) & \
                          (curr_cci_value > -break_threshold)
        up_cross_middle = (prev_cci_value <= 0) & \
                          (curr_cci_value > 0)
        up_cross_upper  = (prev_cci_value <= break_threshold) & \
                          (curr_cci_value > break_threshold)
        down_cross_lower = (prev_cci_value >= -break_threshold) & \
                           (curr_cci_value < -break_threshold)
        down_cross_middle = (prev_cci_value >= -0) & \
                            (curr_cci_value < -0)
        down_cross_upper  = (prev_cci_value >= break_threshold) & \
                            (curr_cci_value < break_threshold)

        self.warn_long = ((self.warn_long == 0) & up_cross_lower & \
                          (ccp_current==0))| self.warn_long

        self.warn_short = ((self.warn_short == 0) & down_cross_upper & \
                           (ccp_current==0))| self.warn_short

        #### position ####

        open_long_condition = self.warn_long & up_cross_middle & \
                              (ccp_current == 0)
        self.warn_long = self.warn_long & (open_long_condition==0)

        open_short_condition = self.warn_short & down_cross_middle & \
                              (ccp_current == 0)
        self.warn_short = self.warn_short & (open_short_condition==0)

        close_long_condition = (ccp_current > 0.) * down_cross_middle
        close_short_condition = (ccp_current < 0.) * up_cross_middle

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

        if prev_cci_ready:
            ccp_target[open_long_condition] = 1
            ccp_target[open_short_condition] = -1
            ccp_target[close_condition] = 0

        #### record trade has happened ####
        has_trade = (ccp_target != ccp_current)
        self.trade_time[has_trade] = time_data[-1][0]
        self.trade_price[has_trade] = close_price[has_trade]

        ref_aum = self.portfolio_manager.init_cash
        weight = ccp_target / n_security
        order = self.return_reserve_target_base_ccy(weight * ref_aum)
        return order.reshape(1,-1)