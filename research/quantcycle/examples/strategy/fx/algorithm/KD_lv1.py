import numpy as np
import numba as nb
from numba.typed import Dict, List
from numba import types
from numba.experimental import jitclass
from quantcycle.utils.indicator import KD
from quantcycle.app.signal_generator.base_strategy import defaultjitclass, BaseStrategy
from quantcycle.app.pms_manager.portfolio_manager import PorfolioManager

try:
    pms_type = PorfolioManager.class_type.instance_type
    kd_type = KD.class_type.instance_type
except:
    pms_type = "PMS_type"
    kd_type = "KD_type"



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
        window_data = data_dict["main"]
        time_data = time_dict["main"][:,:]

        break_threshold = np.float64(self.strategy_param[3])
        stop_profit = np.float64(self.strategy_param[4])
        stop_loss = np.float64(self.strategy_param[5])
        max_hold_days = np.int64(self.strategy_param[6])

        high_price = window_data[-1, :, 1]
        low_price = window_data[-1, :, 2]
        close_price = window_data[-1, :, 3]
        n_security = self.ccy_matrix.shape[1]

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

        weight = ccp_target / n_security
        ref_aum = self.portfolio_manager.init_cash
        target = weight * ref_aum

        return self.return_reserve_target_base_ccy(target).reshape(1,-1)
