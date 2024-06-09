import numpy as np
import numba as nb
from numba.typed import Dict, List
from numba import types
from numba.experimental import jitclass
from quantcycle.utils.indicator import RSI,Moving_series, moving_series_type
from quantcycle.app.signal_generator.base_strategy import defaultjitclass, BaseStrategy
from quantcycle.app.pms_manager.portfolio_manager import PorfolioManager

try:
    pms_type = PorfolioManager.class_type.instance_type
    rsi_type = RSI.class_type.instance_type
except:
    pms_type = "PMS_type"
    rsi_type = "RSI_type"
##===========================================================================##


# @defaultjitclass({
#     'portfolio_manager': pms_type,
#     'ccy_matrix': nb.float64[:, :],
#     'strategy_name': types.unicode_type,
#     'strategy_param': nb.float64[:],
#     'symbol_batch': types.ListType(types.unicode_type),
#     'ccy_list': types.ListType(types.unicode_type),
#     'symbol_type': types.ListType(types.unicode_type),
#     'status':types.DictType(types.unicode_type, nb.float64),
#     'current_fx_data': nb.float64[:],
#     'current_data': nb.float64[:],
#     'is_tradable': nb.boolean[:],
#     'rsi':rsi_type,
#     'trade_time': nb.int64[:],
#     'trade_price': nb.float64[:]
# })
class RSI_strategy(BaseStrategy):
    # class RSI_strategy():
    # open long:   rsi(t-1)< 50 - break_threshold, rsi(t)>50 - break_threshold
    # open short:  rsi(t-1)> 50 + break_threshold, rsi(t)<50 + break_threshold
    # close long:   rsi(t-1)< 50 + break_threshold, rsi(t)>50 + break_threshold
    # close short:  rsi(t-1)> 50 - break_threshold, rsi(t)<50 - break_threshold

    def init(self):
        n_security = self.ccy_matrix.shape[1]
        length = np.int64(self.strategy_param[0])
        self.rsi = RSI(length, n_security)
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
        time_data = time_dict['main'][-1,:]


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
        current_price_level[self.trade_price != 0] = np.divide(close_price[self.trade_price != 0], self.trade_price[self.trade_price != 0])
        current_pnl = current_price_level - 1
        stop_loss_condition = ((ccp_current > 0.) * (current_pnl < -stop_loss)) | \
                              ((ccp_current < 0.) * (current_pnl > stop_loss))
        stop_profit_condition = ((ccp_current > 0.) * (current_pnl > stop_profit)) | \
                                ((ccp_current < 0.) * (current_pnl < -stop_profit))
        hold_period_condition = (((time_data[0] - self.trade_time) / 86400) > max_hold_days) & \
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
        self.trade_time[has_trade] = time_data[0]
        self.trade_price[has_trade] = close_price[has_trade]

        weight = ccp_target / n_security

        ref_aum = self.portfolio_manager.init_cash
        target = weight * ref_aum
        return self.return_reserve_target_base_ccy(target).reshape(1,-1)

