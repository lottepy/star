import numpy as np
import numba as nb
from numba.typed import Dict, List
from numba import types
from numba.experimental import jitclass
from quantcycle.utils.indicator import RSI, Moving_series, moving_series_type
from quantcycle.app.signal_generator.base_strategy import defaultjitclass, BaseStrategy
from quantcycle.app.pms_manager.portfolio_manager import PorfolioManager

try:
    pms_type = PorfolioManager.class_type.instance_type
    rsi_type = RSI.class_type.instance_type
except:
    pms_type = "PMS_type"
    rsi_type = "RSI_type"
##===========================================================================##


@defaultjitclass({
    'portfolio_manager': pms_type,
    'ccy_matrix': nb.float64[:, :],
    'strategy_name': types.unicode_type,
    'strategy_param': nb.float64[:],
    'symbol_batch': types.ListType(types.unicode_type),
    'secondary_symbol_batch': types.DictType(types.unicode_type, types.ListType(types.ListType(types.unicode_type))),
    'ccy_list': types.ListType(types.unicode_type),
    'symbol_type': types.ListType(types.unicode_type),
    'status': types.DictType(types.unicode_type, nb.float64),
    'current_fx_data': nb.float64[:],
    'current_data': nb.float64[:],
    'is_tradable': nb.boolean[:],
    'rsi': rsi_type,
    'trade_time': nb.int64[:],
    'trade_price': nb.float64[:],
    'signal_remark': types.DictType(nb.int64, types.DictType(types.unicode_type, nb.float64)),
    'timestamp': nb.int64
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
        self.status = strategy_status

    def on_data(self, data_dict: dict, time_dict: dict, ref_data_dict: dict, ref_time_dict: dict):
        # Remark checkpoints begin
        remark = Dict.empty(types.unicode_type, nb.float64)
        current_time = time_dict['main'][-1, 0]
        #eps = 1e-5

        # backtest
        if current_time == 1515974400 + 6*60*60+57*60:      # 2018-01-15 00:00:00, reference data empty
            if len(data_dict) == 3 and len(time_dict) == 3 and len(ref_data_dict) == 0 and len(ref_time_dict) == 0:
                pass
            else:
                return
            if data_dict['main'].shape == (2, 8, 4) and time_dict['main'].shape == (2, 9): #and \
                #data_dict['fxrate'].shape == (1, 8, 1) and time_dict['fxrate'].shape == (1, 9) and \
                #data_dict['int'].shape == (1, 8, 1) and time_dict['int'].shape == (1, 9):
                pass
            else:
                return
            remark = {"field_1":1,"field_2":1,"field_3":1,"field_4":1,"field_5":1}

        elif current_time == 1536883200 + 6*60*60+57*60:    # 2018-09-14 00:00:00, all data should be available
            if len(data_dict) == 3 and len(time_dict) == 3 and len(ref_data_dict) == 2 and len(ref_time_dict) == 2:
                pass
            else:
                return
            if data_dict['main'].shape == (2, 8, 4) and time_dict['main'].shape == (2, 9) and \
                """ data_dict['fxrate'].shape == (1, 8, 1) and time_dict['fxrate'].shape == (1, 9) """ and \
                """ data_dict['int'].shape == (1, 8, 1) and time_dict['int'].shape == (1, 9) """ and \
                ref_data_dict['ref_daily'].shape == (14, 5, 4) and ref_time_dict['ref_daily'].shape == (14, 9) and \
                ref_data_dict['ref_hourly'].shape == (14, 3, 1) and ref_time_dict['ref_hourly'].shape == (14, 9):
                pass
            else:
                return
            remark = {"field_1":1,"field_2":1,"field_3":1,"field_4":1,"field_5":1}

        elif current_time == 1537142400 + 6*60*60+57*60:
            #testing static parameters in the strategy
            if self.strategy_name == 'RSI_strategy' and \
                sum(self.strategy_param == [10, 5, 0.01, 0.005, 10]) == 5 and \
                'main' in self.metadata.keys() and \
                sum(self.metadata['main']['symbols'] == ['000005.SZ', '000100.SZ', '000917.SZ', '002415.SZ', '600000.SH', '600016.SH', '600028.SH', '601398.SH']) == 8 and \
                len(self.metadata['main']['symbol_types']) == 8 and self.metadata['main']['symbol_types'][3] == 'CN_STOCK' and \
                self.metadata['main']['fields'] == ['open', 'high', 'low', 'close'] and \
                len(self.metadata['main']['ccy_list']) == 8 and self.metadata['main']['ccy_list'][2] == 'CNY' and \
                'ref_daily' in self.metadata.keys() and \
                self.metadata['ref_daily']['symbols'] ==  ['STOCK_1', 'STOCK_2', 'STOCK_3', 'STOCK_4', 'STOCK_5'] and \
                self.metadata['ref_daily']['fields'] ==  ['open', 'high', 'low', 'close'] and \
                'ref_hourly' in self.metadata.keys() and \
                self.metadata['ref_hourly']['symbols'] ==  ['STOCK_2', 'STOCK_3', 'STOCK_5'] and \
                self.metadata['ref_hourly']['fields'] ==  ['close'] and \
                'remark' not in self.metadata.keys() and \
                '0' not in self.metadata.keys() and 0 not in self.metadata.keys() and \
                sum(self.ccy_matrix[0]) == 8:
                pass
            else:
                return
            remark = {"field_1":1,"field_2":1,"field_3":1,"field_4":1,"field_5":1}


        # on live
        elif current_time == 1547189820:
            if len(data_dict) == 3 and len(time_dict) == 3 and len(ref_data_dict) == 2 and len(ref_time_dict) == 2:
                pass
            else:
                return
            
            if data_dict['main'].shape == (2, 8, 4) and time_dict['main'].shape == (2, 9):
                pass
            else:
                return
            remark = {"field_1":1,"field_2":1,"field_3":1,"field_4":1,"field_5":1}


        elif current_time == 1547189820 + 3*24*60*60:
            remark = {"field_1":1,"field_2":1,"field_3":1,"field_4":1,"field_5":1}

        self.save_signal_remark(remark)

        # RSI strategy begins
        window_data = data_dict['main']
        time_data = time_dict['main']

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

        weight = ccp_target / n_security
        ref_aum = self.portfolio_manager.init_cash
        target = weight * ref_aum
        return self.return_reserve_target_base_ccy(target).reshape(1, -1)
