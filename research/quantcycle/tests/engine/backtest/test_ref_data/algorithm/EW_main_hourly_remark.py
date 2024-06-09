import numpy as np
import numba as nb
from numba.typed import Dict, List
from numba import types
from numba.experimental import jitclass
from quantcycle.app.signal_generator.base_strategy import defaultjitclass, BaseStrategy
from quantcycle.app.pms_manager.portfolio_manager import PorfolioManager

try:
    pms_type = PorfolioManager.class_type.instance_type
except:
    pms_type = "PMS_type"


@defaultjitclass()
class EW_strategy(BaseStrategy):

    def on_data(self, data_dict: dict, time_dict: dict, ref_data_dict: dict, ref_time_dict: dict):
        # Reference data correctness checkpoints
        remark = Dict.empty(types.unicode_type, nb.float64)
        current_time = time_dict['main'][-1, 0]
        eps = 1e-5
        if current_time == 1536735600:      # 2018-09-12 07:00:00, the hour before day end
            if ref_data_dict['ref_daily'].shape[0] == 3 and ref_time_dict['ref_daily'].shape[0] == 3 and \
                    ref_data_dict['ref_daily_open'].shape[0] == 3 and ref_time_dict['ref_daily_open'].shape[0] == 3:
                remark['window_size_correct'] = 1
            else:
                remark['window_size_correct'] = 0
            if ref_time_dict['ref_daily'][-1, 0] == 1536652800 and \
               abs(ref_data_dict['ref_daily'][-1, 3, 3] - 12.22) < eps:
                remark['trading_hour_no_day_end_data'] = 1
            else:
                remark['trading_hour_no_day_end_data'] = 0
            if ref_time_dict['ref_daily_open'][-1, 0] == 1536710400 and \
               abs(ref_data_dict['ref_daily_open'][-1, 0, 0] - 2.7) < eps and \
               ref_data_dict['ref_daily_open'].shape[2] == 1 and \
               abs(ref_data_dict['ref_daily_open'][0, 2, 0] - 29.72) < eps:
                remark['first_day_daily_open_data_correct'] = 1
            else:
                remark['first_day_daily_open_data_correct'] = 0
        elif current_time == 1536739200:    # 2018-09-12 08:00:00, day end
            if abs(ref_data_dict['ref_daily'][2, 4, 3] - 26.69) < eps and \
               abs(ref_data_dict['ref_daily'][2, 0, 3] - 2.96) < eps:
                remark['first_entry_data_correct'] = 1
            else:
                remark['first_entry_data_correct'] = 0
        elif current_time == 1536822000:    # 2018-09-13 07:00:00, the hour before day end on the 2nd day
            if ref_time_dict['ref_daily'][-1, 0] == 1536739200 and \
               abs(ref_data_dict['ref_daily'][-1, 3, 0] - 12.22) < eps:
                remark['trading_hour_no_day_end_data_2nd_day'] = 1
            else:
                remark['trading_hour_no_day_end_data_2nd_day'] = 0
        elif current_time == 1537232400:    # 2018-09-18 01:00:00
            if ref_time_dict['ref_daily'][-1, 5] == 17 and ref_time_dict['ref_daily'][-2, 5] == 16 and \
               ref_time_dict['ref_daily'][-3, 5] == 15 and ref_time_dict['ref_daily_open'][-1, 5] == 18 and \
               ref_time_dict['ref_daily_open'][-2, 5] == 17 and ref_time_dict['ref_daily_open'][-3, 5] == 16:
                remark['dates_in_window_correct'] = 1
            else:
                remark['dates_in_window_correct'] = 0
            if abs(ref_data_dict['ref_daily'][-1, 4, 3] - 27.69) < eps and \
               abs(ref_data_dict['ref_daily'][-1, 0, 3] - 2.94) < eps and \
               abs(ref_data_dict['ref_daily'][-2, 4, 3] - 27.76) < eps and \
               abs(ref_data_dict['ref_daily'][-3, 4, 3] - 27.76) < eps and \
               abs(ref_data_dict['ref_daily'][-3, 0, 3] - 2.96) < eps:
                remark['daily_data_in_window_correct'] = 1
            else:
                remark['daily_data_in_window_correct'] = 0
            if abs(ref_data_dict['ref_daily_open'][-1, 0, 0] - 2.66) < eps and \
               abs(ref_data_dict['ref_daily_open'][-3, 0, 0] - 2.74) < eps and \
               abs(ref_data_dict['ref_daily_open'][-1, 1, 0] - 5.0) < eps and \
               abs(ref_data_dict['ref_daily_open'][1, 2, 0] - 27.6) < eps:
                remark['daily_open_data_in_window_correct'] = 1
            else:
                remark['daily_open_data_in_window_correct'] = 0

        self.save_signal_remark(remark)

        pv = self.portfolio_manager.pv
        symbol_batch = self.metadata["main"]["symbols"]
        target = np.ones(len(symbol_batch)) * pv / len(symbol_batch)
        return self.return_reserve_target_base_ccy(target).reshape(1, -1)

    def load_status(self, pickle_dict):
        strategy_status = pickle_dict
        self.status = strategy_status

    def save_status(self):
        '''可以把需要的status存进strategy_status
           不存任何状态会存一个空的'''
        strategy_status = Dict.empty(types.unicode_type, nb.float64)
        self.status = strategy_status
