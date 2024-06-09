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
    def init(self):
        self.symbol_batch = self.metadata["main"]["symbols"]

    def on_data(self, data_dict: dict, time_dict: dict, ref_data_dict: dict, ref_time_dict: dict):
        # Reference data correctness checkpoints
        remark = Dict.empty(types.unicode_type, nb.float64)
        current_time = time_dict['main'][-1, 0]
        eps = 1e-5

        # 1536739200 2018-09-12 08:00:00
        # ref_daily_info null (csv starts from 9-11-09 but the config cut the data before 9-12)
        # ref hourly 9-12-06 07 08

        # 1536998400 2018年9月15日SaturdayAM8点00分
        # ref_daily_info 9-12-09 9-13-09 9-14-09
        # ref_hourly 9-14-06 07 08

        # 1537084800 16日 之后也是一天天jump


        # data_dict["int"], data_dict["fxrate"], data_dict["main"]
        if current_time == 1536739200:     # 2018-09-12 08:00:00, first time point in the data frame
            
            if ref_data_dict['ref_daily_info'].shape[0] == 3 and \
                ref_data_dict['ref_daily_info'].shape[1] == 3 and \
                ref_data_dict['ref_daily_info'].shape[2] == 4 and \
                ref_time_dict['ref_daily_info'].shape[0] == 3 and \
                ref_data_dict['ref_hourly'].shape[0] == 3 and \
                ref_data_dict['ref_hourly'].shape[1] == 5 and \
                ref_data_dict['ref_hourly'].shape[2] == 4 and \
                ref_time_dict['ref_hourly'].shape[0] == 3:
                # tesing the ref_daily_info and ref_hourly size
                remark['window_size_correct'] = 1
            else:
                remark['window_size_correct'] = 0
            
            if ref_time_dict['ref_daily_info'][-1, 5] == 11 and \
                ref_time_dict['ref_daily_info'][-2, 5] == 10 and \
                ref_time_dict['ref_daily_info'][-3, 5] == 9:
                remark["ref_daily_in_window_correct"] = 1
            else:
                remark["ref_daily_in_window_correct"] = 0

            if ref_time_dict['ref_hourly'][-1, 5] == 12 and \
                ref_time_dict['ref_hourly'][-1, 6] == 8 and \
                ref_time_dict['ref_hourly'][-2, 6] == 7 and \
                ref_time_dict['ref_hourly'][-3, 6] == 6:
                remark["ref_hourly_in_window_correct"] = 1
            else:
                remark["ref_hourly_in_window_correct"] = 0
            

        elif current_time == 1536912000:       # 2018-09-14 08:00:00 UTC
            # daily_info STOCK3 2018-09-13 09:00:00 UTC,5.09,5.17,5.08,5.17
            # daily_info STOCK5 2018-09-13 09:00:00 UTC,27.2,27.95,26.64,27.45
            if abs(ref_data_dict['ref_daily_info'][-1,1,2] - 5.08) < eps and \
                abs(ref_data_dict['ref_daily_info'][-1,2,1] - 27.95) < eps:
                remark["daily_data_in_window_correct"] = 1
            else:
                remark["daily_data_in_window_correct"] = 1
            
            # hourly STOCK1 2018-09-14 08:00:00+00:00,0.935,0.9352,0.9338,0.9339
            # hourly STOCK4 2018-09-14 08:00:00+00:00,111.855,112.049,111.782,112.046
            if abs(ref_data_dict['ref_hourly'][-1,0,3] - 0.9339) < eps and \
                abs(ref_data_dict['ref_hourly'][-1,3,1] - 112.049) < eps:
                remark["daily_data_in_window_correct"] = 1
            else:
                remark["daily_data_in_window_correct"] = 0

        self.save_signal_remark(remark)

        pv = self.portfolio_manager.pv
        target = np.ones(len(self.symbol_batch)) * pv / len(self.symbol_batch)
        return self.return_reserve_target_base_ccy(target).reshape(1, -1)

    def load_status(self, pickle_dict):
        strategy_status = pickle_dict
        self.status = strategy_status

    def save_status(self):
        '''可以把需要的status存进strategy_status
           不存任何状态会存一个空的'''
        strategy_status = Dict.empty(types.unicode_type, nb.float64)
        self.status = strategy_status
