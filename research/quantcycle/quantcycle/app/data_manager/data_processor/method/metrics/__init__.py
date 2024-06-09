from ..method_base import MethodBase
from ....utils.timestamp_manipulation import *
import pandas as pd
import numpy as np
from datetime import datetime,timedelta 
from metrics_calculator_core import run_once as run_cal_metrics
from pandas.tseries.offsets import YearBegin, YearEnd


class MethodMETRICS(MethodBase):
    ''' To evaluate the metrics of strategies '''
    def __init__(self):
        super().__init__()
        self.is_final_step = True
        self.output_data_type = 'np_numeric'
        self.directions = []

    def create_data_mapping(self, data_bundle):
        self.name = data_bundle["Label"].split('-')[0]
        self.symbols = data_bundle["Symbol"]
        self.attrs = ['pnl', 'position', 'ref_aum']
        self.allocation_freq = data_bundle.get("ActionsArgs").get("allocation_freq")
        self.lookback_points_list = data_bundle.get("ActionsArgs").get("lookback_points_list")
        self.addition = data_bundle.get("ActionsArgs").get("addition")
        self.multiplier = data_bundle.get("ActionsArgs").get("multiplier")

        self.data_mapping.update(
            {' '.join([self.name,attr]): self.name+"/"+attr for attr in self.attrs})

    def run(self):
        rets, directions, idx_list = self._resample_data()

        if idx_list.size == 0:
            metrics = np.empty((0,0,0))
            time_arr = np.empty((0,0,0))

            list_symbols = []
            list_fields = []
        else:
            metrics = self._cal_metrics(rets, directions, idx_list)
            _time_array = self.data_dict[f'{self.name} pnl'][1]
            lst_timestamp = [int(_time_array[one[1]]) for one in idx_list]
            time_arr = list2timearr(lst_timestamp)
            if str(self.symbols) == 'ALL':
                list_symbols = self.data_dict[f'{self.name} pnl'][2]
            else:
                list_symbols = self.symbols
            list_fields = ['holding_period_return', 'sharpe_ratio', 'MDD', 'ADD', 'rate_of_return',
                        'number_of_trades', 'return_from_longs', 'return_from_shorts', 'ratio_of_longs',
                        'hit_ratio', 'profit_rate','return_from_hits', 'return_from_misses','return_from_profit_days','return_from_loss_days']

        return {'data_arr': metrics, 'time_arr': time_arr, 'symbol_arr': list_symbols, 'fields_arr': list_fields}

    def _cal_metrics(self, rets, directions, idx_list):
        # TODO reshape need recheck
        array = run_cal_metrics(rets, directions, self.addition, np.int64(
            self.multiplier), True, idx_list)
        array = array.reshape(len(idx_list), -1, 15)
        np.swapaxes(array, 0, 1)
        return array

    def _resample_data(self):

        directions = []

        pnl_tuple = self.data_dict[f'{self.name} pnl']
        pos_tuple = self.data_dict[f'{self.name} position']
        ref_aum = self.data_dict[f'{self.name} ref_aum'].values[0,0]
        pnl_data_arr = pnl_tuple[0]
        pos_data_arr = pos_tuple[0]
        id_list = pnl_tuple[2]
        timestamp_list = pnl_tuple[1]
        if timestamp_list.size == 0:
            return np.empty((0)), np.empty((0)), np.empty((0)) 

        idx_list = np.array(sampling(timestamp_list, self.allocation_freq, self.lookback_points_list, day_offset = 0))
        if idx_list.size == 0:

            return np.empty((0)), np.empty((0)), np.empty((0))      # raise ValueError(f'Not_enough_data_to_cal_metrics')


        rets = ((np.diff(pnl_data_arr, axis=0))/ref_aum)
        rets = rets.reshape(rets.shape[0],rets.shape[1])
        zeros = np.zeros((1, rets.shape[1]))
        rets = np.vstack((zeros, rets))   
        rets = rets.T
 

        # TODO optimize speed, may need to restructure _direction
        if len(self.directions)==0:
            for j in range(id_list.size):
                direction = [_direction(pos_data_arr[i,j]) for i in range(len(timestamp_list))]
                directions.append(direction)
            self.directions = directions
        directions = self.directions

        return rets.astype('float64'), np.array(directions, dtype='int64'), idx_list[0].astype('int64')

def _direction(position_array):
    ''' 
        position_array 1d
    '''
    if len(position_array) != 1:
        # 有很多个underlying的情况
        position = sum(abs(position_array))
    else:
        # 只有一个underlying的情况
        position = position_array[0]
        
    if position > 1e-5:
        return 1
    elif position< -1e-5:
        return -1
    else:
        return 0

def sampling(timestamp_list, allocation_freq, lookback_points_list, day_offset = 0):
    sample_list = _utc_date_range(
        freq=allocation_freq, start_timestamp=timestamp_list[0], end_timestamp=timestamp_list[-1], day_offset=day_offset)
    time_list = _timestamp_to_utc_datetime(timestamp_list)
    ix_list = [_find_offset(i, time_list) for i in sample_list]
    sample_ix_list = [[[sample_ix-lookback_points, sample_ix]
                       for sample_ix in ix_list if sample_ix-lookback_points >= 0] for lookback_points in lookback_points_list]
    return sample_ix_list

def _utc_date_range(freq, start_timestamp, end_timestamp, day_offset=0):
    start = _timestamp_to_utc_datetime(start_timestamp)
    _start = start-YearBegin()-timedelta(days=40)
    end = _timestamp_to_utc_datetime(end_timestamp)
    _end = end+YearEnd()
    output = pd.date_range(start=_start, end=_end, freq=freq)
    if day_offset > 0:
        output = [i+timedelta(days=day_offset) for i in output]
    output = [i for i in output if i >= start and i <= end]
    return output

def _timestamp_to_utc_datetime(timestamp: int or list):
    a = pd.to_datetime(timestamp, unit='s')
    return a

def _find_offset(aValue, aList):
    # 如果要找的数字不在里面, 则会返回最后一个比它小的数字
    offset = next(x[0] for x in enumerate(aList) if x[1] >= aValue)
    if aList[offset] > aValue:
        offset -= 1
    return offset if offset >= 0 else 0 # in case offset == -1