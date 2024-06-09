import os
from datetime import datetime

import numpy as np
import pandas as pd

from metrics_calculator.core import run_once
from metrics_calculator.core.data_reader import (read_basic_data,
                                                 read_benchmark_data,
                                                 read_data)
from metrics_calculator.core.sampler import sample, timestamp_to_str
from metrics_calculator.utils.hdf5_connector import \
    HDF5Connector


def metrics_calculator(path, flatten=True, stock_like=False, aum_adjust=False, allocation_freq='M', lookback_points_list=[60, 252], addition=True, multiplier=252, memory_usage_GigaBytes=4, result_freq='D', day_offset=0, benchmark=None):
    '''
    input:  lv1结果, metrics计算需要的参数
            path: 回测产生的hdf5文件夹路径 'dir_name'
            flatten: 是否把symbol展开
            stock_like: whether the porfolio should be treated as stock portfolio. A major difference is that stock portfolio has an inital portfolio value and return are productive rather than addictive
            aum_adjust: whether an adjustment should be applied to inital portfolio value / reference aum. 
                If False, initial porfolio will be divided by number of stocks when being flattened.
                To clarify, adjustments are made when aum is no longer for whole portfolio (in our case, the recorded aum is actually for each symbol).
            allocation_freq: 'M' 每月底采样
            lookback_points_list: list. 每次采样回看的点数
            addition: True -> return 按照单利计算
            multiplier: 1年有多少个点. daily -> 252.
    output: len(lookback_points_list)个csv文件, 放在path路径下
    '''
    # id_list, universe_list, params_list, params_keys, rtn, direction, commission, timestamp, result_uuid = read_data(
    #     path, flatten, stock_like, aum_adjust)
    id_list,universe_list,params_list,params_keys,timestamp,timestamp_ix,result_uuid,ref_aum = read_basic_data(path,flatten,set_uuid=True,aum_adjust=aum_adjust,freq=result_freq)
    benchmark_return = read_benchmark_data(timestamp,benchmark=benchmark)
    if timestamp_ix[-1]==len(timestamp_ix)-1:
        timestamp_ix_filter = None
    else:
        timestamp_ix_filter = timestamp_ix
    # rtn = rtn.astype(np.float64)
    # direction = direction.astype(np.int64)
    
    columns = ['holding_period_return', 'sharpe_ratio', 'MDD', 'ADD', 'rate_of_return',
                    'number_of_trades', 'return_from_longs', 'return_from_shorts', 'ratio_of_longs',
                    'hit_ratio', 'profit_rate','return_from_hits', 'return_from_misses','return_from_profit_days','return_from_loss_days']
    metrics_chunk_size = (1, np.floor_divide(2**20/8/len(columns), 100)*100, len(columns))
    rtn_chunk_size = int(np.floor_divide(memory_usage_GigaBytes*2**30 / 2 / len(timestamp)/8, metrics_chunk_size[1]) * metrics_chunk_size[1])
    n_strategy = len(id_list)
    split_points = np.arange(1, n_strategy // rtn_chunk_size) * rtn_chunk_size
    # rtn_list = np.split(rtn,split_points)
    # direction_list = np.split(direction,split_points)
    id_split_list = np.split(id_list,split_points)
    
    hdf5_filename = os.path.join(path,'metrics.hdf5')
    h5 = HDF5Connector(hdf5_filename, commpression='lzf')
    # if result_uuid is not None and metrics_uuid matches with result_uuid, then metrics.hdf5 will not be cleaned, else
    # always remove metrics.hdf5
    h5.init(result_uuid)
    h5.create_structure(n_strategy,len(timestamp))
    
    h5.insert_basic_info(id_list, universe_list, params_list, params_keys, timestamp)
    h5.transfer_data(path = path, flatten = flatten, stock_like=stock_like, ref_aum=ref_aum, timestamp_ix_filter=timestamp_ix_filter)
    
    sample_ix_list = sample(timestamp, allocation_freq, lookback_points_list, day_offset=day_offset)
    sample_ix_array_list = [np.array(i, dtype=np.int64)
                            for i in sample_ix_list]

    h5.create_metrics_dataset(columns,allocation_freq,lookback_points_list,sample_ix_array_list,id_list,metrics_chunk_size,benchmark)

    for j in range(len(id_split_list)):
        f = h5.connect()
        rtn = f['data/rtn'][id_split_list[j][0]:id_split_list[j][-1]+1,:] - benchmark_return
        direction = f['data/direction'][id_split_list[j][0]:id_split_list[j][-1]+1,:]
        f = h5.disconnect()
        data = run_once(rtn, direction,addition,multiplier,False,sample_ix_array_list[0])
        h5.insert_metrics(data,id_list=id_split_list[j],location='whole_period')
        del data
        for i in range(len(lookback_points_list)):
            data = run_once(rtn, direction,addition,multiplier,True,sample_ix_array_list[i])
            h5.insert_metrics(data,id_list=id_split_list[j],location=f'{allocation_freq}_{lookback_points_list[i]}')
            del data
        
    return 0