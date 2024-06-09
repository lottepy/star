import os

import numpy as np
import pandas as pd
from backtest_engine.utils.result_processer import ResultProcesser
import uuid
import h5py
from metrics_calculator.utils.resample import timestamp_resample
import pathlib

def read_basic_data(dir_path, flatten=True, set_uuid=True, aum_adjust=False, freq='D'):
    '''
        input:
            dir_path: result.hdf5 save path
            flatten: whether the symbols are independent. If true, all symbols are regarded as independent, 
                so the pnl matrix are flattened.
            set_uuid: if True, a uuid will be append to the original hdf5 file's root group's attrs which is used 
                for identification.
        output:
            id_array
            universe_array
            parameters
            timestamp
            result_uuid
    '''
    with h5py.File(os.path.join(dir_path,'result.hdf5'),'r') as f:
        result_uuid = f.attrs.get('uuid', None)
        
    if result_uuid is None and set_uuid:
        with h5py.File(os.path.join(dir_path,'result.hdf5'),'a') as f:
            result_uuid = str(uuid.uuid1())
            f.attrs['uuid'] = result_uuid

    rp = ResultProcesser(dir_path)
    summary_df = rp.get_summary() # order by 1,2,3,4 as int
        
    # get attr
    attrs = rp.get_attrs('virtual_0')
    
    # First get universe list
    universe_array = np.array(summary_df.loc[:,'universe'].values.tolist())
    output_universe_array = universe_array if not flatten else universe_array.reshape((-1,1))
    
    n_security = universe_array.shape[1]
    ref_aum = attrs['cash']/n_security if flatten and not aum_adjust else attrs['cash']
    # for equity strategy, ref_aum is the inital cash deposit

    # Second generate strategy_id
    output_id_array = np.array(list(range(len(output_universe_array))))
    
    # Then get params_list
    params_keys = attrs['param_keys']
    tmp = summary_df.loc[:,params_keys].values
    # raw_params = np.array([dict(zip(params_keys,i)) for i in tmp])
    raw_params = tmp
    if not flatten:
        output_params_array = raw_params
    else:
        # output_params_array = np.array([raw_params] * n_security).reshape(-1,order='F')
        output_params_array = np.repeat(raw_params,repeats=n_security,axis=0)
    
    del rp
    with h5py.File(os.path.join(dir_path,'result.hdf5'),'r') as f:
        timestamp = f['virtual_0']['timestamp_list'][()]
    resampled_timestamp, timestamp_ix = timestamp_resample(timestamp,freq)
    
    return output_id_array,output_universe_array,output_params_array,params_keys,resampled_timestamp,timestamp_ix,result_uuid,ref_aum
        
    
def read_data(dir_path, flatten=True, stock_like=False, aum_adjust=False):
    '''
        input:
            dir_path: result.hdf5 save path
            flatten: whether the symbols are independent. If true, all symbols are regarded as independent, 
                so the pnl matrix are flattened.
            stock_like: whether the porfolio should be treated as stock portfolio. A major difference is that stock portfolio has an inital portfolio value and return are productive rather than addictive
            aum_adjust: whether an adjustment should be applied to inital portfolio value / reference aum. 
                If False, initial porfolio will be divided by number of stocks when being flattened.
                To clarify, adjustments are made when aum is no longer for whole portfolio (in our case, the recorded aum is actually for each symbol).
             
        output:
            rtn
            direction
            commission
    '''
    rp = ResultProcesser(dir_path)
    # rtn
    if not flatten:
        pnl_df = rp.get_pv()
    else:
        pnl_df = rp.get_separate('pnl')
    pnl_df.drop('datetime',axis=1,inplace=True)
    pnl = pnl_df.values.T    
    pnl_extend = np.hstack((np.zeros((pnl.shape[0],1)),pnl))
    
    if stock_like:
        pnl_extend = pnl_extend + ref_aum
        rtn = (pnl_extend[:,1:]/pnl_extend[:,:-1]-1)
    else:
        rtn = ((pnl_extend[:,1:] - pnl_extend[:,:-1])/ref_aum)
    
    # direction
    if not flatten:
        position_df = rp.get_separate('position')
        position = position_df.iloc[:,1:].values # shape of (T, n*m) T-time, n-strategies, m-symbols
        tmp_t,tmp = position.shape
        assert tmp % n_security == 0 # 防止universe数目不同的情况
        tmp_n = tmp // n_security
        tmp_m = n_security
        # 需要把position整理成 (T, n, m)
        # 现在的格式是[[n1m1, n1m2, n1m3, ...,n2m1, n2m2, ...]]
        position = position.flatten() # 转换成[t1n1m1,t1n1m2, ...]
        position = position.reshape((-1,tmp_m)) # 转换成 [t1n1[m1,m2,m3...],t1n2...]
        # 去除掉第2维度
        position = np.sum(abs(position),axis=1)
        # 转换成[t1[n1,n2,], t2[n1,n2]], 再转置
        position = position.reshape((tmp_t,tmp_n)).T
        
    else:
        position_df = rp.get_separate('position')
        position = position_df.iloc[:,1:].values.T
    direction = np.sign(position)
    
    # commission
    if not flatten:
        # 类似 direction
        commission_df = rp.get_separate('cost')
        commission = commission_df.iloc[:,1:].values # shape of (T, n*m) T-time, n-strategies, m-symbols
        tmp_t,tmp = commission.shape
        assert tmp % n_security == 0 # 防止universe数目不同的情况
        tmp_n = tmp // n_security
        tmp_m = n_security
        # 需要把position整理成 (T, n, m)
        # 现在的格式是[[n1m1, n1m2, n1m3, ...,n2m1, n2m2, ...]]
        commission = commission.flatten() # 转换成[t1n1m1,t1n1m2, ...]
        commission = commission.reshape((-1,tmp_m)) # 转换成 [t1n1[m1,m2,m3...],t1n2...]
        # 去除掉第2维度
        commission = np.sum(commission,axis=1)
        # 转换成[t1[n1,n2,], t2[n1,n2]], 再转置
        commission = commission.reshape((tmp_t,tmp_n)).T
    else:
        commission_df = rp.get_separate('cost')
        commission = commission_df.iloc[:,1:].values.T
    
    
    return output_id_array,output_universe_array,output_params_array,params_keys,rtn,direction,commission,timestamp,result_uuid


def read_benchmark_data(timestamp_list, benchmark=None):
    if not benchmark:
        return np.zeros_like(timestamp_list)
    else:
        p = _read_csv(benchmark)
    target_df = pd.DataFrame(index=timestamp_list)
    large_df = pd.concat([target_df, p],axis=1)
    large_df.fillna(method='ffill',inplace=True)
    large_df.fillna(method='bfill',inplace=True)
    value = large_df.loc[timestamp_list,'close'].values
    rtn = value[1:]/value[:-1]-1
    return np.concatenate([np.array([0]),rtn])    
    
def _read_csv(benchmark):
    data_dir_path = pathlib.Path(__file__).parent.parent.absolute().joinpath('data')
    if benchmark in ["000300.SH"]:
        tmp = pd.read_csv(data_dir_path.joinpath(f"{benchmark}.csv"))
        return _format_daily_result(tmp)
    else:
        raise ValueError(f"{benchmark} is not supported!")
    
    
def _format_daily_result(df):
    '''
        transform the original result from dm to desired format
    '''
    df.set_index('date',inplace=True)
    df.index = [int(x.timestamp()) for x in pd.to_datetime(df.index)]
    df.index.name = 'timestamp'
    return df    