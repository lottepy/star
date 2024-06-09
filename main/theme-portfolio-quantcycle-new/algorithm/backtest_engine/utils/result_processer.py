import ast
import itertools
import os
from datetime import datetime
from enum import Enum
from typing import Dict, List, Set

import h5py
import numpy as np
import pandas as pd

"""
关于数据输出格式的需求见 https://confluence-algo.aqumon.com/x/YQGAAQ
"""




class ResultProcesser:
    def __init__(self, result_path: str):
        self.result_path = result_path
        self.f = h5py.File(os.path.join(result_path, 'result.hdf5'), 'r')

        # 载入后，作一些初步的检查：
        #   1. timestamp必须相同
        #   2. 待定
        #self.timestamp_list: np.ndarray = self.f[list(self.f.keys())[0]]['timestamp_list'][()]  # 获取第一个dataset的timestamp_list
        self.timestamp_list: np.ndarray = self.f[list(self.f.keys())[0]]["timestamp_list"]
        """ for i in range(0,1000):
            timestamp_list_index = f"timestamp_list_{i}"
            if timestamp_list_index in self.f[list(self.f.keys())[0]]:
                self.timestamp_list = np.append(self.timestamp_list,self.f[list(self.f.keys())[0]][timestamp_list_index][()])
            else:
                break """

        #for group in self.f.values():
        #    assert (group['timestamp_list'][()] == self.timestamp_list).all(), 'timestamp 不相同'

    def __del__(self):
        try:
            self.f.close()
        except:
            pass

    def get_summary(self,strategy_type = "virtual") -> pd.DataFrame:
        """
            返回DataFrame

            columns = [param1, param2, .., param_n, universe, final_pv]

            rows = strategies
        """
        rows: List[dict] = []

        for group in self.f.values():
            if group.attrs['type'] != strategy_type:
                continue
            #pnl_array = hdf5_group_linking(group,"pnl_array")
            pnl_array = group["pnl_array"]
            row_dict = {'strategy_id': group.attrs['id'], 'universe': group.attrs['universe'].tolist(),
                        'final_pv': np.sum(pnl_array[-1])}  # final_pv是最后一行的各symbol pv之和
            strategy_param_dict = ast.literal_eval(group.attrs['strategy_param'])
            row_dict.update(strategy_param_dict)
            rows.append(row_dict)

        return pd.DataFrame(rows).sort_values('strategy_id').reset_index(drop=True)

    def get_pv(self,strategy_type = "virtual") -> pd.DataFrame:
        """ 
            返回各strategy的所有symbol的pv之和的DataFrame

            columns = [datetime, 0_pv, 1_pv, 2_pv, .., n_pv]

            rows = time points
        """
        data_dict = {'datetime': [datetime.utcfromtimestamp(ts) for ts in self.timestamp_list]}

        keys_sorted = sorted([(s) for s in self.f.keys() if strategy_type in s],key=lambda x: int(x.split('_')[-1]))
        for strategy_id in keys_sorted:
            temp_f = self.f[f'{strategy_id}']
            if temp_f.attrs['type'] != strategy_type:
                continue
            #pnl_array = hdf5_group_linking(self.f[f'{strategy_id}'],"pnl_array")
            pnl_array = self.f[f'{strategy_id}']["pnl_array"]
            data_dict[f'{strategy_id}_pv'] = np.sum(pnl_array, axis=1)

        return pd.DataFrame(data_dict)

    def get_separate(self, data_type: str, strategy_type = 'virtual') -> pd.DataFrame:
        """
            返回具体到每一个symbol的特定类型数据的DataFrame。data_type可选['pnl', 'cost', 'position']

            e.g. columns = [datetime, 0_symbol1_data_type, 0_symbol2_data_type, .., n_symbol1_data_type, n_symbol2_data_type]

            rows = time points
        """
        assert data_type in ['pnl', 'cost', 'position'], f'{data_type} not supported'

        time = [datetime.utcfromtimestamp(ts) for ts in self.timestamp_list]

        keys = [s for s in self.f.keys() if strategy_type in s]

        keys_sorted = sorted(keys,key=lambda x: int(x.split('_')[-1]))
        _columns = []
        for strategy_id in keys_sorted:
            _columns.extend([f'{strategy_id}_{symbol}_{data_type}' for symbol in self.f[f'{strategy_id}'].attrs['universe']])

        values = np.empty((len(time),len(_columns)))
        ix = 0
        for strategy_id in keys_sorted:
            #tmp = hdf5_group_linking(self.f[f'{strategy_id}'],f'{data_type}_array')
            tmp = self.f[f'{strategy_id}'][f'{data_type}_array']
            values[:,ix:ix+tmp.shape[1]] = tmp
            ix = ix+tmp.shape[1]
            
        output = pd.DataFrame(values,columns=_columns,index=time)
        output.index.rename('datetime',inplace=True)
        return output.reset_index()

    def get_strategy(self, strategy_id: str) -> pd.DataFrame:
        """
            返回给定strategy_id的详细结果的DataFrame

            columns = [datetime, timestamp, symbol1_pv, .., symboln_pv, pv,
                       symbol1_cost, .., symboln_cost, symbol1_position, .., symboln_position,
                       remark]

            rows = time points
        """
        assert isinstance(strategy_id, str), 'strategy_id should be string'

        df = pd.DataFrame({'datetime': [datetime.utcfromtimestamp(ts) for ts in self.timestamp_list]})
        df['timestamp'] = self.timestamp_list
        
        group = self.f[strategy_id]

        # pv
        #pnl_array = hdf5_group_linking(group,"pnl_array")
        pnl_array = group["pnl_array"]
        new_df = pd.DataFrame(pnl_array, columns=[f'{symbol}_pv' for symbol in group.attrs['universe']])
        df = pd.concat([df, new_df], axis=1)

        # pv sum
        df['pv'] = np.sum(pnl_array, axis=1)

        # cost
        cost_array = group["cost_array"]
        new_df = pd.DataFrame(cost_array, columns=[f'{symbol}_cost' for symbol in group.attrs['universe']])
        df = pd.concat([df, new_df], axis=1)

        # position
        position_array = group["position_array"]
        new_df = pd.DataFrame(position_array, columns=[f'{symbol}_position' for symbol in group.attrs['universe']])
        df = pd.concat([df, new_df], axis=1)

        # remark
        if 'signal_remark_list' in group.attrs:
            strategy_param_list = ast.literal_eval(group.attrs['signal_remark_list'])
            new_df = pd.DataFrame(strategy_param_list).add_suffix('_remark')
            new_df['timestamp'] = group["signal_timestamp_list"][()]
            df = pd.merge(df, new_df, on='timestamp', how='outer')

        return df

    def get_result_by_ids(self, data_type: str, strategy_ids: List[str]) -> pd.DataFrame:
        """
            返回给定strategy ids列表的特定类型数据的DataFrame

            data_type可选['pv', 'cost', 'position', 'separate_pv', 'separate_cost', 'separate_position']

            columns = [datetime, n_data_type]

            rows = time points
        """
        assert data_type in ['pv', 'cost', 'position', 'separate_pv', 'separate_cost', 'separate_position'], f'{data_type} not supported'
        assert strategy_ids and isinstance(strategy_ids[0], str), 'strategy_id should be string'

        data_dict = {'datetime': [datetime.utcfromtimestamp(ts) for ts in self.timestamp_list]}
        if 'separate' in data_type:
            data_type = data_type.replace('separate_', '')
            for strategy_id in strategy_ids:
                for i, symbol in enumerate(self.f[f'{strategy_id}'].attrs['universe']):
                    data_dict[f'{strategy_id}_{symbol}_{data_type}'] = self.f[f'{strategy_id}'][f'{data_type}_array'][:, i]  # 按列抽取
        else:
            for strategy_id in strategy_ids:
                data_dict[f'{strategy_id}_{data_type}'] = np.sum(self.f[strategy_id][f'{data_type}_array'][()], axis=1)

        return pd.DataFrame(data_dict)

    def get_result_by_param(self, data_type: str, **params: dict) -> pd.DataFrame:
        """
            返回特定类型数据的、满足给定条件的DataFrame
            
            data_type可选['pv', 'cost', 'position', 'separate_pv', 'separate_cost', 'separate_position']
            params以key=value形式传入，可传入多个params同时过滤，例如 length1=10, threshold=100

            columns = [datetime, 0_data_type, 1_data_type, .., n_data_type]

            rows = time points
        """
        assert data_type in ['pv', 'cost', 'position', 'separate_pv', 'separate_cost', 'separate_position'], f'{data_type} not supported'

        data_dict = {'datetime': [datetime.utcfromtimestamp(ts) for ts in self.timestamp_list]}

        for strategy_id, group in self.f.items():
            strategy_param_dict = ast.literal_eval(group.attrs['strategy_param'])

            match_flag = True
            for key in params:
                if not (key in strategy_param_dict and params[key] == strategy_param_dict[key]):
                    match_flag = False
                    break

            if match_flag:
                if 'separate' in data_type:
                    _data_type = data_type.replace('separate_', '')
                    for i, symbol in enumerate(self.f[f'{strategy_id}'].attrs['universe']):
                        data_dict[f'{strategy_id}_{symbol}_{_data_type}'] = self.f[f'{strategy_id}'][f'{_data_type}_array'][:, i]  # 按列抽取
                else:
                    data_dict[f'{strategy_id}_{data_type}'] = np.sum(self.f[strategy_id][f'{data_type}_array'][()], axis=1)

        return pd.DataFrame(data_dict)

    def get_attrs(self, strategy_id: str) -> dict:
        """
            返回所需要的一些信息
        """
        assert isinstance(strategy_id, str), 'strategy_id should be string'

        group = self.f[strategy_id]
        d = {
            'id': group.attrs['id'],
            'universe': group.attrs['universe'].tolist(),
            'strategy_param': group.attrs['strategy_param'],
            'param_keys': group.attrs['param_keys'].tolist(),
            'cash': group.attrs['cash'],
            'order_time': group.attrs['order_time'],
            'rate_time': group.attrs['rate_time']
        }
        return d
