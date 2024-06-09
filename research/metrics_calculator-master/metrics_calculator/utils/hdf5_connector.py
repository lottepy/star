import h5py
import os
import numpy as np
from backtest_engine.utils.result_processer import ResultProcesser 

class HDF5_Connector:
    def __init__(self,filename,compression='lzf',**kwargs):
        '''
            input:
                filename: path pointed to a file
                compression:
                    False: no compression
                    "lzf": fast
                    "gzip": compact, can further assign compress level "compression_opts" from 0 to 9
        '''
        self.filename = filename
        
        assert compression in [False,'lzf','gzip']
        self.compression_setting = {'compression':compression}
        if compression == 'gzip':
            compression_opts = kwargs.get('compression_opts',5)
            self.compression_setting.update({'compression_opts':compression_opts})
        
    def connect(self):
        '''
            return the root group object. Attention must be paid to close the file after using.
        '''
        self.f = h5py.File(self.filename,'a')
        return self.f

    def disconnect(self):
        '''
            close the h5py file object
        '''
        try:
            self.f.close()
        except Exception as e:
            print(e)
    
    def clean(self):
        '''
            clean the existing hdf5 file at location <filename>
        '''
        if os.path.exists(self.filename):
            os.remove(self.filename)
    
    def _init_hdf5(self,**kwargs):
        '''
            create a hdf5 object at location <filename> with given attrs for root group
        '''
        with h5py.File(self.filename,'a') as f:
            # for key, value in kwargs.items():
            #     f.attrs[key] = value
            f.attrs.update(kwargs)
    
    def init(self, uuid, **kwargs):
        '''
            Given a uuid, if uuid match with the existing hdf5's attrs['uuid'], new data will be appended.
            Otherwise, the old one will be reomved first.
        '''
        if os.path.exists(self.filename):
            f = self.connect()
            my_uuid = f.attrs.get('uuid',None)
            self.disconnect()
            if uuid is not None and my_uuid is not None and uuid==my_uuid:
                self._init_hdf5(**kwargs)
            else:
                self.clean()
                attrs_dict = {'uuid':uuid}
                attrs_dict.update(kwargs)
                self._init_hdf5(**attrs_dict)
        else:
            attrs_dict = {'uuid':uuid}
            attrs_dict.update(kwargs)
            self._init_hdf5(**attrs_dict)
    
    def create_group(self,group_name):
        '''
            create a group at <group_name>, support a name as 'group1/subgroup1'
            do nothing if a group has already existed.
        '''
        with h5py.File(self.filename,'a') as f:
            try:
                f.create_group(group_name)
            except ValueError as e:
                pass

    def create_structure(self,**kwargs):
        '''
            create the desired group structure for different purpose
        '''
        raise NotImplementedError

class HDF5Connector(HDF5_Connector):
    def create_structure(self,n_strategy,n_timepoints):
        '''
            create the desired group structure for use of metrics calculator
        '''
        with h5py.File(self.filename,'a') as f:
            if 'basic_info' not in f.keys():
                f.create_group('basic_info')
            if 'metrics' not in f.keys():
                f.create_group('metrics')
            if 'data' not in f.keys():
                group = f.create_group('data')
                group.create_dataset('rtn',shape=(n_strategy,n_timepoints),dtype=np.float64,**self.compression_setting)
                group.attrs['rtn_shape'] = (n_strategy,n_timepoints)
                group.create_dataset('commission',shape=(n_strategy,n_timepoints),dtype=np.float64,**self.compression_setting)
                group.create_dataset('direction',shape=(n_strategy,n_timepoints),dtype=np.int64,**self.compression_setting)
                group.attrs['filled'] = False
    
    def insert_basic_info(self, id_array, universe_array, params_array, params_keys, timestamp):
        '''
            insert basic info into the hdf5 file
        '''
        with h5py.File(self.filename,'a') as f:
            group = f['basic_info']
            # insert id_array if not already exists
            if 'strategy_id' not in group.keys():
                group.create_dataset('strategy_id',data=id_array,dtype=np.int64,**self.compression_setting) # shape of (n,)
            # insert universe_array
            if 'universe_array' not in group.keys():
                group.create_dataset('universe_array',data=np.array(universe_array,dtype='S'),**self.compression_setting) # shape of (n,1) or (n,m)
            # insert params_array
            if 'params_array' not in group.keys():
                group.create_dataset('params_array',data=params_array,**self.compression_setting) # shape of (n,k) for n strategies and k params
                group.attrs['params_keys'] = np.array(params_keys,dtype='S')
            # insert timestamp
            if 'timestamp' not in group.keys():
                group.create_dataset('timestamp',data=timestamp,dtype=np.int64,**self.compression_setting)
            

    # def insert_data(self, rtn, direction,commission,timestamp):
    #     '''
    #         insert data into the hdf5 file
    #     '''
    #     with h5py.File(self.filename,'a') as f:
    #         group = f['data']
    #         # insert rtn
    #         if 'rtn' not in group.keys():
    #             group.create_dataset('rtn',data=rtn,dtype=np.float64,**self.compression_setting)
    #             group.attrs['rtn_shape'] = rtn.shape
    #         # insert direction
    #         if 'direction' not in group.keys():
    #             group.create_dataset('direction',data=direction,dtype=np.int64,**self.compression_setting)
    #         # insert timestamp
    #         if 'timestamp' not in group.keys():
    #             group.create_dataset('timestamp',data=timestamp,dtype=np.int64,**self.compression_setting)
    #         # insert commission
    #         if 'commission' not in group.keys():
    #             group.create_dataset('commission',data=commission,dtype=np.float64,**self.compression_setting)

    def transfer_data(self,path,flatten=True,stock_like=False,ref_aum=10000,timestamp_ix_filter=None):
        '''
            now this is just a script, not actually a function
            #TODO: Refactor needed!
        '''
        f = self.connect()
        if f['data'].attrs['filled']:
            return
        if not flatten:
            rp = ResultProcesser(path)
            summary_df = rp.get_summary()
            universe_array = np.array(summary_df.loc[:,'universe'].values.tolist())
            n_security = universe_array.shape[1]
            # rtn
            pnl_df = rp.get_pv()
            if timestamp_ix_filter:
                pnl_df = pnl_df.iloc[timestamp_ix_filter]
            pnl_df.drop('datetime',axis=1,inplace=True)
            pnl = pnl_df.values.T    
            pnl_extend = np.hstack((np.zeros((pnl.shape[0],1)),pnl))
            
            if stock_like:
                pnl_extend = pnl_extend + ref_aum
                rtn = (pnl_extend[:,1:]/pnl_extend[:,:-1]-1)
            else:
                rtn = ((pnl_extend[:,1:] - pnl_extend[:,:-1])/ref_aum)
            f['data/rtn'][()] = rtn
            
            # commission
            # 类似 direction
            commission_df = rp.get_separate('cost')
            if timestamp_ix_filter:
                commission_df = commission_df.iloc[timestamp_ix_filter]
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
            f['data/commission'][()] = commission
            
            # direction
            position_df = rp.get_separate('position')
            if timestamp_ix_filter:
                position_df = position_df.iloc[timestamp_ix_filter]
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
            f['data/direction'][()] = np.sign(position)
            del rp
        else:
            g = h5py.File(os.path.join(path,'result.hdf5'),'r')
            keys = [key for key in g.keys() if 'virtual' in key]
            ix_start = 0
            for i in range(len(keys)):
                key = f'virtual_{i}'
                
                # rtn
                pnl = g[key]['pnl_array'][()].T
                if timestamp_ix_filter:
                    pnl = pnl[:,timestamp_ix_filter]
                pnl_extend = np.hstack((np.zeros((pnl.shape[0],1)),pnl))
    
                if stock_like:
                    pnl_extend = pnl_extend + ref_aum
                    rtn = (pnl_extend[:,1:]/pnl_extend[:,:-1]-1)
                else:
                    rtn = ((pnl_extend[:,1:] - pnl_extend[:,:-1])/ref_aum)
                f['data/rtn'][ix_start:(ix_start+len(rtn)),:] = rtn
                # commission
                commission = g[key]['cost_array'][()].T
                if timestamp_ix_filter:
                    pcommissionnl = commission[:,timestamp_ix_filter]
                f['data/commission'][ix_start:(ix_start+len(rtn)),:] = commission
                                
                # direction
                position = g[key]['position_array'][()].T
                if timestamp_ix_filter:
                    position = position[:,timestamp_ix_filter]
                f['data/direction'][ix_start:(ix_start+len(rtn)),:] = np.sign(position)
                
                ix_start = ix_start+len(rtn)
            g.close()
        f['data'].attrs['filled'] = True
        self.disconnect()
        

    def create_metrics_dataset(self, metrics_name, allocation_freq, look_back_points_list, sample_ix_array_list, id_list, chunk_size, benchmark):
        '''
            create metrics group and its dataset.
            add metrics_name as an attr of metrics group.
            use {allocation_freq}_{look_back_points} as the name of the metrics group.
            use (1, 2^20 / len(metrics_name) /8 (bytes),len(metrics_name)) as chunk size so that chunk size less than 1MiB
            dimension as (len(sample_ix_array), len(strategies), len(metrics_name))
        '''
        metrics_group_name_list = [f'{allocation_freq}_{look_back_point}' for look_back_point in look_back_points_list]
        with h5py.File(self.filename,'a') as f:
            # sample metrics
            group = f['metrics']
            for i,metrics_group_name in enumerate(metrics_group_name_list):
                sample_ix_array = sample_ix_array_list[i]
                if metrics_group_name not in group.keys():
                    dim = (len(sample_ix_array),len(id_list),len(metrics_name))
                    chunk_size = tuple([min(dim[i],chunk_size[i]) for i in range(len(dim))])
                    sub_group = group.create_group(metrics_group_name)
                    sub_group.attrs['metrics_name'] = metrics_name
                    sub_group.attrs['benchmark'] = benchmark if benchmark else ""
                    sub_group.create_dataset('data', shape=dim, chunks=chunk_size, dtype=np.float64, **self.compression_setting)
                    sub_group.create_dataset('sample_ix_array',data=sample_ix_array,dtype=np.int64,**self.compression_setting)
            # whole period metrics
            if 'whole_period' not in group.keys():
                sub_group = group.create_group('whole_period')
                sub_group.create_dataset('data', shape = (len(id_list), len(metrics_name)),dtype=np.float64,**self.compression_setting)
                sub_group.attrs['metrics_name'] = metrics_name
                sub_group.attrs['benchmark'] = benchmark if benchmark else ""
                
    def insert_metrics(self, data, id_list, location):
        '''
            insert metrics into the hdf5 file
        '''
        with h5py.File(self.filename,'a') as f:
            id_start = min(id_list)
            id_end = max(id_list)+1
            if location == 'whole_period':
                f['metrics/whole_period/data'][id_start:id_end,:] = data
            else:
                n = len(id_list)
                m = data.shape[1]
                f[f'metrics/{location}/data'][:,id_start:id_end,:] = data.reshape(-1,n,m)
                
            