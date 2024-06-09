import numpy as np
import pandas as pd
from datetime import datetime as dt
from numba.experimental import jitclass
from numba import types
import numba as nb
from numba.typed import Dict, List

float64_3d_array_type = nb.float64[:,:,:]
int64_2d_array_type = nb.int64[:,:]
@jitclass({
        'window_size_dict' : types.DictType(types.unicode_type, nb.int64),
        'start_time' : nb.int64,
        'end_time': nb.int64,
        'data_dict' : types.DictType(types.unicode_type, float64_3d_array_type),
        'time_dict': types.DictType(types.unicode_type, int64_2d_array_type),
        'idx': nb.int64[:],
        'exchange_data_idx': nb.int64[:],
        'pair_num': nb.int64,
        'time_dots': nb.int64,
        'start_index_array': nb.int64[:,:],
        'end_index_array': nb.int64[:,:],
        'strategy_mask_dict': types.DictType(types.unicode_type, nb.boolean[:,:]),
        'name_list': types.ListType(types.unicode_type),
        'main_field': types.unicode_type
})

class DataDistributor():
    def __init__(self, window_size_dict, start_time, end_time, n_strategy, mask_dict, main_field):
        self.window_size_dict = window_size_dict
        self.start_time = start_time
        self.end_time=end_time
        self.pair_num = 0
        self.idx = np.zeros(n_strategy, dtype=np.int64)
        self.exchange_data_idx = np.zeros(n_strategy, dtype=np.int64)
        self.strategy_mask_dict = mask_dict
        self.data_dict = Dict.empty(types.unicode_type, float64_3d_array_type)
        self.time_dict = Dict.empty(types.unicode_type, int64_2d_array_type)
        self.name_list = List.empty_list(types.unicode_type)
        self.main_field = main_field

    def load_data(self,data_array,time_array, name):
        self.pair_num += 1
        self.name_list.append(name)
        self.data_dict[name] = data_array
        self.time_dict[name] = time_array

    @staticmethod
    def find_start_index(index_array, end_index_array, window_size):
        for i in nb.prange(end_index_array.shape[0]):
            index_array[i] = max(end_index_array[i] - window_size, 0)
        return index_array

    @staticmethod
    def find_end_index(index_array, main_time, other_time):
        #找到main_time中每个元素，other_time中大于这个元素的最小index
        # 例如 main_time = [1, 10, 20]
        #     other_time = [0,1,2,3,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,21]
        #     index_array = [2,10,19]
        # 若   main_time = [1,2,3,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,21]
        #     other_time = [1, 10, 20]
        #     index_array = [1,1,1,1,1,1,1,1,2,2,2,2,2,2,2,2,2,2,3]
        k = 0
        j = 0
        for i in nb.prange(main_time.shape[0]):
            for j in nb.prange(k, other_time.shape[0]):
                if other_time[j] > main_time[i]:
                    index_array[i] = j
                    break
            if (j == other_time.shape[0] - 1) & (other_time[j] <= main_time[i]):
                index_array[i] = j+1
                break
            k = j
        return index_array

    def prepare(self):
        main = self.main_field
        main_start_idx = np.where(self.time_dict[main][:, 0] >= self.start_time)[0][0]
        main_end_idx = np.where(self.time_dict[main][:, 0] <= self.end_time)[0][-1]
        self.time_dots = main_end_idx - main_start_idx + 1

        self.start_index_array = np.ones((self.time_dots, self.pair_num), dtype=np.int64)
        self.end_index_array = np.ones((self.time_dots, self.pair_num), dtype=np.int64)
        main_time_array = self.time_dict[main][main_start_idx:main_end_idx+1, 0]
        for i in range(self.pair_num):
            other_time_array = self.time_dict[self.name_list[i]][:, 0]
            self.end_index_array[:, i].fill(len(other_time_array))
            self.end_index_array[:, i] = self.find_end_index(self.end_index_array[:, i],
                                                             main_time_array, other_time_array)
            name = self.name_list[i]
            window_size = self.window_size_dict[name]
            self.start_index_array[:, i] = self.find_start_index(self.start_index_array[:, i],
                                                                 self.end_index_array[:, i], window_size)

    def distribute(self, strategy_id):
        window_data_dict = Dict()
        window_time_dict = Dict()
        if self.idx[strategy_id] < self.time_dots:
            for i in range(self.pair_num):
                name = self.name_list[i]
                start_idx = self.start_index_array[self.idx[strategy_id], i]
                end_idx = self.end_index_array[self.idx[strategy_id], i]
                window_data_dict[name] = self.data_dict[name][start_idx:end_idx]
                window_time_dict[name] = self.time_dict[name][start_idx:end_idx]
                #Todo: add symbol index to dict
                symbol_idx = self.strategy_mask_dict[name][strategy_id]
                window_data_dict[name] = window_data_dict[name][:, symbol_idx]

        self.idx[strategy_id] += 1
        return window_data_dict, window_time_dict

    # this is not a nature feature
    # should be optimized later
    def distribute_exchange_data(self, strategy_id):
        window_data_dict = Dict()
        window_time_dict = Dict()
        if self.exchange_data_idx[strategy_id] < self.time_dots:
            for i in range(self.pair_num):
                name = self.name_list[i]
                start_idx = self.start_index_array[self.exchange_data_idx[strategy_id], i]
                end_idx = self.end_index_array[self.exchange_data_idx[strategy_id], i]
                window_data_dict[name] = self.data_dict[name][start_idx:end_idx]
                window_time_dict[name] = self.time_dict[name][start_idx:end_idx]
        self.exchange_data_idx[strategy_id] += 1
        return window_data_dict,window_time_dict




if __name__=="__main__":

    # daily 数据1,主循环
    period1 = 3000
    symbol1 = 2
    field1 = 3
    data_array1 = np.random.rand(period1, symbol1, field1)
    time_array1 = np.array([dt.timestamp() for dt in
                            list(pd.date_range(start = '1-1-2010', periods=period1, freq ='D'))]).reshape(-1,1,1)



    # daily 数据2
    period2 = 2900
    symbol2 = 10
    field2 = 1
    data_array2 = np.random.rand(period2, symbol2, field2)
    time_array2 = np.array([dt.timestamp() for dt in
                            list(pd.date_range(start = '1-1-2010', periods=period2, freq ='D'))]).reshape(-1,1,1)

    # hourly 数据1
    period3 = 100000
    symbol3 = 2
    field3 = 1
    data_array3 = np.random.rand(period3, symbol3, field3)
    time_array3 = np.array([dt.timestamp() for dt in
                            list(pd.date_range(start = '1-1-2010', periods=period3, freq ='H'))]).reshape(-1,1,1)

    # minly 数据1
    period4 = 1000000
    symbol4 = 1
    field4 = 1
    data_array4 = np.random.rand(period4, symbol4, field4)
    time_array4 = np.array([dt.timestamp() for dt in
                            list(pd.date_range(start = '1-1-2010', periods=period4, freq ='min'))]).reshape(-1,1,1)



    window_size_dict = Dict()

    window_size_dict["main"] = 1
    window_size_dict["daily"] = 1
    window_size_dict["hourly"] = 1
    window_size_dict["minly"] = 1

    start_time = dt(2011,2,1)
    end_time = dt(2011,3,20)

    start_time=dt.timestamp(start_time)
    end_time=dt.timestamp(end_time)



    data_distributor = DataDistributor(window_size_dict, start_time,end_time)
    data_distributor.load_data(data_array1,time_array1, "main")
    data_distributor.load_data(data_array2, time_array2, "daily")
    data_distributor.load_data(data_array3, time_array3, "hourly")
    data_distributor.load_data(data_array4, time_array4, "minly")
    data_distributor.prepare()

    t_start = dt.now()
    for i in range(data_distributor.time_dots):
        data_distributor.distribute()
    print(data_distributor.time_dots)
    t_end = dt.now()
    print(t_end-t_start)

    print("finish")
