import numpy as np
import pickle
from backtest_engine.utils.constants import Data, Rate,Time


class SuperStrategyBase:
    def __init__(self, symbols, ccy_list):
        self.symbols = symbols
        self.ccy_list = ccy_list

        self.real_strategy_dict = {}
        self.virtual_strategy_dict = {}
        self.real_strategy_weight = {}
        self.strategy_id = []
        self.strategy_id_2_strategy_symbol_index = {}
        self.strategy_id_2_strategy_ccy_index = {}

        # e.g. 有1000个strategy，但是只有2个不同的universe，则self.strategy_id_2_universe_id={1:1, 2:1, 3:1, .., 999:2}
        #      如果只有几个universe，就可以cache下来，不需要每次都用strategy_symbol_index对array做slicing
        #      但是，如果有很多个universe，cache就会特别占内存
        #      ccy也同理
        self.universe_id_counter = 1
        self.ccy_id_counter = 1
        self.strategy_id_2_universe_id = {}
        self.strategy_id_2_ccy_id = {}
        self.strategy_symbol_index_2_universe_id = {}
        self.strategy_ccy_index_2_ccy_id = {}

        self.real_strategy_weight_list = []
        self.timestamp_list = []

    def add_param(self , is_strategy_allocation = False):
        self.is_strategy_allocation = is_strategy_allocation

    def add_strategy(self, real_strategy, virtual_strategy, id, strategy_symbols_index: np.ndarray,
                     strategy_ccy_index: np.ndarray):
        self.real_strategy_dict[id] = real_strategy
        self.virtual_strategy_dict[id] = virtual_strategy
        self.real_strategy_weight[id] = None
        self.strategy_id_2_strategy_symbol_index[id] = np.array(strategy_symbols_index)
        self.strategy_id_2_strategy_ccy_index[id] = np.array(strategy_ccy_index)
        self.strategy_id.append(id)

        # 增加strategy的时候先做 id -> universe_id 的mapping
        strategy_symbols_index_tuple = tuple(strategy_symbols_index)
        if strategy_symbols_index_tuple in self.strategy_symbol_index_2_universe_id:
            self.strategy_id_2_universe_id[id] = self.strategy_symbol_index_2_universe_id[strategy_symbols_index_tuple]
        else:
            self.strategy_id_2_universe_id[id] = self.universe_id_counter
            self.strategy_symbol_index_2_universe_id[strategy_symbols_index_tuple] = self.universe_id_counter
            self.universe_id_counter += 1

        # ccy同理
        strategy_ccy_index_tuple = tuple(strategy_ccy_index)
        if strategy_ccy_index_tuple in self.strategy_ccy_index_2_ccy_id:
            self.strategy_id_2_ccy_id[id] = self.strategy_ccy_index_2_ccy_id[strategy_ccy_index_tuple]
        else:
            self.strategy_id_2_ccy_id[id] = self.ccy_id_counter
            self.strategy_ccy_index_2_ccy_id[strategy_ccy_index_tuple] = self.ccy_id_counter
            self.ccy_id_counter += 1


    def strategy_allocate(self, current_time):
        pass

    def return_allocation_time(self, time_array):
        if len(time_array)>400 and time_array[1][Time.TIMESTAMP.value] - time_array[0][Time.TIMESTAMP.value] > 3601:
            size = int(len(time_array)/5)
            return np.array( [time_array[size],time_array[size*2],time_array[size*3],time_array[size*4]] )
        else:
            return np.array([])


    #@profile
    def on_data(self, window_data, time_data, ref_window_data, ref_window_data_time, ref_window_factor,ref_window_factor_time,is_close):
        shared_data = {}  # 对于每一次的数据输入，每个virtual strategy & real strategy所需要的一些指标计算结果是一致的；另外不同参数的strategy可能也能共享一部分计算结果
        for id in self.strategy_id:
            strategy_symbol_index = self.strategy_id_2_strategy_symbol_index[id]
            strategy_window_data = window_data[self.strategy_id_2_universe_id[id]]
            strategy_ccy_index = self.strategy_id_2_strategy_ccy_index[id]
            strategy_ref_window_data = {}
            for key, data in ref_window_data.items():
                strategy_ref_window_data[key] = data[:, strategy_symbol_index, :]
            is_contain_f = hasattr(self.virtual_strategy_dict[id], 'on_data' if is_close else 'on_data_open')
            if is_contain_f:
                virtual_f = self.virtual_strategy_dict[id].on_data if is_close else self.virtual_strategy_dict[id].on_data_open
                real_f = self.real_strategy_dict[id].on_data if is_close else self.real_strategy_dict[id].on_data_open

            if self.virtual_strategy_dict[id].strategy_utility.is_tradable.any() and virtual_f is not None:
                virtual_f(strategy_window_data, time_data, strategy_ref_window_data,ref_window_data_time, ref_window_factor, ref_window_factor_time, shared_data)
                                                    
            if self.is_strategy_allocation and self.real_strategy_dict[id].strategy_utility.is_tradable.any() and real_f is not None:
                real_f(strategy_window_data, time_data, strategy_ref_window_data,ref_window_data_time, ref_window_factor, ref_window_factor_time, shared_data)

        virtual_order = {}
        real_order = {}
        combine_signal = None
        for id in self.strategy_id:
            strategy_symbol_index = self.strategy_id_2_strategy_symbol_index[id]
            virtual_order[id] = self.virtual_strategy_dict[id].strategy_utility.fire_order()
            if self.is_strategy_allocation:
                real_order[id] = self.real_strategy_dict[id].strategy_utility.fire_order() if self.real_strategy_weight[id] is not None else None
            else:
                real_order[id] = None

            if real_order[id] is not None:
                if combine_signal is None:
                    combine_signal = np.zeros(len(self.symbols))
                temp_ccy_target = np.zeros(len(self.symbols))
                temp_ccy_target[strategy_symbol_index] = (self.real_strategy_dict[id].strategy_utility.fire_target())  * self.real_strategy_weight[id]
                combine_signal += temp_ccy_target
        return virtual_order, real_order, combine_signal


    def rec_feedback(self, id2virtual_order_feedback, id2real_order_feedback):
        for id in self.strategy_id:
            self.virtual_strategy_dict[id].strategy_utility.rec_feedback(id2virtual_order_feedback[id])
            if self.is_strategy_allocation:
                self.real_strategy_dict[id].strategy_utility.rec_feedback(id2real_order_feedback[id])

    def strategy_capture(self, timestamp):
        for id in self.strategy_id:
            self.virtual_strategy_dict[id].strategy_utility.capture(timestamp)
            if self.is_strategy_allocation:
                self.real_strategy_dict[id].strategy_utility.capture(timestamp)

    def capture(self, timestamp):
        if self.is_strategy_allocation:
            self.real_strategy_weight_list.append(str(np.copy(self.real_strategy_weight)))
            self.timestamp_list.append(int(timestamp))

    def save_window_data(self, window_past_data, window_past_fx_data, window_rate_data, window_rate_time_data):
        # 一般来说 #universe,#ccy << #strategy，针对universe和ccy进行缓存以加速本函数
        universe_id_2_window_past_data = {}
        universe_id_2_window_rate_data = {}
        ccy_id_2_window_past_fx_data = {}

        for (strategy_symbol_index_tuple, universe_id) in self.strategy_symbol_index_2_universe_id.items():
            universe_id_2_window_past_data[universe_id] = window_past_data[:, list(strategy_symbol_index_tuple), :]
            universe_id_2_window_rate_data[universe_id] = window_rate_data[:, list(strategy_symbol_index_tuple)]

        for (strategy_ccy_index_tuple, ccy_id) in self.strategy_ccy_index_2_ccy_id.items():
            ccy_id_2_window_past_fx_data[ccy_id] = window_past_fx_data[:, list(strategy_ccy_index_tuple)]

        for id in self.strategy_id:
            # original
            # strategy_window_past_data = window_past_data[:, strategy_symbol_index, :]
            # strategy_window_past_fx_data = window_past_fx_data[:, strategy_ccy_index, ]
            # strategy_window_rate_data = window_rate_data[:, strategy_symbol_index]
            strategy_window_past_data = universe_id_2_window_past_data[self.strategy_id_2_universe_id[id]]
            strategy_window_rate_data = universe_id_2_window_rate_data[self.strategy_id_2_universe_id[id]]
            strategy_window_past_fx_data = ccy_id_2_window_past_fx_data[self.strategy_id_2_ccy_id[id]]
            if self.is_strategy_allocation:
                self.real_strategy_dict[id].strategy_utility.save_window_data(strategy_window_past_data,
                                                                            strategy_window_past_fx_data,
                                                                            strategy_window_rate_data,
                                                                            window_rate_time_data)
            self.virtual_strategy_dict[id].strategy_utility.save_window_data(strategy_window_past_data,
                                                                             strategy_window_past_fx_data,
                                                                             strategy_window_rate_data,
                                                                             window_rate_time_data)

    def save_current_data(self, current_fx_data, current_data, current_rate,is_tradable):
        """
            Input:
            - current_fx_data: shape (n_ccy, )
            - current_data: shape (n_symbols, )
            - current_rate: shape (n_symbols, )
        """
        for id in self.strategy_id:
            strategy_symbol_index = self.strategy_id_2_strategy_symbol_index[id]
            strategy_ccy_index = self.strategy_id_2_strategy_ccy_index[id]
            strategy_current_fx_data = current_fx_data[strategy_ccy_index]
            strategy_current_data = current_data[strategy_symbol_index]
            strategy_current_rate = current_rate[strategy_symbol_index]
            strategy_is_tradable = is_tradable[strategy_symbol_index]
            if self.is_strategy_allocation:
                self.real_strategy_dict[id].strategy_utility.save_current_data(strategy_current_fx_data,
                                                                                strategy_current_data,
                                                                                strategy_current_rate,strategy_is_tradable)
            self.virtual_strategy_dict[id].strategy_utility.save_current_data(strategy_current_fx_data,
                                                                              strategy_current_data,
                                                                              strategy_current_rate,strategy_is_tradable)


    def is_active(self, id):
        return id in self.strategy_id  # and self.real_strategy_weight[id] > 0

    def reset_field_rollover_day(self,is_tradable):
        for id in self.strategy_id:
            strategy_symbol_index = self.strategy_id_2_strategy_symbol_index[id]
            strategy_is_tradable = is_tradable[strategy_symbol_index]
            self.virtual_strategy_dict[id].order_manager.reset_field_rollover_day(strategy_is_tradable)
            if self.is_strategy_allocation:
                self.real_strategy_dict[id].order_manager.reset_field_rollover_day(strategy_is_tradable)

    def calculate_rate(self, current_fx_data, current_data, current_rate):
        for id in self.strategy_id:
            strategy_symbol_index = self.strategy_id_2_strategy_symbol_index[id]
            strategy_ccy_index = self.strategy_id_2_strategy_ccy_index[id]
            strategy_current_fx_data = current_fx_data[strategy_ccy_index]
            strategy_current_data = current_data[strategy_symbol_index]
            strategy_current_rate = current_rate[strategy_symbol_index]
            self.virtual_strategy_dict[id].order_manager.calculate_rate(strategy_current_rate, strategy_current_data,strategy_current_fx_data)
            if self.is_strategy_allocation:
                self.real_strategy_dict[id].order_manager.calculate_rate(strategy_current_rate, strategy_current_data,strategy_current_fx_data)

    def calculate_spot(self, current_fx_data, current_data):
        for id in self.strategy_id:
            strategy_symbol_index = self.strategy_id_2_strategy_symbol_index[id]
            strategy_ccy_index = self.strategy_id_2_strategy_ccy_index[id]
            strategy_current_fx_data = current_fx_data[strategy_ccy_index]
            strategy_current_data = current_data[strategy_symbol_index]
            self.virtual_strategy_dict[id].order_manager.calculate_spot(strategy_current_data, strategy_current_fx_data)
            if self.is_strategy_allocation:
                self.real_strategy_dict[id].order_manager.calculate_spot(strategy_current_data, strategy_current_fx_data)

    def cross_order(self, virtual_ccp_target: dict,real_ccp_target : dict, current_data, current_fx_data):
        id2virtual_order_feedback = {}
        id2real_order_feedback = {}

        for id in self.strategy_id:
            strategy_symbol_index = self.strategy_id_2_strategy_symbol_index[id]
            strategy_ccy_index = self.strategy_id_2_strategy_ccy_index[id]
            strategy_current_fx_data = current_fx_data[strategy_ccy_index]
            strategy_current_data = current_data[strategy_symbol_index]
            id2virtual_order_feedback[id] = self.virtual_strategy_dict[id].order_manager.cross_order(virtual_ccp_target[id],
                                                                                                     strategy_current_data,
                                                                                                     strategy_current_fx_data)
            if self.is_strategy_allocation:
                id2real_order_feedback[id] = self.real_strategy_dict[id].order_manager.cross_order(real_ccp_target[id],
                                                                                                        strategy_current_data,
                                                                                                        strategy_current_fx_data)
        return id2virtual_order_feedback,id2real_order_feedback

    def order_manager_capture(self, current_time_ts):
        for id in self.strategy_id:
            self.virtual_strategy_dict[id].order_manager.capture(current_time_ts)
            if self.is_strategy_allocation:
                self.real_strategy_dict[id].order_manager.capture(current_time_ts)

    def close_position(self,current_data,current_fx_data):
        if not self.is_strategy_allocation:
            return
        for id in self.strategy_id:
            if self.real_strategy_weight[id] is None:
                self.real_strategy_dict[id].order_manager.close_position(current_data,current_fx_data)

    def forget(self,record_ts):
        for id in self.strategy_id:
            self.virtual_strategy_dict[id].order_manager.forget(record_ts)
            if self.is_strategy_allocation:
                self.real_strategy_dict[id].order_manager.forget(record_ts)
