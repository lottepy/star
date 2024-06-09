import importlib
import os
from datetime import datetime

import h5py
import numpy as np
import pandas as pd
from numba import typed

from ..algorithm.strategy_utility.strategy_utility import StrategyUtility
from ..utils.backtest_data_processor import (prepare_current_data,
                                             prepare_time_related_data,
                                             prepare_window_related_data)
from ..utils.constants import Data, Rate, Time, TimeFreq
from ..utils.get_logger import get_logger
from ..utils.helper import (get_ref_window_and_ref_window_time,
                            get_time_index_tracker_dict,
                            hdf5_group_delete_dataset, hdf5_group_linking,
                            raw_array_to_array)
from ..utils.observer import Observer


class BackTester:
    def __init__(self, global_data_dict: dict, config_dict: dict, params: dict,hdf5_lock):
        self.logger = get_logger('backtest_engine.core.backtester')
        self.hdf5_lock = hdf5_lock
        self.hdf5_index = 0
        order_module = params.get('order', {}).get("order_module", 'backtest_engine.order.order_manager')
        order_manager = params.get('order', {}).get('order_manager', 'OrderManagerBase')
        super_stratetgy_module = params['algo'].get('super_stratetgy_module',"backtest_engine.algorithm.super_strategy.super_strategy_base")
        super_stratetgy = params['algo'].get('super_stratetgy',"SuperStrategyBase")


        self.config_dict = config_dict
        self.params = params
        ORDER_MODULE = importlib.import_module(order_module)
        self.ORDER_MANAGER = getattr(ORDER_MODULE, order_manager)
        SUPER_STRATEGY_MODULE = importlib.import_module(super_stratetgy_module)
        self.SUPER_STRATEGY = getattr(SUPER_STRATEGY_MODULE, super_stratetgy)

        self.data_array = raw_array_to_array(global_data_dict['data_array_raw'], global_data_dict['data_array_shape'])
        self.data_time_array = raw_array_to_array(global_data_dict['time_array_raw'], global_data_dict['time_array_shape'])

        self.fx_data_array = raw_array_to_array(global_data_dict['fx_data_array_raw'], global_data_dict['fx_data_array_shape'])
        self.fx_data_time_array = raw_array_to_array(global_data_dict['fx_data_time_array_raw'],
                                                     global_data_dict['fx_data_time_array_shape'])

        self.rate_array = raw_array_to_array(global_data_dict['rate_array_raw'], global_data_dict['rate_array_shape'])
        self.rate_time_array = raw_array_to_array(global_data_dict['rate_time_array_raw'],
                                                  global_data_dict['rate_time_array_shape'])

        self.ccy_matrix = raw_array_to_array(global_data_dict['ccy_matrix_array_raw'], global_data_dict['ccy_matrix_array_shape'])

        self.ref_info = {}  # = {'ref_data': {}, 'ref_factor': {'ref_ticker1': {'data_array':...,'time_array':...}, 'ref_ticker2': {...}}}
        for ref_type in ["ref_data", "ref_factor"]:
            self.ref_info[ref_type] = {}
            for data_type, data in global_data_dict[ref_type].items():
                self.ref_info[ref_type][data_type] = {}
                self.ref_info[ref_type][data_type]["data_array"] = raw_array_to_array(data['data_array_raw'],
                                                                                      data['data_array_shape'])
                self.ref_info[ref_type][data_type]["time_array"] = raw_array_to_array(data['time_array_raw'],
                                                                                      data['time_array_shape'])

        CASH = params['account']['cash']
        self.WINDOW_SIZE = params['algo']['window_time']
        self.REF_WINDOW_SIZE = params['algo']['ref_window_time']
        self.SAVE_DATA_SIZE = params['algo']['save_data_time']
        self.ORDER_TIME = params['time']['order_time']
        self.RATE_TIME = params['time']['rate_time']

        START_YEAR = params['start_year']
        START_MONTH = params['start_month']
        START_DAY = params['start_day']
        END_YEAR = params['end_year']
        END_MONTH = params['end_month']
        END_DAY = params['end_day']
        self.start_dt_ts = int(
            datetime.timestamp(pd.to_datetime(f"{START_YEAR}-{START_MONTH}-{START_DAY}", format='%Y-%m-%d', utc=True)))
        self.end_dt_ts = int(datetime.timestamp(pd.to_datetime(f"{END_YEAR}-{END_MONTH}-{END_DAY}", format='%Y-%m-%d', utc=True)))
        

        symbol_batch = config_dict['symbol_batch']
        ccy_list = config_dict['ccy_list']
        commission_dict = config_dict["commission"]
        lot_size_dict = config_dict["lot_size"]
        instrument_type_dict = config_dict["instrument_type"]
        symbol2exchange_id_dict = config_dict["symbol2exchange_id"]
        strategy_pool = config_dict["strategy_pool"]
        is_strategy_allocation = config_dict["is_strategy_allocation"]
        #self.hdf5_path = hdf5_path

        all_commission_array = np.array([commission_dict.get(symbol, params['account']['commission']) for symbol in symbol_batch],
                                        dtype="float64")
        all_lot_size_array = np.array([lot_size_dict[symbol] for symbol in symbol_batch])
        all_instrument_type_array = np.array([instrument_type_dict[symbol] for symbol in symbol_batch], dtype=int)
        self.symbol2exchange_id = np.array([symbol2exchange_id_dict[symbol] if not pd.isnull(symbol2exchange_id_dict[symbol]) else 0 for symbol in symbol_batch], dtype=int)
        self.exchange2calender = config_dict["exchange2calender"]
        self.is_trade_at_open = config_dict["is_trade_at_open"]
        self.is_save_data = config_dict["is_save_data"]
        # n_security = ccy_matrix.shape[1]  # shape (n_ccy, n_security)

        self.global_order_manager = self.ORDER_MANAGER(CASH,all_commission_array,self.ccy_matrix,all_instrument_type_array)
        self.super_strategy = self.SUPER_STRATEGY(symbol_batch, ccy_list)
        self.super_strategy.add_param(is_strategy_allocation=is_strategy_allocation)
        self.real_order_manager_dict = {}
        self.virtual_order_manager_dict = {}

        self.time_index_tracker_dict = get_time_index_tracker_dict(self.ref_info, self.rate_time_array, self.fx_data_time_array)

        # 从strategy pool生成各strategy，并加入到super strategy
        for id, strategy_dict in strategy_pool.items():
            strategy_symbols = sorted(strategy_dict["symbol"],
                                      key=lambda x: symbol_batch.index(x))  # strategy中的symbol按传入的所有symbol排序
            strategy_symbols_index = np.array([symbol_batch.index(x) for x in strategy_symbols])
            strategy_ccy_index = np.array([ccy_list.index(x) for x in ccy_list])
            strategy_ccy_matrix = self.ccy_matrix[:, strategy_symbols_index]  # shape (strategy_n_ccy, strategy_n_security)
            strategy_ccy_list = ccy_list
            strategy_commission = all_commission_array[strategy_symbols_index]  # universe of this strategy
            strategy_instrument_type_array = all_instrument_type_array[strategy_symbols_index]
            strategy_lot_size = all_lot_size_array[strategy_symbols_index]

            
            self.real_order_manager_dict[id] = self.ORDER_MANAGER(CASH,strategy_commission,strategy_ccy_matrix,strategy_instrument_type_array)
            self.virtual_order_manager_dict[id] = self.ORDER_MANAGER(CASH, strategy_commission, strategy_ccy_matrix,strategy_instrument_type_array)
            real_utility = StrategyUtility(strategy_dict["strategy_name"],0,0,list(strategy_dict["params"].values()),strategy_lot_size,strategy_symbols,
                                            strategy_ccy_list ,strategy_ccy_matrix)
            virtual_utility = StrategyUtility(strategy_dict["strategy_name"], 0, 0, list(strategy_dict["params"].values()),
                                              strategy_lot_size, strategy_symbols, strategy_ccy_list, strategy_ccy_matrix)

            STRATEGY_MODULE = importlib.import_module(strategy_dict["strategy_module"])
            strategy = getattr(STRATEGY_MODULE, strategy_dict["strategy_name"])
            real_strategy = strategy(self.real_order_manager_dict[id],real_utility)
            virtual_strategy = strategy(self.virtual_order_manager_dict[id], virtual_utility)

            self.super_strategy.add_strategy(real_strategy, virtual_strategy, id, strategy_symbols_index, strategy_ccy_index)

        universe_id_2_data_array = {}
        for (strategy_symbol_index_tuple, universe_id) in self.super_strategy.strategy_symbol_index_2_universe_id.items():
            universe_id_2_data_array[universe_id] = self.data_array[:, list(strategy_symbol_index_tuple), :] 

        self.universe_id_2_data_array = universe_id_2_data_array

    #@profile
    def start_backtest(self):
        # for convenience
        ref_info = self.ref_info
        data_array = self.data_array  # shape (n_data_points, n_symbols, 4)     constants.Data
        data_time_array = self.data_time_array  # shape (windows_size, 9)   constants.Time
        fx_data_array = self.fx_data_array
        fx_data_time_array = self.fx_data_time_array
        rate_array = self.rate_array  # shape (n_rate_points, n_symbols, 3)   constants.Rate
        rate_time_array = self.rate_time_array  # shape (n_rate_points, 9)  constants.Time
        universe_id_2_data_array = self.universe_id_2_data_array


        start_dt_ts = self.start_dt_ts
        end_dt_ts = self.end_dt_ts
        super_strategy = self.super_strategy
        global_order_manager = self.global_order_manager
        time_index_tracker_dict = self.time_index_tracker_dict
        WINDOW_SIZE = self.WINDOW_SIZE
        REF_WINDOW_SIZE = self.REF_WINDOW_SIZE
        SAVE_DATA_SIZE = self.SAVE_DATA_SIZE
        ORDER_TIME = self.ORDER_TIME
        RATE_TIME = self.RATE_TIME
        symbol2exchange_id = self.symbol2exchange_id
        exchange2calender = self.exchange2calender
        is_trade_at_open = self.is_trade_at_open


        is_tradable = None
        can_calc_rate = False
        prev_year_month_day = np.array([0, 0, 0], dtype=np.uint64)
        return_allocation_time = super_strategy.return_allocation_time(np.array(list(filter(lambda x: x[Time.TIMESTAMP.value]>=start_dt_ts,data_time_array))))
        record_ts = None
        # ..................................................................for loop begin.........................................................................................................
        for i in range(WINDOW_SIZE - 1, data_time_array.shape[0]):

            # prepare time data
            time_data, current_time_ts, current_year_month_day, current_time_value, current_time_weekday = \
                prepare_time_related_data(data_time_array, i, WINDOW_SIZE)
            current_time = time_data[-1]
            current_time_end_ts = int(current_time[Time.END_TIMESTAMP.value])
            # check time condition for backtest
            if current_time_ts < start_dt_ts or current_time_ts > end_dt_ts:
                continue

            # 更新每一个ref data的tracker的index 使index指向的时间刚好≤current_time_ts
            for tracker in time_index_tracker_dict.values():
                tracker.update_time_index(current_time_ts)
            # find at the moment ref data or factor using the timepoint found in above tracker.get_time_index function
            ref_window, ref_window_time = \
                get_ref_window_and_ref_window_time(ref_info, time_index_tracker_dict, REF_WINDOW_SIZE)

            # prepare window data
            window_data, window_past_data, window_past_fx_data, window_rate_data, window_rate_time_data = \
                prepare_window_related_data(data_array, fx_data_array, rate_array, rate_time_array,
                                            time_index_tracker_dict, i, WINDOW_SIZE, SAVE_DATA_SIZE)
            universe_id_2_window_data = dict(map(lambda x: (x[0],x[1][i - WINDOW_SIZE + 1: i + 1]) , universe_id_2_data_array.items()))
            # save window data in strategy utility for some risk calculation like var
            if self.is_save_data:
                super_strategy.save_window_data(window_past_data, window_past_fx_data, window_rate_data, window_rate_time_data)

            # prepare current data
            current_data, current_fx_data, current_rate = \
                prepare_current_data(data_array, fx_data_array, rate_array, time_index_tracker_dict, i)


            is_same_day = (current_year_month_day == prev_year_month_day).all()
            if not is_same_day:
                if not can_calc_rate:
                    can_calc_rate = True  # 每日重置can_calc_rate状态
                
                exchange_id2is_tradable = dict(map(lambda id: (id, np.all(exchange2calender[id]==current_year_month_day, axis=1).any() ), exchange2calender.keys()   ))
                exchange_id2is_tradable[0] = True
                temp_exchange_id2is_tradable_array = np.ones(max(exchange_id2is_tradable.keys())+1, dtype=bool)
                temp_exchange_id2is_tradable_array[np.array(list(exchange_id2is_tradable.keys()))] = np.array(list(exchange_id2is_tradable.values()))
                is_tradable = temp_exchange_id2is_tradable_array[symbol2exchange_id]
            
                super_strategy.reset_field_rollover_day(is_tradable)
                global_order_manager.reset_field_rollover_day(is_tradable)
                if len(return_allocation_time) > 0 and current_time_ts >= return_allocation_time[0][Time.TIMESTAMP.value]:
                    super_strategy.strategy_allocate(current_time)
                    super_strategy.capture(current_time_ts)
                    super_strategy.close_position(current_data[:,Data.CLOSE.value], current_fx_data)
                    return_allocation_time = return_allocation_time[1:]
                    if record_ts is not None:
                        super_strategy.forget(record_ts)
                        if super_strategy.is_strategy_allocation:
                            global_order_manager.forget(record_ts)
                    record_ts = current_time_ts
                    self.export_result()



            index_list = [(Data.OPEN.value,current_time_ts,False),(Data.CLOSE.value,current_time_end_ts,True)] if is_trade_at_open else [(Data.CLOSE.value,current_time_end_ts,True)]
            current_rate_last = current_rate[:, Rate.LAST.value]
            for index,ts,is_close in index_list:
                current_data_value = current_data[:, index]
                super_strategy.calculate_spot(current_fx_data, current_data_value)
                global_order_manager.calculate_spot(current_data_value,current_fx_data)
                # save current data and fx data for reserve order placing
                super_strategy.save_current_data(current_fx_data, current_data_value, current_rate_last,is_tradable)
                ## trade at close
                if current_time_value >= ORDER_TIME:
                    # send data to strategies
                    
                    virtual_order, real_order, combine_signal = super_strategy.on_data(universe_id_2_window_data,time_data, ref_window["ref_data"],ref_window_time["ref_data"],
                                                                                                                ref_window["ref_factor"],ref_window_time["ref_factor"],is_close)
                    # cross order
                    id2virtual_order_feedback,id2real_order_feedback = super_strategy.cross_order(virtual_order,real_order, current_data_value, current_fx_data)
                    if super_strategy.is_strategy_allocation:
                        global_order_manager.cross_target(combine_signal, current_data_value, current_fx_data)
                    super_strategy.rec_feedback(id2virtual_order_feedback, id2real_order_feedback)
                    if (is_tradable).any():
                        super_strategy.strategy_capture(ts)

                if (is_tradable).any():
                    super_strategy.order_manager_capture(ts)
                    if super_strategy.is_strategy_allocation:
                        global_order_manager.capture(ts)
                

            # calculate dividend 一个交易日最多一次
            if can_calc_rate and current_time_value >= RATE_TIME:
                super_strategy.calculate_rate(current_fx_data, current_data[:,Data.CLOSE.value], current_rate[:,Rate.LAST.value])
                if super_strategy.is_strategy_allocation:
                    global_order_manager.calculate_rate(current_rate[:,Rate.LAST.value], current_data[:,Data.CLOSE.value],current_fx_data)
                can_calc_rate = False

            prev_year_month_day = current_year_month_day
        # ..................................................................for loop end.........................................................................................................
        if record_ts is not None:
            super_strategy.forget(record_ts)
            if super_strategy.is_strategy_allocation:
                global_order_manager.forget(record_ts)
        self.export_result(is_end=True)
        self.export_observer_result()


    def export_observer_result(self):
        super_strategy = self.super_strategy
        if super_strategy.is_strategy_allocation:
            self.observer = Observer(universe_set_id=0, save_path=self.config_dict['observer_save_path'],
                                    file_name=self.config_dict['observer_file_name'],
                                    is_export_csv=self.config_dict['is_export_csv'], collect_freq_mode=TimeFreq['DAILY'])
            self.observer.export_other_to_csv([super_strategy.real_strategy_weight_list], ["real_weight"], super_strategy.timestamp_list)


    def export_result(self,is_end = False):
        real_order_manager_dict = self.real_order_manager_dict
        virtual_order_manager_dict = self.virtual_order_manager_dict
        super_strategy = self.super_strategy
        global_order_manager = self.global_order_manager
        is_strategy_allocation =  super_strategy.is_strategy_allocation

        strategy_types = ["virtual", "real","global"]  if is_strategy_allocation else ["virtual"]

        if not os.path.exists(self.config_dict['save_dir']):
            os.makedirs(self.config_dict['save_dir'])

        compression_dict = {'compression': 'gzip', 'compression_opts': 3}
        hdf5_lock = self.hdf5_lock

        with hdf5_lock:
            with h5py.File(self.config_dict['hdf5_path'], 'a') as f:
                for strategy_type in strategy_types:
                    for id in super_strategy.strategy_id:
                        strategy = None
                        # order_manager = None
                        # strategy_param = None
                        # universe = None
                        # signal_remark = None
                        if strategy_type == "virtual":
                            strategy = super_strategy.virtual_strategy_dict[id]
                            order_manager = strategy.order_manager
                            group_name = f"{strategy_type}_{id}"
                        elif strategy_type == "real":
                            strategy = super_strategy.real_strategy_dict[id]
                            order_manager = strategy.order_manager
                            group_name = f"{strategy_type}_{id}"

                        if strategy_type == "global":
                            #pass
                            order_manager = global_order_manager
                            strategy_param = [0]
                            universe = super_strategy.symbols
                            ccy_list = super_strategy.ccy_list
                            ccy_matrix = global_order_manager.ccy_matrix
                            group_name = f"{strategy_type}_0"
                        else:
                            strategy_param = strategy.strategy_utility.strategy_param
                            universe = strategy.strategy_utility.symbol_batch
                            ccy_list = strategy.strategy_utility.ccy_list
                            ccy_matrix = order_manager.ccy_matrix

                        group = f.get(f'{group_name}')
                        if group is None:
                            group = f.create_group(f'{group_name}')
                            group.attrs['id'] = id
                            group.attrs['type'] = strategy_type
                            group.attrs['universe'] = universe
                            group.attrs['strategy_param'] = str(self.config_dict['strategy_pool'][id]['params'])
                            group.attrs['param_keys'] = list(self.config_dict['strategy_pool'][id]['params'].keys())
                            group.attrs['cash'] = self.params['account']['cash']
                            group.attrs['order_time'] = self.params['time']['order_time']
                            group.attrs['rate_time'] = self.params['time']['rate_time']

                        group.create_dataset(f'timestamp_list_{self.hdf5_index}', data=order_manager.historial_time, **compression_dict)
                        group.create_dataset(f'pnl_array_{self.hdf5_index}', data=order_manager.historial_pnl, **compression_dict)
                        group.create_dataset(f'position_array_{self.hdf5_index}', data=order_manager.historial_position, **compression_dict)
                        group.create_dataset(f'cost_array_{self.hdf5_index}', data=order_manager.historial_commission_fee, **compression_dict)

                        if is_end:
                            if strategy_type != "global":
                                group.create_dataset(f'signal_timestamp_list', data=strategy.strategy_utility.signal_remark_ts, **compression_dict)
                                group.attrs['signal_remark_list'] = str(strategy.strategy_utility.signal_remark_list)

                            group.create_dataset(f'timestamp_list', data=hdf5_group_linking(group,'timestamp_list'), **compression_dict)
                            group.create_dataset(f'pnl_array', data=hdf5_group_linking(group,'pnl_array'), **compression_dict)
                            group.create_dataset(f'position_array', data=hdf5_group_linking(group,'position_array'), **compression_dict)
                            group.create_dataset(f'cost_array', data=hdf5_group_linking(group,'cost_array'), **compression_dict)
                            hdf5_group_delete_dataset(group,'timestamp_list')
                            hdf5_group_delete_dataset(group,'pnl_array')
                            hdf5_group_delete_dataset(group,'position_array')
                            hdf5_group_delete_dataset(group,'cost_array')
                            
                        if strategy_type == "global":
                            break
                        
        self.logger.info(f'saved record of strategy {super_strategy.strategy_id} to result.hdf5 at hdf5_index:{self.hdf5_index}')
        self.hdf5_index += 1
