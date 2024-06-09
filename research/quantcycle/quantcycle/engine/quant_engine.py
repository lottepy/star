import importlib
import os
import traceback
from collections import defaultdict
from datetime import datetime, timedelta
from functools import reduce
from multiprocessing import Process, Queue

import numpy as np
import pandas as pd
import pytz
from datamaster import dm_client
# BDay is business day, not birthday...
from pandas.tseries.offsets import BDay

from quantcycle.app.data_manager import DataDistributorSub, DataManager
from quantcycle.app.order_crosser.order_router import TestOrderRouter
from quantcycle.app.pms_manager.portfolio_manager import PorfolioManager
from quantcycle.app.pms_manager.super_portfolio_manager import \
    SuperPorfolioManager
from quantcycle.app.risk_manager.risk_manager import RiskManager
from quantcycle.app.settle_manager.settlement import Settler
from quantcycle.app.signal_generator.super_strategy_manager import \
    SuperStrategyManager
from quantcycle.app.status_recorder.status_recorder import BaseStatusRecorder
from quantcycle.engine.backtest_master import BacktestMaster
from quantcycle.engine.production_master import ProductionMaster
from quantcycle.utils import get_logger
from quantcycle.utils.production_constant import (DataType, InstrumentType,
                                                  TerminationType, Time)
from quantcycle.utils.production_helper import (TimeSeriesTracker,
                                                get_symbol2ccy_matrix,
                                                init_strategy_pms, MessageBox)
from quantcycle.utils.production_data_loader import DataLoader, find_highest_frequency


def quant_engine_function_handler(**base_kwarg):
    is_raise_error = base_kwarg.get("is_raise_error", True)

    def decorator(func):
        def wrapper(self, *arg, **kwarg):
            name = f"quant_engine_function_handler {self.__class__.__name__}:{self.engine_name} {func.__name__}"
            get_logger.get_logger().info(f"{name} run with arg:{arg} kwarg:{kwarg}")
            try:
                func(self, *arg, **kwarg)
            except Exception as error:
                exception_msg = f"{name}|Exception:{str(error)}"
                traceback_msg = str(traceback.format_exc())
                get_logger.get_logger().info(exception_msg)
                get_logger.get_logger().info(traceback_msg)
                MessageBox.send_msg(text = exception_msg)
                MessageBox.send_msg(text = traceback_msg)
                if is_raise_error:
                    raise Exception(f"{name} can't run")
            get_logger.get_logger().info(f"{name} end")
        return wrapper
    return decorator



# read config
# load diff app
# control production master which is a process
# can init multi master as process to accelerate
class QuantEngine():
    def __init__(self,output_data_queue = Queue(),output_data_resp_queue = Queue()):
        self.strategy_pool = {}
        self.config = {}
        self.symbol_feature_df = None
        self.streaming_data_queue = Queue()
        self.output_data_queue = output_data_queue
        self.output_data_resp_queue = output_data_resp_queue
        self.strategy_id2needed_field = {}
        self.engine_name = ""
        self.need_secondary_data = False
        self.need_signal_remark = False
        self.order_router = None
        self.production_process = None
        self.production_master = None
        self.production_process_other = None
        self.data_info = None
        self.ref_data_info = None
        self.start_dt = None
        self.end_dt = None
        self.window_size_dict = None
        self.instrument_type_to_symbols_dict = None
        self.risk_config = None
        self.need_backtest = True
        self.base_ccy = None
        self.secondary_data_config = None
        self.signal_remark_dict = None
        self.data_loader = None

    @quant_engine_function_handler()
    def load_config(self,config : dict ,strategy_pool_df : pd.DataFrame):
    
        self.config = config
        self.data_info = config["data"]
        self.ref_data_info = config.get("ref_data",{})
        for key,value in self.ref_data_info.items():
            if "StartDate" not in value:
                value["StartDate"] = { "Year": config["start_year"],"Month": config["start_month"],"Day": config["start_day"] }
            if "EndDate" not in value:
                value["EndDate"] = { "Year": config["end_year"],"Month": config["end_month"],"Day": config["end_day"] }

        # read from strategy pool df
        instrument_type_to_symbols_dict = defaultdict(set)    # main data {'FX': {..}, 'STOCKS': {..}}
        for index in strategy_pool_df.index:
            temp_strategy_row = dict(strategy_pool_df.loc[index])
            temp_strategy_row["params"] = eval(temp_strategy_row["params"])
            temp_symbol_dict = eval(temp_strategy_row["symbol"])
            temp_strategy_row["symbol"]=[]
            temp_strategy_row["secondary_ticker"]={}
            for sym_type, sym_list in temp_symbol_dict.items():
                if sym_type in ["FX", "STOCKS", "FUTURES"]:
                    instrument_type_to_symbols_dict[sym_type] = instrument_type_to_symbols_dict[sym_type].union(set(sym_list))
                    temp_strategy_row["symbol"] = list(set(sym_list +temp_strategy_row["symbol"]))
                else:
                    temp_strategy_row["secondary_ticker"][sym_type]=sym_list
                    self.need_secondary_data=True
            self.strategy_pool[index] = temp_strategy_row
            self.strategy_id2needed_field[index] = temp_strategy_row["secondary_ticker"]
        self.instrument_type_to_symbols_dict = instrument_type_to_symbols_dict.copy()
        
        self.secondary_data_config = config.get("secondary_data", {})
        self.signal_remark_dict = config.get("signal_remark", {})
        self.need_signal_remark = "load_name" in self.signal_remark_dict
        self.start_dt = datetime(config["start_year"],config["start_month"],config["start_day"],tzinfo=pytz.utc)
        #TODO: 回测部分pack了235959 to end time，实盘如果pack会有stock tests中两组fail(hk & mixed)
        self.end_dt = datetime(config["end_year"],config["end_month"],config["end_day"], tzinfo=pytz.utc) #23, 59, 59, 
        self.engine_name = config["engine"]["engine_name"]
        self.base_ccy = config["algo"]["base_ccy"]
        self.need_backtest = config["engine"].get("need_backtest",True)
        self.window_size_dict = config["algo"]['window_size']

        self.data_loader = DataLoader(self.data_info, self.base_ccy, self.instrument_type_to_symbols_dict,
                                      self.secondary_data_config, self.strategy_id2needed_field, self.ref_data_info)
        self.symbol_feature_df = self.data_loader.get_info_df()
        self.risk_config = config.get("risk_config",{
                                                "active":False,
                                                "Order_flow_limit":None,
                                                "Order_size_limit":0,
                                                "Trade_limit":0,
                                                "Order_cancel_limit":0,
                                                "time_interval":None,
                                                "security_weight_limit":None,
                                                "industry_limit":None,
                                                "black_list":None
                                                })
        
        # for field in config["data"]:
        #     self.frequency = config["data"][field]["Frequency"]
        #TODO self.frequency usage in calculating load time & load time >> calculating win (should use max freq)
        #TODO expecting multi asset test may be affected

        concat_symbols = []
        if len(instrument_type_to_symbols_dict) > 1:       # multi-assets
            freq_list = []
            for symbol_type, symbol_set in instrument_type_to_symbols_dict.items():
                concat_symbols.extend(symbol_set)
                freq_list.append(self.data_info[symbol_type]['Frequency'])
            self.frequency = find_highest_frequency(freq_list)
            symbol_type = 'MIX'
        else:
            symbol_type = list(instrument_type_to_symbols_dict.keys())[0]
            concat_symbols.extend(instrument_type_to_symbols_dict[symbol_type])
            self.frequency = self.data_info[symbol_type]['Frequency']

    @quant_engine_function_handler()
    def load_app(self,order_router = None):

        self.order_router = order_router if order_router is not None else TestOrderRouter()
        if not isinstance(self.order_router,TestOrderRouter) and self.need_backtest == True:
            raise Exception(f"{type(self.order_router)} can't hv backtest")
        self.order_router.update_data_loader(self.data_loader)
        status_recorder = BaseStatusRecorder()
        id2risk_manager = {}

        # init super_strategy_manager, super_porfolio_manager and settler to contain multi strategy and pms 
        symbol2instrument_type_dict = dict(map(lambda x: (x[0], InstrumentType[x[1].upper()].value), dict(self.symbol_feature_df['instrument_type']).items()))
        symbol2ccy_dict = dict(self.symbol_feature_df["base_ccy"])
        global_ccy_matrix = get_symbol2ccy_matrix(self.symbol_feature_df.index,symbol2ccy_dict)
        symbols = list(global_ccy_matrix.columns)
        ccys = list(global_ccy_matrix.index)
        super_strategy_manager = SuperStrategyManager(symbols)
        super_porfolio_manager = SuperPorfolioManager(symbols)
        settler = Settler()
        settler.add_data_loader(self.data_loader)
        for key,value in self.ref_data_info.items():
            if "Symbol" not in value:
                value["Symbol"] = symbols

        status_path = self.config.get("status_path",None)
        if status_path is not None and os.path.exists(status_path):
            status_path = None

        time_array = [self.start_dt.timestamp(),self.start_dt.timestamp(),self.start_dt.weekday(),self.start_dt.year,self.start_dt.month,self.start_dt.day,self.start_dt.hour,self.start_dt.minute,self.start_dt.second]
        #cash = order_router.check_cash(time_array) if self.config["engine"].get("is_sync_cash",False) else self.config["account"].get("cash",0)
        #if cash is None:
        cash = self.config["account"].get("cash",1)

        if cash <= 0:
            raise Exception(f"cash:{cash} is negative or zero")

        # cast strategy from strategy pool csv into super_strategy_manager and super_porfolio_manager
        for id,strategy_dict in self.strategy_pool.items():
            base_strategy,pms,ccy_matrix,symbol2instrument_type = init_strategy_pms(strategy_dict,cash,symbol2instrument_type_dict,symbol2ccy_dict,self.ref_data_info)
            temp_symbols = ccy_matrix.columns
            super_strategy_manager.add_strategy(id,base_strategy,temp_symbols)
            super_porfolio_manager.add_pms(id,pms,temp_symbols)
            settler.add(pms=pms,symbols=temp_symbols,symbol_types=symbol2instrument_type,id=id)
            id2risk_manager[id] = RiskManager(len(temp_symbols),0,0,np.zeros(len(temp_symbols)))

            id2risk_manager[id].update_setting(self.risk_config["active"], self.risk_config["Order_flow_limit"], self.risk_config["Order_size_limit"], self.risk_config["Trade_limit"], self.risk_config["Order_cancel_limit"],
                               self.risk_config["time_interval"], self.risk_config["security_weight_limit"], self.risk_config["industry_limit"], super_porfolio_manager.id2pms[id], self.risk_config["black_list"])

            # return pickle_dict to strategy
            if status_path is not None:
                pickle_dict = status_recorder.load(id,status_path)
                super_strategy_manager.load_status(id,pickle_dict)

        # init master for production or backtest
        self.production_master = ProductionMaster(self.config,self.streaming_data_queue,self.output_data_queue,self.output_data_resp_queue ,self.order_router,super_porfolio_manager,super_strategy_manager,
                                                    id2risk_manager,settler,symbols,ccys,global_ccy_matrix.values,self.need_secondary_data)
   


    @quant_engine_function_handler()
    def load_data(self):
        # 1、init data_manager
        # 2、load css and csd data
        # 3、init data_distributor
        
        #get_logger.get_logger().info(f"{self.__class__.__name__}:{self.engine_name} load_data")

        # download data
        temp_start_dt = self.start_dt
        temp_end_dt = self.end_dt

        temp_window_size = self.window_size_dict["main"]
        if not self.need_backtest:
            temp_window_size -= 1
            temp_start_dt = self.end_dt
            if temp_window_size == 0:
                get_logger.get_logger().info(f"engine_name:{self.engine_name} don't need need_backtest") 
                self.streaming_data_queue.put((TerminationType.END_load_data.value, 0, 0))
                get_logger.get_logger().info("END_load_data is put in queue")
                return

        load_start, load_end = self.data_loader.find_load_dt_range_by_req(temp_start_dt, temp_end_dt, temp_window_size,
                                                                          self.frequency)
        #TODO config end date 2020/12/28 win=100 no_backtest > load 99 data
        #TODO 2020/1/1 win=100 no_backtest > load 98 data only > 要改成load 99 data
        # 1. double check 问题存在 （复现问题）
        # 2. 找出解决方法

        ret_dict = self.data_loader.get_main_data(load_start.year, load_start.month, load_start.day,
                                                  load_end.year, load_end.month, load_end.day)
        current_data_array = ret_dict['current_data_array']
        current_fx_array = ret_dict['current_fx_array']
        current_rate_array = ret_dict['current_rate_array']
        ts = ret_dict['current_data_time_array']
        current_fx_time_array = ret_dict['current_fx_time_array']
        current_rate_time_array = ret_dict['current_rate_time_array']
        tradable_array = ret_dict['tradable_array']
        tradable_time_array = ret_dict['tradable_time_array']
        trading_ccy = ret_dict['trading_ccy']
        sym_list_all = list(ret_dict['fx_raw_sym'])
        
        # init time tracker for ref data
        time_series_tracker_dict = {}
        if len(self.ref_data_info) > 0:
            for key, value in self.data_loader.get_ref_data().items():
                ref_data = {"fields":{key:value['fields_arr']}}
                temp_time_array = [self.start_dt.timestamp(),self.start_dt.timestamp(),self.start_dt.weekday(),self.start_dt.year,
                        self.start_dt.month,self.start_dt.day,self.start_dt.hour,self.start_dt.minute,self.start_dt.second]
                if len(value['time_arr']) != 0:
                    temp_time_array = value['time_arr'][0]
                    time_series_tracker_dict[key] = TimeSeriesTracker(value['time_arr'][:,Time.TIMESTAMP.value],value['time_arr'],value['data_arr'])
                self.streaming_data_queue.put((DataType.ref_data.value, ref_data, temp_time_array))

                # time_tracker: starting from 9-8-08 and then 9-9-08 ... (matches config: 9-8)
        fx_time_series_tracker = TimeSeriesTracker(current_fx_time_array[:,Time.TIMESTAMP.value],current_fx_time_array,current_fx_array)

        current_rate_time_array_pointer = current_rate_time_array[:,Time.TIMESTAMP.value] if len(current_rate_time_array) > 0 else []
        rate_time_series_tracker = TimeSeriesTracker(current_rate_time_array_pointer,current_rate_time_array,current_rate_array)

        tradable_time_series_tracker = TimeSeriesTracker(tradable_time_array[:, Time.TIMESTAMP.value],
                                                         tradable_time_array, tradable_array)

        no_of_data = 0

        if ts is not None:
            get_logger.get_logger().info(f"ts shape is {ts.shape}")
        else:
            get_logger.get_logger().info(f"ts is None")
        for i in range(len(ts)):
            current_timestamp = ts[i]

            if current_timestamp[Time.TIMESTAMP.value] > temp_end_dt.timestamp() + 86400 - 1:
                continue
            no_of_data += 1
            # put ref data into queue
            for key,tracker in time_series_tracker_dict.items():
                tracker.compare_timestamp(current_timestamp[Time.TIMESTAMP.value])
                select_time_array,select_data_array = tracker.return_data()
                if select_time_array is not None:
                    for j in range(len(select_time_array)):
                        ref_data = np.array(select_data_array[j][:])
                        #ref_data = dict(zip([key], ref_data))
                        ref_data = { key : ref_data }
                        get_logger.get_logger().info((DataType.ref_data.value,key, ref_data, select_time_array[j][Time.YEAR.value:Time.MINUTE.value]))
                        self.streaming_data_queue.put((DataType.ref_data.value, ref_data, select_time_array[j]))

            # select tradable table & put it in queue
            tradable_time_series_tracker.compare_timestamp(current_timestamp[Time.TIMESTAMP.value])
            select_tradable_time, select_tradable_data = tradable_time_series_tracker.return_data()
            if select_tradable_time is not None:
                current_tradable_data = dict(zip(sym_list_all, select_tradable_data[-1, :, 0]))
                current_tradable_time = select_tradable_time[-1]
                get_logger.get_logger().info((DataType.current_tradable_data.value, current_tradable_data, current_tradable_time[Time.YEAR.value:Time.MINUTE.value]))
                self.streaming_data_queue.put((DataType.current_tradable_data.value, current_tradable_data, current_tradable_time))

            # select fx data & put it into queue
            fx_time_series_tracker.compare_timestamp(current_timestamp[Time.TIMESTAMP.value])
            select_current_fx_time,select_current_fx_data = fx_time_series_tracker.return_data()
            if select_current_fx_time is not None:
                current_fx_data = dict(zip(trading_ccy, select_current_fx_data[-1]))
                current_fx_time = select_current_fx_time[-1]
                get_logger.get_logger().info((DataType.current_fx_data.value, current_fx_data, current_fx_time[Time.YEAR.value:Time.MINUTE.value]))
                self.streaming_data_queue.put((DataType.current_fx_data.value, current_fx_data, current_fx_time))
            
            # put current data into queue
            current_data = np.array(current_data_array[i][:len(sym_list_all)])
            current_data = dict(zip(sym_list_all, current_data))
            get_logger.get_logger().info((DataType.current_data.value, current_data, ts[i][Time.YEAR.value:Time.MINUTE.value]))
            self.streaming_data_queue.put((DataType.current_data.value, current_data, ts[i]))
            get_logger.get_logger().info((DataType.fire_order.value, None, ts[i][Time.YEAR.value:Time.MINUTE.value]))
            self.streaming_data_queue.put((DataType.fire_order.value, None, ts[i]))

            # select rate data & put it into queue
            rate_time_series_tracker.compare_timestamp(current_timestamp[Time.TIMESTAMP.value])
            select_current_rate_time,select_current_rate_data = rate_time_series_tracker.return_data()
            if select_current_rate_time is not None:
                current_rate_data = dict(zip(sym_list_all, select_current_rate_data[-1][:,0]))
                current_rate_time = select_current_rate_time[-1]
                get_logger.get_logger().info((DataType.current_rate_data.value, current_rate_data, current_rate_time[Time.YEAR.value:Time.MINUTE.value]))
                self.streaming_data_queue.put((DataType.current_rate_data.value, current_rate_data, current_rate_time))

        get_logger.get_logger().info("END_load_data is put in queue")
        get_logger.get_logger().info(f"{self.__class__.__name__}:{self.engine_name} input number:{no_of_data} of data")
        self.streaming_data_queue.put((TerminationType.END_load_data.value, 0, 0))

    @quant_engine_function_handler()
    def reload_current_data(self, timepoint):
        time_str_format = "%Y%m%d%H%M%S" if len(timepoint) > 8 else "%Y%m%d"
        load_end = datetime.strptime(timepoint, time_str_format)
        time_array = [load_end.timestamp(),load_end.timestamp(),load_end.weekday(),load_end.year,load_end.month,load_end.day,load_end.hour,load_end.minute,load_end.second]
        get_logger.get_logger().info((DataType.reload_current_data.value, None, time_array))
        self.streaming_data_queue.put((DataType.reload_current_data.value, None, time_array))

    @quant_engine_function_handler()
    def load_current_data(self, timepoint):
        time_str_format = "%Y%m%d%H%M%S" if len(timepoint) > 8 else "%Y%m%d"
        load_end = datetime.strptime(timepoint, time_str_format)
        start_hour = load_end.hour if len(timepoint) > 8 else 0
        end_hour = load_end.hour if len(timepoint) > 8 else 23
        next_day = load_end
        ret_dict = self.data_loader.get_main_data(load_end.year, load_end.month, load_end.day,
                                                  next_day.year, next_day.month, next_day.day, start_hour = start_hour,end_hour = end_hour)
        current_data_array = ret_dict['current_data_array']
        ts = ret_dict['current_data_time_array']
        tradable_array = ret_dict['tradable_array']
        tradable_time_array = ret_dict["tradable_time_array"]
        sym_list_all = list(ret_dict['fx_raw_sym'])
        tradable_time_series_tracker = TimeSeriesTracker(tradable_time_array[:, Time.TIMESTAMP.value],
                                                         tradable_time_array, tradable_array)

        for i in range(len(ts)):
            current_time = ts[i]
            #if current_time[Time.TIMESTAMP.value] >= next_day.timestamp():
            #    continue
            tradable_time_series_tracker.compare_timestamp(current_time[Time.TIMESTAMP.value])
            select_tradable_time, select_tradable_data = tradable_time_series_tracker.return_data()

            current_data = np.array(current_data_array[i][:len(sym_list_all)])
            current_data = dict(zip(sym_list_all, current_data))

            current_tradable_data = np.array(select_tradable_data[-1, :, 0])
            current_tradable_data = dict(zip(sym_list_all, current_tradable_data))

            get_logger.get_logger().info((DataType.current_tradable_data.value, current_tradable_data, select_tradable_time[-1][Time.YEAR.value:Time.MINUTE.value]))
            self.streaming_data_queue.put((DataType.current_tradable_data.value, current_tradable_data, select_tradable_time[-1]))
            get_logger.get_logger().info((DataType.current_data.value, current_data, current_time[Time.YEAR.value:Time.MINUTE.value]))
            self.streaming_data_queue.put((DataType.current_data.value, current_data, current_time))

        get_logger.get_logger().info((DataType.fire_order.value, None, ts[0][Time.YEAR.value:Time.MINUTE.value]))
        self.streaming_data_queue.put((DataType.fire_order.value, None, ts[0]))
        if len(ts) == 0:
            raise Exception(f"there is no current data for timepoint:{timepoint}")

    @quant_engine_function_handler()
    def load_current_fx_data(self, timepoint):
        time_str_format = "%Y%m%d%H%M%S" if len(timepoint) > 8 else "%Y%m%d"
        load_end = datetime.strptime(timepoint, time_str_format)
        start_hour = load_end.hour if len(timepoint) > 8 else 0
        end_hour = load_end.hour if len(timepoint) > 8 else 23
        
        #try:
        ret_dict = self.data_loader.get_main_data(load_end.year, load_end.month, load_end.day,
                                                  load_end.year, load_end.month, load_end.day, start_hour = start_hour,end_hour = end_hour)
        current_fx_array = ret_dict['current_fx_array']
        current_fx_time_array = ret_dict['current_fx_time_array']
        symbol_quote_ccy = ret_dict['trading_ccy']

        for i in range(len(current_fx_time_array)):

            current_fx_data = np.array(current_fx_array[i][:])
            current_fx_data = dict(zip(symbol_quote_ccy, current_fx_data))

            get_logger.get_logger().info((DataType.current_fx_data.value, current_fx_data, current_fx_time_array[i][Time.YEAR.value:Time.MINUTE.value]))
            self.streaming_data_queue.put((DataType.current_fx_data.value, current_fx_data, current_fx_time_array[i]))
        if len(current_fx_time_array) == 0:
            raise Exception(f"there is no current fx data for timepoint:{timepoint}")

    @quant_engine_function_handler()
    def load_current_rate_data(self, timepoint):
        time_str_format = "%Y%m%d%H%M%S" if len(timepoint) > 8 else "%Y%m%d"
        load_start = datetime.strptime(timepoint, time_str_format)
        load_end = load_start + BDay(1)

        #try:
        ret_dict = self.data_loader.get_main_data(load_start.year, load_start.month, load_start.day,
                                                  load_end.year, load_end.month, load_end.day)
        current_rate_array = ret_dict['current_rate_array']
        current_rate_time_array = ret_dict['current_rate_time_array']
        sym_list_all = list(ret_dict['fx_raw_sym'])

        if len(current_rate_time_array) == 0:
            get_logger.get_logger().info(f"there is no current rate data for timepoint:{timepoint}")
            raise Exception(f"there is no current rate data for timepoint:{timepoint}")

        for i in range(1):
            current_rate_data = np.array(current_rate_array[i][:len(sym_list_all)][:, 0])
            current_rate_data = dict(zip(sym_list_all, current_rate_data))
            get_logger.get_logger().info((DataType.current_rate_data.value, current_rate_data, current_rate_time_array[i][Time.YEAR.value:Time.MINUTE.value]))
            self.streaming_data_queue.put((DataType.current_rate_data.value, current_rate_data, current_rate_time_array[i]))


    @quant_engine_function_handler()
    def load_secondary_data(self, strategy_id, timepoint, info_dict):
        
        """
        "main_data": window_data_array,
        " RSI_pnl": RSI_pnl_array,
        " MACD_pnl": MACD_pnl_array
        }  
        window_data_array -> np.array((time, symbol, field)) 
        RSI_pnl_array -> np.array((time, strategy_id, 1))
        MACD_pnl_array -> np.array((time, strategy_id, 1))
        """
        end_dt = datetime.utcfromtimestamp(timepoint[0]).date() + BDay(1)
        
        temp_secondary_data = self.data_loader.get_secondary_data(strategy_id, self.start_dt, end_dt, info_dict)

        get_logger.get_logger().info((DataType.secondary_data.value, temp_secondary_data.keys(),'strategy_id:',temp_secondary_data['strategy_id'],timepoint[Time.YEAR.value:Time.MINUTE.value]))
        self.streaming_data_queue.put((DataType.secondary_data.value, temp_secondary_data,timepoint))


    @quant_engine_function_handler()
    def load_ref_data(self, start_timepoint,end_timepoint):

        load_start = datetime.strptime(start_timepoint, "%Y%m%d")
        load_end = datetime.strptime(end_timepoint, "%Y%m%d") + BDay(1)
        if len(self.ref_data_info) == 0:
            get_logger.get_logger().info("config doesn't contain ref_data part")
            return

        start_dt_dict = {"Year": load_start.year,"Month": load_start.month,"Day": load_start.day} 
        end_dt_dict = {"Year": load_end.year,"Month": load_end.month,"Day": load_end.day} 
        for key, value in self.data_loader.get_ref_data(start_date=start_dt_dict, end_date=end_dt_dict).items():
            time_array = value['time_arr']
            data_array = value['data_arr']
            if time_array is not None:
                get_logger.get_logger().info(f"time_array shape is {time_array.shape} key:{key}")
            else:
                get_logger.get_logger().info(f"time_array is None key:{key}")
            for i in range(len(time_array)):
                ref_data = np.array(data_array[i][:])
                #ref_data = dict(zip([key], ref_data))
                ref_data = { key : ref_data }
                get_logger.get_logger().info((DataType.ref_data.value,key, ref_data, time_array[i][Time.YEAR.value:Time.MINUTE.value]))
                self.streaming_data_queue.put((DataType.ref_data.value, ref_data, time_array[i]))


    @quant_engine_function_handler()
    def load_signal_remark(self, timepoint ,info_dict):
        get_logger.get_logger().info(f"{self.__class__.__name__} {self.engine_name} load_signal_remark timepoint:{timepoint} info_dict:{info_dict}")
        remark_load_dir = info_dict["remark_load_dir"]
        remark_load_name = info_dict["remark_load_name"]
        load_start = datetime.strptime(timepoint, "%Y%m%d")
        load_end = datetime.strptime(timepoint, "%Y%m%d") + BDay(1)
        #try:
        data = self.data_loader.get_signal_remark(remark_load_dir, remark_load_name,
                                                  start_date=load_start, end_date=load_end+BDay(1))
        time_array = data['time_arr']
        data_array = data['data_arr']
        symbol_array = data['symbol_arr']
        fields = data["fields_arr"]
        if time_array is not None:
            get_logger.get_logger().info(f"time_array shape is {time_array.shape} symbol_array:{symbol_array} fields:{fields}")
        else:
            get_logger.get_logger().info(f"time_array is None symbol_array:{symbol_array} fields:{fields}")
        for i in range(len(time_array)):
            signal_remark = {"signal_remark":np.array(data_array[i][:]),"fields":fields}
            self.streaming_data_queue.put((DataType.signal_remark.value, signal_remark, time_array[i]))


    @quant_engine_function_handler()
    def load_holding(self,timepoint,include_price = False,reduce_from_cash = False):
        is_sync_holding = self.config["engine"].get("is_sync_holding",False)
        if is_sync_holding:
            time_str_format = "%Y%m%d%H%M%S" if len(timepoint) > 8 else "%Y%m%d"
            load_end = datetime.strptime(timepoint, time_str_format)
            time_array = [load_end.timestamp(),load_end.timestamp(),load_end.weekday(),load_end.year,load_end.month,load_end.day,load_end.hour,load_end.minute,load_end.second]
            self.production_master.sync_holding(time_array,include_price = include_price,reduce_from_cash = reduce_from_cash)
        else:
            get_logger.get_logger().info(f"{self.__class__.__name__} {self.engine_name} not load_holding timepoint:{timepoint} and is_sync_holding:{is_sync_holding}")

    @quant_engine_function_handler()  
    def load_cash(self,timepoint):
        is_sync_cash = self.config["engine"].get("is_sync_cash",False)
        if is_sync_cash:
            get_logger.get_logger().info(f"{self.__class__.__name__} {self.engine_name} load_cash timepoint:{timepoint}")
            time_str_format = "%Y%m%d%H%M%S" if len(timepoint) > 8 else "%Y%m%d"
            load_end = datetime.strptime(timepoint, time_str_format)
            time_array = [load_end.timestamp(),load_end.timestamp(),load_end.weekday(),load_end.year,load_end.month,load_end.day,load_end.hour,load_end.minute,load_end.second]
            #try:
            self.production_master.sync_cash(time_array)
        else:
            get_logger.get_logger().info(f"{self.__class__.__name__} {self.engine_name} not load_cash timepoint:{timepoint} and is_sync_cash:{is_sync_cash}")
        

    @quant_engine_function_handler()  
    def settle(self,timepoint):
        time_str_format = "%Y%m%d%H%M%S" if len(timepoint) > 8 else "%Y%m%d"
        load_end = datetime.strptime(timepoint, time_str_format)
        time_array = [load_end.timestamp(),load_end.timestamp(),load_end.weekday(),load_end.year,load_end.month,load_end.day,load_end.hour,load_end.minute,load_end.second]
        self.production_master.settle_symbol(time_array)



    def return_waiting_order_list(self):
        return self.order_router.return_waiting_order_list()

    def return_fail_order_list(self):
        return self.order_router.return_fail_order_list()

    @property
    def is_backtest_completed(self):
        return self.production_master.is_backtest_completed

    @property
    def have_secondary_data(self):
        return self.production_master.have_secondary_data

    def start_backtest(self):
        pass

    def start_production_engine(self):
        self.production_process = Process(target=self.production_master.run(),daemon = True,name = self.config["engine"]["engine_name"])
        self.production_process.start()

    def end_production_engine(self):
        self.production_master.threads[2].hard_kill()
        self.production_master.threads[0].slow_kill()
        self.production_master.threads[0].join()
        self.production_master.threads[1].slow_kill()
        self.production_master.threads[1].join()
        

    def wait_production_engine(self):
        self.production_master.join()

    def terminate_production_engine(self):
        self.production_process.terminate()

    def start_production_engine_other(self):
        self.production_process_other = Process(target=self.production_master.run_other_thread(),daemon = True,name = self.config["engine"]["engine_name"])
        self.production_process_other.start()

    def end_production_engine_other(self):
        self.production_master.threads_other[0].slow_kill()

    def wait_production_engine_other(self):
        self.production_master.join_other_thread()
