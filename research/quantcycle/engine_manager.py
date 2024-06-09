
import datetime

import os
import time
import traceback
from functools import reduce
from multiprocessing import Process, Queue
from quantcycle.utils.production_helper import Multi_event,ThreadHander,MessageBox
import time
import numpy as np
import pandas as pd
from pandas.tseries.offsets import BDay
from threading import Thread
from quantcycle.utils import get_logger
import importlib
from quantcycle.engine.quant_engine import QuantEngine
from quantcycle.utils.production_constant import (DataType, InstrumentType,
                                                  TerminationType, Time)
is_debug = False

class EngineInfo:

    def __init__(self,engine_name,hdf5_path,is_ready_dict,strategy_ids_dict,config_json,strategy_df,need_secondary_data,need_backtest,is_simple_backtest):
        self.engine_name = engine_name
        self.hdf5_path = hdf5_path
        self.is_ready_dict = is_ready_dict
        self.strategy_ids_dict = strategy_ids_dict
        self.config_json = config_json
        self.strategy_df = strategy_df
        self.need_secondary_data = need_secondary_data
        self.need_backtest = need_backtest
        self.is_simple_backtest = is_simple_backtest
        self.is_started = False
        self.is_loaded = False
        self.is_synced = False
        self.signal_remark_save_name = None
        self.signal_remark_path = None
        self.signal_remark_load_name = None
        self.need_signal_remark = False
        self.is_backtest_completed = False

    def update_signal_remark(self,signal_remark_save_name,signal_remark_path,signal_remark_load_name,need_signal_remark):
        self.signal_remark_save_name = signal_remark_save_name
        self.signal_remark_path = signal_remark_path
        self.signal_remark_load_name = signal_remark_load_name
        self.need_signal_remark = need_signal_remark

        


class EngineManager:

    def __init__(self):
        self.threads = []
        self.output_data_queue = Queue()
        self.id2engine= {}
        self.id2engine_info = {}
        self.id2output_data_resp_queue = {}
        self.runned = False
        self.load_data_event = None
        self.handle_current_data_event = None

    def add_engine(self,config : dict , strategy_df : pd.DataFrame):
        #checking for engine addition
        engine_name = config["engine"]["engine_name"]
        if engine_name in self.id2engine_info:
            raise Exception(f"engine_name:{engine_name} already added in  engine manager")
        
        abspath = os.path.dirname(__file__)
        config_json = config
        if "signal_remark" in config_json:
            config_json["signal_remark"]["save_dir"] = os.path.join(abspath, config_json["signal_remark"]["save_dir"])
            get_logger.get_logger().info(f"save_dir:{config_json['signal_remark']['save_dir']} is modified to abso path")
            signal_remark_save_path = os.path.join(config_json["signal_remark"]["save_dir"], config_json["signal_remark"]["save_name"])
            if signal_remark_save_path in [ os.path.join(info.signal_remark_path,info.signal_remark_save_name) for key,info in self.id2engine_info.items()]:
                raise Exception(f"engine_name:{engine_name} signal_remark_save_path repeated in engine manager")

        config_json["result_output"]["save_dir"] = os.path.join(abspath, config_json["result_output"]["save_dir"])
        get_logger.get_logger().info(f"save_dir:{config_json['result_output']['save_dir']} is modified to abso path")
        hdf5_path = os.path.join(config_json["result_output"]["save_dir"],config_json["result_output"]["save_name"])
        if hdf5_path in [ info.hdf5_path for key,info in self.id2engine_info.items()]:
            raise Exception(f"engine_name:{engine_name} hdf5_path already added in engine manager")

        #initiaition and engine addition
        output_data_resp_queue = Queue()
        quant_engine = QuantEngine(output_data_queue = self.output_data_queue,output_data_resp_queue=output_data_resp_queue)
        quant_engine.load_config(config_json,strategy_df)
        ORDER_ROUTER_MODULE = importlib.import_module(config_json["engine"].get("Order_Router_module","quantcycle.app.order_crosser.order_router"))
        c_path = config_json["account"].get("commission_pool_path", None)
        com_fee = config_json["account"].get("commission_fee", 0)
        order_router_config = config_json.get(config_json["engine"]["Order_Router_name"], {})
        order_router_config["currency"] = config["algo"]["base_ccy"]
        #OrderRouter
        Order_Router_name = config_json["engine"].get("Order_Router_name","TestOrderRouter")
        if Order_Router_name == "OrderRouter":
            raise Exception("OrderRouter can't be used, need to use TestOrderRouter")
        OrderRouter = getattr(ORDER_ROUTER_MODULE, config_json["engine"].get("Order_Router_name","TestOrderRouter"))(commission_fee=com_fee,commission_pool_path=c_path,
                                                                                                order_router_config=order_router_config)
        
        
        quant_engine.load_app(order_router=OrderRouter)
        strategy_ids = quant_engine.strategy_id2needed_field.keys()
        strategy_ids_dict = dict(zip(strategy_ids, np.zeros(len(strategy_ids)).astype(bool))) 
        keys = list(reduce(lambda x,y : set(x).union(set(y)),list(quant_engine.strategy_id2needed_field.values())))
        is_ready_dict = dict(zip(keys, np.zeros(len(keys)).astype(bool)))
        self.id2output_data_resp_queue[engine_name] = output_data_resp_queue
        self.id2engine[engine_name] = quant_engine
        self.id2engine_info[engine_name] = EngineInfo(engine_name,hdf5_path,is_ready_dict,strategy_ids_dict,config_json,strategy_df,quant_engine.need_secondary_data,
                                                        quant_engine.need_backtest,Order_Router_name == "TestOrderRouter")
        # add info back in id2engine_info
        if "signal_remark" in config_json:
            signal_remark_path = config_json["signal_remark"]["save_dir"]
            signal_remark_save_name = config_json["signal_remark"]["save_name"]
            signal_remark_load_name = config_json["signal_remark"].get("load_name",None)
            need_signal_remark = quant_engine.need_signal_remark 
            self.id2engine_info[engine_name].update_signal_remark(signal_remark_save_name,signal_remark_path,signal_remark_load_name,need_signal_remark)
        get_logger.get_logger().info(f"engine_name:{engine_name} is inited with config_json:{config_json} and strategy_df:{strategy_df}")
        

    def load_engine_data(self):
        """ if len(self.id2engine) == 0:
            return """
        engine_load_status_dict = dict([ (id,not engine_info.is_loaded) for id,engine_info in self.id2engine_info.items() ])
        engine_load_status = np.array(list(engine_load_status_dict.values()))
        engine_load_status = engine_load_status[engine_load_status == True]

        self.load_data_event = Multi_event(name="load_data_event")
        self.load_data_event.set_event(size = len(engine_load_status))
        for id,value in engine_load_status_dict.items():
            if not value:
                get_logger.get_logger().info(f"engine_name:{id} not need load data")
                continue
            else:
                get_logger.get_logger().info(f"engine_name:{id} load data")
                self.id2engine[id].load_data()
                self.id2engine_info[id].is_loaded = True
        return self.load_data_event

    def start_engine(self):
        for id,engine in self.id2engine.items():
            if not self.id2engine_info[id].is_started:
                get_logger.get_logger().info(f"engine_name:{id} start")
                engine.start_production_engine()
                self.id2engine_info[id].is_started = True
            else:
                get_logger.get_logger().info(f"engine_name:{id} hv already started")
        

    def kill_engine(self,engine_name = None):
        is_wait = True
        is_remove = True
        removed_engine = []
        for id,engine in self.id2engine.items():
            if engine_name is None or engine_name in id:
                get_logger.get_logger().info(f"engine_name:{id} kill")
                if self.id2engine_info[id].is_started:
                    engine.end_production_engine()
                    get_logger.get_logger().info(f"engine_name:{id} killed")
                    if is_wait:
                        get_logger.get_logger().info(f"engine_name:{id} wait")
                        engine.wait_production_engine()
                        get_logger.get_logger().info(f"engine_name:{id} terminate")
                        engine.terminate_production_engine()
                        get_logger.get_logger().info(f"engine_name:{id} terminated")
                        #get_logger.get_logger().info(f"remove_engine engine_name:{id}")
                    else:
                        get_logger.get_logger().info(f"engine_name:{id} not wait")
                else:
                    get_logger.get_logger().info(f"engine_name:{id} is not started")
                removed_engine.append(id)
                
            else:
                get_logger.get_logger().info(f"{id} doesn't match engine_name:{engine_name} for kill engine")
        get_logger.get_logger().info(f"{self.__class__.__name__} kill_engine")

        get_logger.get_logger().info(f"{self.__class__.__name__} removed_engine:{removed_engine}")
        if is_remove and is_wait:
            for id in removed_engine:
                self.remove_engine(id)

    def remove_engine(self,id = None):
        if id is None:
            get_logger.get_logger().info(f"{self.__class__.__name__} remove all engine")
            self.id2engine = {}
            self.id2engine_info = {}
            self.id2output_data_resp_queue = {}
        else:
            if id in self.id2engine_info:
                get_logger.get_logger().info(f"{self.__class__.__name__} remove engine engine_name:{id}")
                if id in self.id2engine:
                    del self.id2engine[id]
                if id in self.id2engine_info:
                    del self.id2engine_info[id]
                if id in self.id2output_data_resp_queue:
                    del self.id2output_data_resp_queue[id]

    def wait_engine(self):
        return

    def reload_current_data(self,timepoint,engine_name = None):
        if len(self.id2engine) == 0:
            return {}
        if engine_name is None or engine_name not in self.id2engine:
            raise Exception("engine_name can't be None or not exist for reload_current_data")
        engine = self.id2engine[engine_name]
        error_list = {}
        try:
            engine.reload_current_data(timepoint=timepoint)
        except Exception as error:
            error_list[id] = { "status": "fail","reason": str(error), "traceback": str(traceback.format_exc()) }
        return error_list

    def handle_current_data(self,timepoint,engine_name = None):
        if len(self.id2engine) == 0:
            return None,{}
        self.handle_current_data_event = Multi_event(name="handle_current_data_event")
        self.handle_current_data_event.set_event(size = len(self.id2engine))
        error_list = {}
        for id,engine in self.id2engine.items():
            if not self.id2engine_info[id].need_secondary_data and not self.id2engine_info[id].need_signal_remark:
                try:
                    if engine_name is None or engine_name in id:
                        get_logger.get_logger().info(f"engine_name:{id} load current data")
                        engine.load_current_data(timepoint=timepoint)
                    else:
                        get_logger.get_logger().info(f"{id} doesn't match engine_name:{engine_name} for load current data")
                except Exception as error:
                    error_list[id] = { "status": "fail","reason": str(error), "traceback": str(traceback.format_exc()) }
            else:
                if self.id2engine_info[id].need_secondary_data:
                    get_logger.get_logger().info(f"engine_name:{id} need secondary data")
                if self.id2engine_info[id].need_signal_remark:
                    get_logger.get_logger().info(f"engine_name:{id} need signal_remark")
        return self.handle_current_data_event,error_list

    def handle_current_fx_data(self,timepoint,engine_name = None):
        error_list = {}
        for id,engine in self.id2engine.items():
            try:
                if engine_name is None or engine_name in id:
                    get_logger.get_logger().info(f"engine_name:{id} load current fx data")
                    engine.load_current_fx_data(timepoint = timepoint)
                else:
                    get_logger.get_logger().info(f"{id} doesn't match engine_name:{engine_name} for load current fx data")
            except Exception as error:
                error_list[id] = { "status": "fail","reason": str(error), "traceback": str(traceback.format_exc()) }
        return error_list

    def handle_current_rate_data(self,timepoint,engine_name = None):
        error_list = {}
        for id,engine in self.id2engine.items():
            if self.id2engine_info[id].is_simple_backtest:
                try:
                    if engine_name is None or engine_name in id:
                        get_logger.get_logger().info(f"engine_name:{id} load current rate data")
                        engine.load_current_rate_data(timepoint = timepoint)
                    else:
                        get_logger.get_logger().info(f"{id} doesn't match engine_name:{engine_name} for load current rate data")
                except Exception as error:
                    error_list[id] = { "status": "fail","reason": str(error), "traceback": str(traceback.format_exc()) }
            else:
                get_logger.get_logger().info(f"{id} is not simple backtest")
        return error_list

    def handle_ref_data(self,start_timepoint,end_timepoint,engine_name = None):
        error_list = {}
        for id,engine in self.id2engine.items():
            get_logger.get_logger().info(f"engine_name:{id} load ref data")
            try:
                if engine_name is None or engine_name in id:
                    get_logger.get_logger().info(f"engine_name:{id} load ref data")
                    engine.load_ref_data(start_timepoint = start_timepoint,end_timepoint=end_timepoint)
                else:
                    get_logger.get_logger().info(f"{id} doesn't match engine_name:{engine_name} for load ref data")
            except Exception as error:
                error_list[id] = { "status": "fail","reason": str(error), "traceback": str(traceback.format_exc()) }
        return error_list

    def load_holding(self,timepoint,engine_name = None,include_price = False,reduce_from_cash = False,is_init = True):
        error_list = {}
        for id,engine in self.id2engine.items():
            try:
                if engine_name is None or engine_name in id:
                    if not is_init or not self.id2engine_info[id].is_synced:
                        get_logger.get_logger().info(f"engine_name:{id} load_holding is_init:{is_init}")
                        engine.load_holding(timepoint = timepoint,include_price=include_price,reduce_from_cash=reduce_from_cash)
                        self.id2engine_info[id].is_synced = True
                    else:
                        get_logger.get_logger().info(f"engine_name:{id} already loaded_holding")
                else:
                    get_logger.get_logger().info(f"{id} doesn't match engine_name:{engine_name} for load_holding")
            except Exception as error:
                error_list[id] = { "status": "fail","reason": str(error), "traceback": str(traceback.format_exc()) }
        return error_list

    def load_cash(self,timepoint,engine_name = None):
        error_list = {}
        for id,engine in self.id2engine.items():
            try:
                if engine_name is None or engine_name in id:
                    get_logger.get_logger().info(f"engine_name:{id} load_cash")
                    engine.load_cash(timepoint = timepoint)
                else:
                    get_logger.get_logger().info(f"{id} doesn't match engine_name:{engine_name} for load_cash")
            except Exception as error:
                error_list[id] = { "status": "fail","reason": str(error), "traceback": str(traceback.format_exc()) }
        return error_list

    def load_settle(self,timepoint,engine_name = None):
        error_list = {}
        for id,engine in self.id2engine.items():
            try:
                if engine_name is None or engine_name in id:
                    get_logger.get_logger().info(f"engine_name:{id} load_settle")
                    engine.settle(timepoint = timepoint)
                else:
                    get_logger.get_logger().info(f"{id} doesn't match engine_name:{engine_name} for load_settle")
            except Exception as error:
                error_list[id] = { "status": "fail","reason": str(error), "traceback": str(traceback.format_exc()) }
        return error_list

        
    def run_secondary_thread(self,items):
        if items == TerminationType.END.value:
            return
        engine_name,strategy_id,ts = items
        if engine_name == TerminationType.END_load_data.value:
            self.load_data_event.release()
            get_logger.get_logger().info("END_load_data is receive from queue")
            return
        dt = datetime.datetime.utcfromtimestamp(ts[0]).strftime('%Y%m%d')
        is_backtest_completed = self.id2engine[engine_name].is_backtest_completed
        get_logger.get_logger().info(f"trigger engine:{engine_name} ts:{ts[Time.YEAR.value:Time.MINUTE.value]} dt:{dt} strategy_id:{strategy_id} is_backtest_completed:{is_backtest_completed} is received for output data queue")
        self.id2engine_info[engine_name].strategy_ids_dict[strategy_id] = is_backtest_completed

        strategy_ids_dict = self.id2engine_info[engine_name].strategy_ids_dict
        if np.array(list(strategy_ids_dict.values())).all():
            get_logger.get_logger().info(f"trigger engine:{engine_name} with strategy_ids:{strategy_ids_dict} is ready for next step")
            for id,engine_info in self.id2engine_info.items():
                quant_engine = self.id2engine[id]
                need_signal_remark = engine_info.need_signal_remark
                need_secondary_data = engine_info.need_secondary_data
                hv_signal_remark = False
                hv_secondary_data = False

                if need_signal_remark:
                    trigger_signal_remark_save_name = self.id2engine_info[engine_name].signal_remark_save_name
                    signal_remark_load_name = engine_info.signal_remark_load_name
                    if signal_remark_load_name == trigger_signal_remark_save_name:
                        signal_remark_path = self.id2engine_info[engine_name].signal_remark_path
                        info_dict = {"remark_load_dir":signal_remark_path,"remark_load_name":signal_remark_load_name}
                        get_logger.get_logger().info(f"trigger engine:{engine_name} push engine:{id} with info_dict:{info_dict} to load signal_remark")
                        quant_engine.load_signal_remark(dt ,info_dict)
                        hv_signal_remark = True
                    else:
                        get_logger.get_logger().info(f"trigger engine:{engine_name} push engine:{id} not load signal_remark")
                else:
                    hv_signal_remark = True
                    get_logger.get_logger().info(f"engine:{id} not need signal_remark")

                if need_secondary_data:
                    is_ready_dict = engine_info.is_ready_dict
                    strategy_ids = engine_info.strategy_ids_dict.keys()
                    if engine_name in list(is_ready_dict.keys()):
                        #info_dict = {"RSI" :{"field":...,"hdf5_path":...,"strategy_id_list":...}}
                        info_dict = dict(map(lambda x: (x,{"hdf5_path":self.id2engine_info[x].hdf5_path} ),[engine_name]))
                        for strategy_id in strategy_ids:
                            get_logger.get_logger().info(f"trigger engine:{engine_name} push engine:{id} strategy_id:{strategy_id} with info_dict:{info_dict} to load secondary data ")
                            quant_engine.load_secondary_data(strategy_id, ts, info_dict)
                        is_ready_dict[engine_name] = True
                        if np.array(list(is_ready_dict.values())).all():
                            get_logger.get_logger().info(f"engine:{id} with fields:{is_ready_dict} is ready to load current data")
                            hv_secondary_data = True
                            #quant_engine.load_current_data(dt)
                            engine_info.is_ready_dict = dict([(x,False) for x in is_ready_dict.keys()])
                        else:
                            get_logger.get_logger().info(f"engine:{id} with fields:{is_ready_dict} is not ready to load current data, wait for next trigger")
                    else:
                        get_logger.get_logger().info(f"trigger engine:{engine_name} can't trigger engine:{id} to load secondary data becasue {id}'fields:{is_ready_dict} don't contain that engine_name")
                else:
                    hv_secondary_data = True
                    get_logger.get_logger().info(f"engine:{id} not need secondary_data")

                if (need_signal_remark or need_secondary_data) and hv_signal_remark and hv_secondary_data:
                    quant_engine.load_current_data(dt)
                else:
                    if need_signal_remark or need_secondary_data:
                        get_logger.get_logger().info(f"engine:{id} hv_signal_remark:{hv_signal_remark} hv_secondary_data:{hv_secondary_data} not load_current_data dt:{dt}")
                    else:
                        get_logger.get_logger().info(f"engine:{id} need_signal_remark:{need_signal_remark} need_secondary_data:{need_secondary_data} loaded_current_data dt:{dt}")


            self.id2engine_info[engine_name].strategy_ids_dict = dict(map(lambda x: (x,False),self.id2engine_info[engine_name].strategy_ids_dict.keys()))
            self.handle_current_data_event.release()
        else:
            get_logger.get_logger().info(f"trigger engine:{engine_name} with strategy_ids:{strategy_ids_dict} is not ready for next step, wait for next trigger")
        self.id2output_data_resp_queue[engine_name].put((engine_name,strategy_id,ts))


    def run(self):
        # run all thread for master
        if not self.runned:
            is_debug = False
            f = self.run_secondary_thread
            thread_hander = ThreadHander(f = f,thread_name = f"{self.__class__.__name__}|{f.__name__}",
                                            streaming_queue = self.output_data_queue,is_debug = is_debug,need_queue = True)
            self.threads.append(thread_hander)
            for t in self.threads:
                t.run()
            get_logger.get_logger().info(f"{self.__class__.__name__} run")
            MessageBox.send_msg(f"{self.__class__.__name__} run")
            self.runned = True
        else:
            get_logger.get_logger().info(f"{self.__class__.__name__} already run")
            MessageBox.send_msg(f"{self.__class__.__name__} already run")

    def kill(self):
        for t in self.threads:
            t.slow_kill()

    def wait(self):
        for t in self.threads:
            t.join()
