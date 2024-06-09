import os
import time
import traceback
from datetime import datetime
from multiprocessing import Queue
from threading import Thread

import numpy as np
import pandas as pd
from pandas.tseries.offsets import BDay

from quantcycle.app.order_crosser.order_router import (BacktestOrderRouter,
                                                       OrderRouter)
from quantcycle.app.pms_manager.portfolio_manager import PorfolioManager
from quantcycle.app.pms_manager.super_portfolio_manager import \
    SuperPorfolioManager
from quantcycle.app.settle_manager.settlement import Settler
from quantcycle.app.signal_generator.super_strategy_manager import \
    SuperStrategyManager
from quantcycle.app.status_recorder.status_recorder import BaseStatusRecorder
from quantcycle.engine.runner import Runner
from quantcycle.utils import get_logger
from quantcycle.utils.production_constant import (
    CaptureType, DataType, OrderFeedback, PmsStatus, PmsTaskType,
    TerminationType, Time)
from quantcycle.utils.production_helper import (ThreadHander, WindowDataGenerator, WindowDataGeneratorNew)

is_debug = False

# control data flow, order feedback flow
class ProductionMaster():
    def __init__(self, config : dict ,streaming_data_queue : Queue,output_data_queue : Queue, output_data_resp_queue : Queue ,order_router : OrderRouter,super_porfolio_manager : SuperPorfolioManager,
                        super_strategy_manager : SuperStrategyManager, id2risk_manager,settler : Settler,symbols,ccys,ccy_matrix,need_secondary_data):
        self.pms_task_queue = Queue()
        self.pms_task_resp_queue = Queue()
        self.streaming_data_queue = streaming_data_queue
        self.output_data_queue = output_data_queue
        self.output_data_resp_queue = output_data_resp_queue
        self.order_router = order_router
        self.backtest_order_router = BacktestOrderRouter(commission_pool_path=self.order_router.commission_pool_path)

        self.order_router.update_order_feedback_queue(self.pms_task_queue)
        self.backtest_order_router.update_order_feedback_queue(self.pms_task_queue)
        self.order_router.update_symbol(symbols)
        self.backtest_order_router.update_symbol(symbols)

        self.threads = []
        self.threads_other = []
        self.runner = Runner(config,super_porfolio_manager,id2risk_manager,super_strategy_manager)
        self.settler = settler
        self.symbols = symbols
        self.ccys = ccys
        self.ccy_matrix = ccy_matrix
        self.current_data = np.zeros((len(symbols),4))
        self.current_fx_data = np.ones((len(symbols)))
        self.current_rate_data = np.zeros(len(symbols))
        self.current_raw_fx_data = np.ones((len(symbols),1))
        self.current_tradable_data = np.zeros(len(symbols))
        self.engine_name = config["engine"]["engine_name"]
        self.secondary_data= {}
        self.need_secondary_data = need_secondary_data
        self.have_secondary_data = False
        self.end_dt = datetime(config["end_year"],config["end_month"],config["end_day"])
        self.start_dt = datetime(config["start_year"],config["start_month"],config["start_day"])
        self.is_backtest_completed = False
        self.need_backtest = config["engine"].get("need_backtest",True)
        self.window_size = config["algo"]['window_size']["main"]
        self.ref_window_size = config["algo"]['window_size'].get("ref_data",1)
        # window data generator
        self.ref_data_window_gen_dict = {}
        self.ref_time_window_gen_dict = {}
        """ self.window_fx_data_gen = WindowDataGenerator(config["algo"]['window_size'].get("fxrate",1))
        self.window_fx_time_gen = WindowDataGenerator(config["algo"]['window_size'].get("fxrate",1))
        self.window_rate_data_gen = WindowDataGenerator(config["algo"]['window_size'].get("int",1))
        self.window_rate_time_gen = WindowDataGenerator(config["algo"]['window_size'].get("int",1)) """

        self.window_fx_data_gen = WindowDataGeneratorNew(self.window_size)
        self.window_fx_time_gen = WindowDataGeneratorNew(self.window_size)
        self.window_rate_data_gen = WindowDataGeneratorNew(self.window_size)
        self.window_rate_time_gen = WindowDataGeneratorNew(self.window_size)
        self.window_data_gen = WindowDataGeneratorNew(self.window_size)
        self.window_time_gen = WindowDataGeneratorNew(self.window_size)
        self.window_tradable_data_gen = WindowDataGeneratorNew(self.window_size)
        self.window_tradable_time_gen = WindowDataGeneratorNew(self.window_size)
        self.signal_remark_gen_dict = {}
        self.signal_remark_time_gen_dict = {}
        self.signal_remark_fields = None 
        self.ref_data_fields = {}

    def sync_holding(self,timestamp,include_price = False,reduce_from_cash = False):
        for id,value in self.runner.super_strategy_manager.id2universe.items():
            matrix_msg = self.order_router.check_holding(id,value,timestamp)
            if matrix_msg is not None:
                pointer = None
                task_type = None
                if reduce_from_cash:
                    task_type = PmsTaskType.sync_cash.value
                    current_data = matrix_msg[OrderFeedback.current_data.value]
                    transaction = matrix_msg[OrderFeedback.transaction.value]
                    current_fx_data = matrix_msg[OrderFeedback.current_fx_data.value]
                    cash = self.runner.super_porfolio_manager.id2pms[id].cash
                    pv = cash + np.sum(current_data*transaction*current_fx_data)
                    self.pms_task_queue.put((id,task_type,pv,timestamp))

                    task_type = PmsTaskType.order_feedback.value
                    pointer = matrix_msg
                else:
                    if include_price:
                        task_type = PmsTaskType.sync_current_data.value
                        current_data = matrix_msg[OrderFeedback.current_data.value]
                        self.pms_task_queue.put((id,task_type,current_data,timestamp))
                    task_type = PmsTaskType.sync_holding.value
                    pointer = matrix_msg[OrderFeedback.transaction.value]
                self.pms_task_queue.put((id,task_type,pointer,timestamp))

    def sync_cash(self,timestamp):
        for id,value in self.runner.super_strategy_manager.id2universe.items():
            cash = self.order_router.check_cash(timestamp)
            if cash is not None:
                task_type = PmsTaskType.sync_cash.value
                self.pms_task_queue.put((id,task_type,cash,timestamp))

    def settle_symbol(self,timestamp):
        self.settler.check_future_settle(timestamp[Time.YEAR.value],timestamp[Time.MONTH.value],timestamp[Time.DAY.value])
        
    def run_data_thread(self,items):
        if items == TerminationType.END.value:
            return
        data_type,data,ts = items
        if data_type == TerminationType.END_load_data.value:
            self.pms_task_queue.put((-1,TerminationType.END_load_data.value,0,0))
            return
        if data_type not in [e.value for e in DataType]:
            raise Exception("data_type error")


        dt = datetime(int(ts[Time.YEAR.value]), int(ts[Time.MONTH.value]), int(ts[Time.DAY.value]))

        # prepare task for pms calculation
        if data_type in [DataType.current_data.value, DataType.current_fx_data.value, DataType.current_rate_data.value,
                         DataType.current_tradable_data.value]:
            # data in dict format, use to recast it into np.array with self.symbols order
            # send pms task to pms thread before strategy receive data and fire order
            data_keys = list(data.keys())
            data_value = np.array(list(data.values()))
            ref_ticker = self.symbols
            symbol_index = np.array(list(map(lambda x: ref_ticker.index(x), data_keys)))
            if data_type == DataType.current_data.value:
                self.current_data[symbol_index] = data_value[:,:4]
                pms_task = (self.current_data,self.current_fx_data)
                self.pms_task_queue.put((-1, PmsTaskType.update_spot.value, pms_task, ts))
            elif data_type == DataType.current_fx_data.value:
                self.current_raw_fx_data[symbol_index] = data_value
                self.current_fx_data = self.current_raw_fx_data[:,0]
                pms_task = (self.current_data,self.current_fx_data)
                self.pms_task_queue.put((-1, PmsTaskType.update_spot.value, pms_task, ts))
            elif data_type == DataType.current_rate_data.value:
                self.current_rate_data[symbol_index] = data_value
                pms_task = (self.current_data,self.current_fx_data,self.current_rate_data)
                self.pms_task_queue.put((-1, PmsTaskType.update_dividends.value, pms_task, ts))
            elif data_type == DataType.current_tradable_data.value:
                self.current_tradable_data[symbol_index] = data_value
                pms_task = self.current_tradable_data.astype(np.int64)
                self.pms_task_queue.put((-1, PmsTaskType.reset_trade_status.value, pms_task, ts))
            # wait pms for resp
            id, task_type, ts = self.pms_task_resp_queue.get()
            get_logger.get_logger().info(f"receive pms task resp id:{id} task_type:{task_type} ts:{ts[3:6]}")
        else:
            # secondary_data is not needed for pms calculation
            pass

        # route data to strategy to fire order
        if data_type in [DataType.current_data.value, DataType.reload_current_data.value,DataType.reload_order_feedback.value]:
            if data_type == DataType.current_data.value:
                self.window_data_gen.receive_data(np.array(self.current_data))
                self.window_time_gen.receive_data(np.array(ts))
                self.order_router.update_data(self.current_data[:,3],self.current_fx_data)
                self.backtest_order_router.update_data(self.current_data[:,3],self.current_fx_data)
                get_logger.get_logger().info(f"succeed update window_data and order_router")
            else:
                get_logger.get_logger().info(f"not need update window_data and order_router")
                pass
            get_logger.get_logger().info(f"master hv enough secondary_data len:{len(self.secondary_data)}")

            ref_data_dict = {}
            ref_time_dict = {}
            static_info_dict = {}
            
            for key in self.ref_data_window_gen_dict.keys():
                if self.ref_data_window_gen_dict[key].ready:
                    if "ref_data_fields" not in self.ref_data_fields:
                        static_info_dict["ref_data_fields"] = self.ref_data_fields
                    ref_data_dict[key] = self.ref_data_window_gen_dict[key].return_window_data()
                    ref_time_dict[key] = self.ref_time_window_gen_dict[key].return_window_data()

            for key in self.signal_remark_gen_dict.keys():
                if self.signal_remark_gen_dict[key].ready:
                    if "signal_remark_fields" not in self.signal_remark_fields:
                        static_info_dict["signal_remark_fields"] = self.signal_remark_fields
                    ref_data_dict[key]  = self.signal_remark_gen_dict[key].return_window_data()
                    ref_time_dict[key] = self.signal_remark_time_gen_dict[key].return_window_data()

            window_data = self.window_data_gen.return_window_data()
            window_ts = self.window_time_gen.return_window_data()
            window_fx_data = self.window_fx_data_gen.return_window_data()
            window_fx_time = self.window_fx_time_gen.return_window_data()
            window_rate_data = self.window_rate_data_gen.return_window_data()
            window_rate_time = self.window_rate_time_gen.return_window_data()
            window_tradable_data = self.window_tradable_data_gen.return_window_data()
            window_tradable_time = self.window_tradable_time_gen.return_window_data()
            order_feedback = data if data_type in [DataType.reload_order_feedback.value] else None
            self.is_backtest_completed = dt > self.end_dt
            if (dt >= self.start_dt and self.need_backtest) or (not self.need_backtest and self.is_backtest_completed): # can't place order until reach start time in config
                roll_task_dict = self.settler.return_roll_task()
                id_order, backup_symbols_dict = self.runner.handle_current_data(window_data, window_ts,
                                                                                self.current_fx_data, window_fx_data,
                                                                                window_fx_time, window_rate_data,
                                                                                window_rate_time, window_tradable_data,
                                                                                window_tradable_time, self.secondary_data,
                                                                                ref_data_dict, ref_time_dict,
                                                                                self.need_secondary_data, self.order_router.ready,
                                                                                static_info_dict,order_feedback=order_feedback)
                temp_order_router = self.order_router if self.is_backtest_completed else self.backtest_order_router
                for str_id,order_dict in id_order.items():
                    roll_task = roll_task_dict.get(str_id,{})
                    symbols = self.runner.super_strategy_manager.id2universe[str_id]
                    symbols = [str(symbol) for symbol in symbols]
                    temp_order_router.receive_pending_order(order_dict, str_id,ts,symbols,roll_order_dict=roll_task,back_up_symbols=backup_symbols_dict.get(str_id,[]))
                    #temp_order_router.cross_order(order_dict, str_id,ts,symbols,roll_order_dict=roll_task,back_up_symbols=backup_symbols_dict.get(str_id,[]))
            else:
                get_logger.get_logger().info(f"dt:{dt} and need_backtest:{self.need_backtest}")
        elif data_type == DataType.fire_order.value:
            temp_order_router = self.order_router if self.is_backtest_completed else self.backtest_order_router
            temp_order_router.handle_pending_order()
        elif data_type == DataType.current_fx_data.value:
            self.window_fx_data_gen.receive_data(np.array(self.current_fx_data).reshape((-1, 1)))
            self.window_fx_time_gen.receive_data(np.array(ts))
            self.order_router.update_data(self.current_data[:,3],self.current_fx_data)
            self.backtest_order_router.update_data(self.current_data[:,3],self.current_fx_data)
        elif data_type == DataType.current_rate_data.value:
            self.window_rate_data_gen.receive_data(np.array(self.current_rate_data).reshape((-1, 1)))
            self.window_rate_time_gen.receive_data(np.array(ts))
        elif data_type == DataType.current_tradable_data.value:
            self.window_tradable_data_gen.receive_data(np.array(self.current_tradable_data).reshape((-1, 1)))
            self.window_tradable_time_gen.receive_data(np.array(ts))
        elif data_type == DataType.secondary_data.value:
            if data["strategy_id"] not in self.secondary_data:
                self.secondary_data[data["strategy_id"]] = {}
            for k,v in data.items():
                if k == "strategy_id":
                    continue
                elif k == "ID_symbol_map":
                    if k not in self.secondary_data[data["strategy_id"]]:
                        self.secondary_data[data["strategy_id"]][k] = {}
                    else:
                        get_logger.get_logger().info(f"org_ID_symbol_map:{self.secondary_data[data['strategy_id']][k].keys()}")
                        get_logger.get_logger().info(f"new_ID_symbol_map:{v.keys()}")
                    self.secondary_data[data["strategy_id"]][k].update(v)
                else:
                    self.secondary_data[data["strategy_id"]][k] = v
            self.have_secondary_data = len(self.secondary_data) > 0
            strategy_id = data["strategy_id"]
            get_logger.get_logger().info(f"current self.secondary_data:{self.secondary_data[strategy_id].keys()} strategy_id:{strategy_id} ts:{ts[Time.YEAR.value:Time.HOUR.value]}")
        elif data_type == DataType.ref_data.value:
            for symbol,value in data.items():
                if symbol == "fields":
                    self.ref_data_fields.update(value)
                    continue
                if symbol not in self.ref_data_window_gen_dict:
                    self.ref_data_window_gen_dict[symbol] = WindowDataGeneratorNew(self.ref_window_size)
                    self.ref_time_window_gen_dict[symbol] = WindowDataGeneratorNew(self.ref_window_size)
                window_ref_data = self.ref_data_window_gen_dict[symbol].receive_data(np.array(value))
                window_ref_time = self.ref_time_window_gen_dict[symbol].receive_data(np.array(ts))
                get_logger.get_logger().info(f"window_ref_data:{window_ref_data.shape if window_ref_data is not None else window_ref_data} \
                                window_ref_time:{window_ref_time.shape if window_ref_time is not None else window_ref_time} \
                                ts:{ts[Time.YEAR.value:Time.HOUR.value]}")
        elif data_type == DataType.signal_remark.value:
            for symbol,value in data.items():
                if symbol == "fields":
                    self.signal_remark_fields = value
                    continue
                if symbol not in self.signal_remark_gen_dict:
                    self.signal_remark_gen_dict[symbol] = WindowDataGeneratorNew(self.window_size)
                    self.signal_remark_time_gen_dict[symbol] = WindowDataGeneratorNew(self.window_size)
                signal_remark_data = self.signal_remark_gen_dict[symbol].receive_data(np.array(value))
                signal_remark_time = self.signal_remark_time_gen_dict[symbol].receive_data(np.array(ts))
            pass
        self.runner.save_status()

    def run_pms_task_thread(self,items):
        if items == TerminationType.END.value:
            return
        # receive pms_task
        id,task_type,pms_task,ts  = items
        if task_type == TerminationType.END_load_data.value:
            self.output_data_queue.put((TerminationType.END_load_data.value,0,0))
            return
        if task_type not in [e.value for e in PmsTaskType]:
            raise Exception("task_type error")
        date = datetime.utcfromtimestamp(ts[0]).date()
        self.runner.update_time(date,ts)

        if task_type == PmsTaskType.order_feedback.value:
            self.runner.handle_order_feedback(id,pms_task)
            self.runner.update_signal_remark(id)
            #self.streaming_data_queue.put((DataType.reload_order_feedback.value,pms_task, ts))
        elif task_type == PmsTaskType.reset_trade_status.value:
            current_tradable_status = pms_task
            self.runner.reset_trade_status(current_tradable_status)
            self.pms_task_resp_queue.put((id, task_type, ts))
        elif task_type == PmsTaskType.update_spot.value:
            current_data,current_fx_data = pms_task
            self.runner.update_spot(current_data[:,3],current_fx_data)
            self.pms_task_resp_queue.put((id, task_type, ts))
        elif task_type == PmsTaskType.update_dividends.value:
            current_data,current_fx_data,current_rate_data = pms_task
            self.runner.update_dividends(current_data[:,3],current_fx_data,current_rate_data)
            self.pms_task_resp_queue.put((id, task_type, ts))
        elif task_type == PmsTaskType.sync_holding.value:
            holding = np.copy(pms_task)
            self.runner.sync_holding(id,holding)
        elif task_type == PmsTaskType.sync_current_data.value:
            current_data = np.copy(pms_task)
            self.runner.sync_current_data(id,current_data)
        elif task_type == PmsTaskType.sync_cash.value:
            cash = pms_task
            cash_residual = self.runner.sync_cash(id,cash)
            get_logger.get_logger().info(f"cash_residual:{cash_residual} cash:{cash} id:{id} ts:{ts[Time.YEAR.value:Time.HOUR.value]}")

        self.runner.capture(ts[0], task_type)

        if task_type == PmsTaskType.order_feedback.value:
            self.output_data_queue.put((self.engine_name,id,ts))
            engine_name,id,ts = self.output_data_resp_queue.get()
            get_logger.get_logger().info(f"engine_name:{engine_name} id:{id} ts:{ts[Time.YEAR.value:Time.MINUTE.value]} receive resp from output_data_resp_queue")

    def run_regular_task_thread(self):
        get_logger.get_logger().info(f"I am still running")
        self.order_router.run_regular_task()
        self.backtest_order_router.run_regular_task()
        time.sleep(60)


    def run_consume_output_data_req_resp_thread(self,items):
        if items == TerminationType.END.value:
            return
        engine_name,id,ts = items
        if engine_name == TerminationType.END_load_data.value:
            get_logger.get_logger().info(f"{engine_name} is receive from queue")
        else:
            get_logger.get_logger().info(f"engine_name:{engine_name} id:{id} ts:{ts[Time.YEAR.value:Time.HOUR.value]} receive req from output_data_queue")
        self.output_data_resp_queue.put((engine_name,id,ts))
    

    def run(self):
        # run all thread for master
        f = self.run_data_thread
        thread_hander = ThreadHander(f = f,thread_name = f"{self.engine_name}|{self.__class__.__name__}|{f.__name__}",
                                        streaming_queue = self.streaming_data_queue,is_debug = is_debug,need_queue = True)
        self.threads.append(thread_hander)

        f = self.run_pms_task_thread
        thread_hander = ThreadHander(f = f,thread_name = f"{self.engine_name}|{self.__class__.__name__}|{f.__name__}",
                                        streaming_queue = self.pms_task_queue ,is_debug = is_debug,need_queue = True)
        self.threads.append(thread_hander)

        f = self.run_regular_task_thread
        thread_hander = ThreadHander(f = f,thread_name = f"{self.engine_name}|{self.__class__.__name__}|{f.__name__}",
                                        is_debug = is_debug,need_queue = False)
        self.threads.append(thread_hander)
        for t in self.threads:
            t.run()


    def join(self):
        for t in self.threads:
            t.join()


    def run_other_thread(self):
        f = self.run_consume_output_data_req_resp_thread
        thread_hander = ThreadHander(f = f,thread_name = f"{self.engine_name}|{self.__class__.__name__}|{f.__name__}",
                                        streaming_queue = self.output_data_queue ,is_debug = is_debug,need_queue = True)
        self.threads_other.append(thread_hander)
        for t in self.threads_other:
            t.run()

    def join_other_thread(self):
        for t in self.threads_other:
            t.join()
