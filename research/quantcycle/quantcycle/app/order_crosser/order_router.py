import datetime
import json
import os
import time
import uuid
from collections import defaultdict
from multiprocessing import Event, Queue

import numpy as np
import pandas as pd
import pysftp
from pandas.tseries.offsets import BDay

from quantcycle.utils import get_logger
from quantcycle.utils.production_constant import (OrderFeedback, OrderStatus,
                                                  Time)
from quantcycle.utils.production_data_loader import DataLoader
from quantcycle.utils.production_helper import (GetFXSnapshot,
                                                GetStocksSnapshot, GetSubTask,
                                                GetTask, MessageBox,
                                                checkBalance, checkHolding,
                                                checkSubAccountBalance,
                                                checkSubAccountHolding,
                                                doSubTask, generateSubTasks,
                                                postPortfolioTask, postTask,
                                                reviewSingleTask)
from quantcycle.utils.production_mapping import (ccy_usd_map,
                                                 instrument_type_map)

cnopts = pysftp.CnOpts()
cnopts.hostkeys = None




class OrderRouter():
    def __init__(self,commission_fee = 0, commission_pool_path = None,order_router_config = {}):
        self.pending_order_list = {}
        self.waiting_order_list = []
        self.fail_order_list = []
        self.order_feedback_queue = None
        self.execution_current_data = None
        self.execution_current_fx_data = None
        self.ready = False
        self.commission_fee=commission_fee
        self.symbols = None
        self.data_loader = None
        self.commission_pool_path=commission_pool_path
        self.currency = order_router_config.get("currency",None)
        self.commission = pd.read_csv(self.commission_pool_path,index_col=0) if self.commission_pool_path is not None else 0

    def update_symbol(self,symbols):
        self.symbols = symbols

    def update_data_loader(self,data_loader : DataLoader):
        self.data_loader = data_loader

    def update_order_feedback_queue(self, order_feedback_queue: Queue):
        get_logger.get_logger().info(f"{self.__class__.__name__} update_order_feedback_queue")
        self.order_feedback_queue = order_feedback_queue


    def update_data(self,execution_current_data , execution_current_fx_data):
        get_logger.get_logger().info(f"{self.__class__.__name__} update_data")
        self.execution_current_data = execution_current_data
        self.execution_current_fx_data = execution_current_fx_data
        self.ready = True

    def receive_pending_order(self, order_dict, strategy_id,timestamps,symbols,roll_order_dict = {},back_up_symbols=[]):
        get_logger.get_logger().info(f"{self.__class__.__name__} receive_pending_order dt:{timestamps[3:6]} strategy_id:{strategy_id} order_dict:{order_dict} roll_order_dict:{roll_order_dict} back_up_symbols:{back_up_symbols}" )
        MessageBox.send_msg(text=f"{self.__class__.__name__} receive_pending_order dt:{timestamps[3:6]} strategy_id:{strategy_id} order_dict:{order_dict} roll_order_dict:{roll_order_dict} back_up_symbols:{back_up_symbols}")
        if strategy_id not in self.pending_order_list:
            self.pending_order_list[strategy_id] = (order_dict,timestamps,symbols,roll_order_dict,back_up_symbols)
        else:
            saved_order_dict,saved_timestamps,saved_symbols,saved_roll_order_dict,saved_back_up_symbols = self.pending_order_list[strategy_id]
            if set(saved_symbols) != set(symbols):
                raise Exception("pending symbols do not match")
            temp_order_dict = saved_order_dict.copy()
            for k,v in order_dict.items():
                temp_order_dict[k] = temp_order_dict.get(k,0) + v
            temp_roll_order_dict = saved_roll_order_dict.copy()
            for k,v in roll_order_dict.items():
                temp_roll_order_dict[k] = temp_roll_order_dict.get(k,0) + v
            temp_back_up_symbols = list(set(saved_back_up_symbols.copy()).union(set(back_up_symbols)))
            self.pending_order_list[strategy_id] = (temp_order_dict,timestamps,symbols,temp_roll_order_dict,temp_back_up_symbols)

    def handle_pending_order(self):
        for strategy_id in self.pending_order_list:
            saved_order_dict,saved_timestamps,saved_symbols,saved_roll_order_dict,saved_back_up_symbols = self.pending_order_list[strategy_id]
            get_logger.get_logger().info(f"{self.__class__.__name__} receive_pending_order dt:{saved_timestamps[3:6]} strategy_id:{strategy_id} order_dict:{saved_order_dict} back_up_symbols:{saved_back_up_symbols}" )
            MessageBox.send_msg(text=f"{self.__class__.__name__} receive_pending_order dt:{saved_timestamps[3:6]} strategy_id:{strategy_id} order_dict:{saved_order_dict} back_up_symbols:{saved_back_up_symbols}")
            self.cross_order(saved_order_dict,strategy_id,saved_timestamps,saved_symbols,roll_order_dict=saved_roll_order_dict,back_up_symbols=saved_back_up_symbols)
        self.pending_order_list.clear()


    def cross_order(self, order_dict, strategy_id,timestamps,symbols,roll_order_dict = {},back_up_symbols=[]):
        get_logger.get_logger().info(f"{self.__class__.__name__} cross_order dt:{timestamps[3:6]} strategy_id:{strategy_id}")
        order = np.array([order_dict.get(symbol,0) for symbol in symbols])
        symbol_index = np.array([self.symbols.index(symbol) for symbol in symbols])

        matrix_msg = np.zeros((6,len(symbols)))
        matrix_msg[OrderFeedback.transaction.value] = np.array(order)
        matrix_msg[OrderFeedback.current_data.value] = self.execution_current_data[symbol_index]
        matrix_msg[OrderFeedback.current_fx_data.value] = self.execution_current_fx_data[symbol_index]
        if self.commission_pool_path != None:
            com=self.commission.loc[symbols].values.flatten()
            matrix_msg[OrderFeedback.commission_fee.value] = np.abs(order) * self.execution_current_data[symbol_index] * \
                                                             self.execution_current_fx_data[symbol_index] \
                                                             * com[symbol_index]
        else:

            matrix_msg[OrderFeedback.commission_fee.value] = np.abs(order) * self.execution_current_data[symbol_index] * self.execution_current_fx_data[symbol_index] \
                                                                * self.commission_fee
        nan_id = np.isnan(matrix_msg[OrderFeedback.commission_fee.value])
        matrix_msg[OrderFeedback.commission_fee.value][nan_id] = 0

        matrix_msg[OrderFeedback.order_status.value] = np.array([OrderStatus.FILLED.value for i in range(len(order))])
        matrix_msg[OrderFeedback.timestamps.value] = np.array([timestamps[0] for i in range(len(order))])
        task_type = "order_feedback"
        self.order_feedback_queue.put((strategy_id,task_type,matrix_msg,timestamps))

    def get_order_feedback(self):
        get_logger.get_logger().info(f"{self.__class__.__name__} get_order_feedback")
        return self.order_feedback_queue.get()

    def run_regular_task(self):
        pass

    def return_orderfeedback(self,symbols,timestamp,result_dict,roll_order_dict,instrument_id_symbol_dict):
        
        for key,value in result_dict.items():
            result_dict[key]["volumeFilled"] *= (1 if result_dict[key]["direction"]=="BUY" else -1)
        for symbol,temp_order in roll_order_dict.get('orders',{}).items():
            if symbol in result_dict:
                result_dict[symbol]["volumeFilled"] -= temp_order
            else:
                result_dict[symbol] = {}
                result_dict[symbol]["volumeFilled"] = - temp_order
                result_dict[symbol]["priceFilled"] = np.nan
                result_dict[symbol]["commission"] = 0
                #result_dict[symbol]["direction"] = "BUY"
        
        symbol2dividends = {}
        for symbol,ticker_map in roll_order_dict.get('main2ind_ticker',{}).items():
            before_ticker = ticker_map["before_ticker"]
            after_ticker = ticker_map["after_ticker"]
            before_ticker_price = result_dict.get(before_ticker,{"priceFilled":np.nan})["priceFilled"]
            after_ticker_price = result_dict.get(after_ticker,{"priceFilled":np.nan})["priceFilled"]
            dividends = (after_ticker_price - before_ticker_price)
            symbol2dividends[symbol] = dividends if not np.isnan(dividends) else 0
        get_logger.get_logger().info(f"{self.__class__.__name__} symbol2dividends:{symbol2dividends}")
        MessageBox.send_msg(text=f"{self.__class__.__name__} symbol2dividends:{symbol2dividends}")

        temp_result = {"volumeFilled":0 ,"priceFilled":np.nan ,"commission":0 ,"direction":"BUY"}
        result_dict = dict([(symbol,result_dict.get(instrument_id_symbol_dict[symbol],temp_result.copy())) for symbol in symbols])

        matrix_msg = np.zeros((6,len(result_dict)))
        matrix_msg[OrderFeedback.transaction.value] = np.float64(list(map(lambda x:result_dict[x]["volumeFilled"],symbols)))
        matrix_msg[OrderFeedback.current_data.value] = np.float64(list(map(lambda x:result_dict[x]["priceFilled"],symbols)))
        matrix_msg[OrderFeedback.current_fx_data.value] = np.float64(list(map(lambda x:1,symbols)))
        matrix_msg[OrderFeedback.commission_fee.value] = np.float64(list(map(lambda x:result_dict[x]["commission"] + symbol2dividends.get(x,0),symbols)))
        matrix_msg[OrderFeedback.order_status.value] = np.ones(len(symbols)) * OrderStatus.FILLED.value
        matrix_msg[OrderFeedback.timestamps.value] = np.ones(len(symbols)) * timestamp
        return matrix_msg


    def check_holding(self,strategy_id,symbols,timestamps):
        return None

    def return_holding_msg(self,strategy_id,symbols,timestamps,holding_position_dict,holding_price_dict,ccy_dict):
        trading_symbol_dict,rev_trading_symbol_dict,symbol_type_dict = self.translate_symbol()
        ccy_list = list(set( ccy_dict.values() ))
        ccy_fx_dict = dict(map(lambda x:(x,GetFXSnapshot(trading_ccy = x,base_ccy = self.currency)),ccy_list))
        get_logger.get_logger().info(f"{self.__class__.__name__} ccy_fx_dict:{ccy_fx_dict}")
        fx_dict = dict([(key,ccy_fx_dict[ccy]) for key,ccy in ccy_dict.items()])

        holding_position_dict = dict([( (symbol,holding_position_dict.get(trading_symbol_dict[symbol],0))  ) for symbol in symbols])
        holding_price_dict = dict([( (symbol,holding_price_dict.get(trading_symbol_dict[symbol],0))  ) for symbol in symbols])
        fx_dict = dict([( (symbol,fx_dict.get(trading_symbol_dict[symbol],1))  ) for symbol in symbols])

        matrix_msg = np.zeros((6,len(symbols)))
        matrix_msg[OrderFeedback.transaction.value] = np.float64(list(map(lambda x:holding_position_dict.get(x,0),symbols)))
        matrix_msg[OrderFeedback.current_data.value] = np.float64(list(map(lambda x:holding_price_dict.get(x,0),symbols)))
        matrix_msg[OrderFeedback.current_fx_data.value] = np.float64(list(map(lambda x:fx_dict.get(x,1),symbols)))
        matrix_msg[OrderFeedback.commission_fee.value] = np.float64(list(map(lambda x:0,symbols)))
        matrix_msg[OrderFeedback.order_status.value] = np.ones(len(symbols)) * OrderStatus.FILLED.value
        matrix_msg[OrderFeedback.timestamps.value] = np.ones(len(symbols)) * timestamps[Time.TIMESTAMP.value]
        #task_type = "order_feedback"
        #self.order_feedback_queue.put((strategy_id,task_type,matrix_msg,timestamps))
        return matrix_msg

    def check_cash(self,timestamps):
        return None

    def return_cash_msg(self,timestamps,balance_dict):
        ccy_fx_dict = dict(map(lambda x:(x[0],(GetFXSnapshot(trading_ccy = x[0] ,base_ccy = self.currency) if x[0] in ccy_usd_map else 0) ),balance_dict.items()))
        ccy_fx_dict["USD"] = 1
        balance_in_base_ccy = sum([balance * ccy_fx_dict[ccy] for ccy,balance in balance_dict.items()])
        return balance_in_base_ccy

    def return_waiting_order_list(self):
        return self.waiting_order_list.copy()

    def return_fail_order_list(self):
        return self.fail_order_list.copy()

    def translate_symbol(self,is_before = False,timestamps = None):

        if timestamps is None:
            load_end = datetime.datetime.today() - BDay(1)
            timestamps = [load_end.timestamp(),load_end.timestamp(),load_end.weekday(),load_end.year,load_end.month,load_end.day,load_end.hour,load_end.minute,load_end.second]
        ticker_info = self.data_loader.get_info_df().transpose().to_dict()
        main_instrument_id_symbol_dict = dict([(key,str(value['instrument_id'])) for key,value in ticker_info.items()])
        symbol_type_dict = dict([(key,str(value['instrument_type'])) for key,value in ticker_info.items()])
        main_instrument_id = list(main_instrument_id_symbol_dict.values())
        temp_roll_indictor,before_ticker_dict,after_ticker_dict = self.data_loader.check_future_mainforce(main_instrument_id,timestamps[Time.YEAR.value],timestamps[Time.MONTH.value],timestamps[Time.DAY.value])
        ticker_dict = before_ticker_dict if is_before else after_ticker_dict
        trading_instrument_id_symbol_dict = dict([(k,ticker_dict.get(k,v)) for k,v in main_instrument_id_symbol_dict.items()])
        rev_trading_instrument_id_symbol_dict = dict([(v,k) for k,v in trading_instrument_id_symbol_dict.items()])
        return trading_instrument_id_symbol_dict,rev_trading_instrument_id_symbol_dict,symbol_type_dict

class BacktestOrderRouter(OrderRouter):
    pass

class TestOrderRouter(OrderRouter):
    pass

class CsvOrderRouter(OrderRouter):
    def __init__(self,commission_fee = 0, commission_pool_path = None,order_router_config = {}):
        super().__init__(commission_fee = commission_fee, commission_pool_path = commission_pool_path,order_router_config = order_router_config)
        self.dir_path = order_router_config.get("dir_path",None)
        self.pending_items_dict = {}
        self.prev_date = None
        self.is_rolled = False

    def cross_order(self, order_dict, strategy_id,timestamps,symbols,roll_order_dict = {},back_up_symbols=[]):
        get_logger.get_logger().info(f"{self.__class__.__name__} cross_order dt:{timestamps[Time.YEAR.value:Time.MINUTE.value]} strategy_id:{strategy_id}")
        date = f"{timestamps[Time.YEAR.value]}{str(timestamps[Time.MONTH.value]).zfill(2)}{str(timestamps[Time.DAY.value]).zfill(2)}"
        self.reset_fields(date)
        if len(roll_order_dict.get("orders",{})) != 0:
            self.is_rolled = True

        instrument_id_symbol_dict,rev_instrument_id_symbol_dict,symbol_type_dict = self.translate_symbol(is_before=(not self.is_rolled),timestamps=timestamps)
        instrument_id_order_dict = dict([(instrument_id_symbol_dict[symbol],value) for symbol,value in order_dict.items()])
        for symbol,value in roll_order_dict.get("orders",{}).items():
            if symbol not in instrument_id_order_dict:
                instrument_id_order_dict[symbol] = 0
            instrument_id_order_dict[symbol] += value

        instrument_id_order_dict = dict([(k,v) for k,v in instrument_id_order_dict.items() if v != 0])
        order_list = [{"Symbol":symbol,"Order":value} for symbol,value in instrument_id_order_dict.items()]
        df = pd.DataFrame(order_list)
        abspath = os.path.dirname(__file__)
        df_name = os.path.join(abspath,self.dir_path,f"Order_{date}.csv")
        df.to_csv(df_name,index = False)
        get_logger.get_logger().info(f"{self.__class__.__name__} df_name:{df_name} df:{df.to_json()} dt:{date}")
        self.waiting_order_list.append(date)
        get_logger.get_logger().info(f"{self.__class__.__name__} dt:{date} waiting_order_list:{self.waiting_order_list}")
        self.pending_items_dict[date] = (symbols,instrument_id_symbol_dict,rev_instrument_id_symbol_dict,roll_order_dict)

    def reset_fields(self,date):
        if self.prev_date is None or self.prev_date != date:
            self.is_rolled = False
            self.prev_date = date



    def run_regular_task(self):
        get_logger.get_logger().info(f"{self.__class__.__name__} run_regular_task")
        self.check_feedback()


    def check_feedback(self):
        abspath = os.path.dirname(__file__)
        files = os.listdir(self.dir_path)
        for file_path in files:
            if ".csv" not in file_path:
                continue
            name = file_path.split(".")[0]
            dir_type,date = name.split("_")
            if not (dir_type == "Executed" and date in self.waiting_order_list):
                continue

            full_file_path = os.path.join(abspath,self.dir_path,file_path)
            df = pd.read_csv(full_file_path)
            df.index = [str(s) for s in df["Symbol"]]
            del df["Symbol"]
            get_logger.get_logger().info(f"{self.__class__.__name__} date:{date} df:{df.to_json()}")
            MessageBox.send_msg(text=f"{self.__class__.__name__} date:{date} df:{df.to_json()}")
            df_new = df.rename(columns={'Amount': 'volumeFilled',"PriceTraded":"priceFilled"})
            df_new["commission"] = 0
            df_new["direction"] = [ "BUY" if x else "SELL" for x in list(df_new["volumeFilled"] >= 0)]
            df_new["volumeFilled"] = abs(df_new["volumeFilled"])

            result_dict = dict([(key,dict(df_new.loc[key])) for key in df_new.index])

            timestamp = datetime.datetime.timestamp(pd.to_datetime(date, format='%Y%m%d', utc=True))
            symbols,instrument_id_symbol_dict,rev_instrument_id_symbol_dict,roll_order_dict = self.pending_items_dict[date]
            matrix_msg = self.return_orderfeedback(symbols,timestamp,result_dict,roll_order_dict,instrument_id_symbol_dict)
            
            task_type = "order_feedback"
            strategy_id = 0
            dt = pd.to_datetime(date, format='%Y%m%d', utc=True)
            timestamps = datetime.datetime.timestamp(dt)
            time_array = np.array([ timestamps,timestamps,dt.weekday(),dt.year,dt.month,dt.day,0,0,0 ]) 

            transaction_dict = dict(zip(symbols,matrix_msg[OrderFeedback.transaction.value]))
            get_logger.get_logger().info(f"{self.__class__.__name__} transaction:{transaction_dict} date:{date}")
            MessageBox.send_msg(text=f"{self.__class__.__name__} transaction:{transaction_dict} date:{date}")

            self.waiting_order_list.remove(date)
            get_logger.get_logger().info(f"{self.__class__.__name__} date:{date} waiting_order_list:{self.waiting_order_list}")
            del self.pending_items_dict[date]
            self.order_feedback_queue.put((strategy_id,task_type,matrix_msg,time_array))
    
    def check_holding(self,strategy_id,symbols,timestamps):
        if strategy_id != 0:
            raise Exception("strategy_id must be 0")
        date = f"{timestamps[Time.YEAR.value]}{str(timestamps[Time.MONTH.value]).zfill(2)}{str(timestamps[Time.DAY.value]).zfill(2)}"
        abspath = os.path.dirname(__file__)
        file_path = f"Holding_{date}.csv"
        full_file_path = os.path.join(abspath,self.dir_path,file_path)
        if not os.path.exists(full_file_path):
            return None
        else:
            df = pd.read_csv(full_file_path)
            get_logger.get_logger().info(f"{self.__class__.__name__} date:{date} holding df:{df.to_json()}")
        df.index = [str(s) for s in df["Symbol"]]
        
        holding_position_dict = dict(df["Position"])
        holding_price_dict = dict(df["Cost"])
        ccy_dict = {}
        msg = self.return_holding_msg(strategy_id,symbols,timestamps,holding_position_dict,holding_price_dict,ccy_dict)
        get_logger.get_logger().info(f"{self.__class__.__name__} transaction:{msg[OrderFeedback.transaction.value]} date:{date}")
        MessageBox.send_msg(text=f"{self.__class__.__name__} transaction:{msg[OrderFeedback.transaction.value]} date:{date}")
        return msg

    def check_cash(self,timestamps):
        base_ccy = self.currency
        if base_ccy not in ["HKD","USD","CNY","CNH"]:
            raise Exception(f"base_ccy:{base_ccy} is not support")

        date = f"{timestamps[Time.YEAR.value]}{str(timestamps[Time.MONTH.value]).zfill(2)}{str(timestamps[Time.DAY.value]).zfill(2)}"
        abspath = os.path.dirname(__file__)
        file_path = f"Cash_{date}.csv"
        full_file_path = os.path.join(abspath,self.dir_path,file_path)
        if not os.path.exists(full_file_path):
            return None
        else:
            df = pd.read_csv(full_file_path)
        df.index = df["Ccys"]
        balance_dict = dict(map(lambda x:(x[0],x[1]),dict(df["Balance"]).items()))
        get_logger.get_logger().info(f"{self.__class__.__name__} check balance:{balance_dict} date:{date}")
        MessageBox.send_msg(text=f"{self.__class__.__name__} check balance:{balance_dict} date:{date}")
        return self.return_cash_msg(timestamps,balance_dict)

class PortfolioTaskEngineOrderRouter(OrderRouter):
    def __init__(self,commission_fee = 0, commission_pool_path = None,order_router_config = {}):
        super().__init__(commission_fee = commission_fee, commission_pool_path = commission_pool_path,order_router_config = order_router_config)
        self.account = order_router_config.get("ACCOUNT",None)
        self.task_ippath = order_router_config.get("task_ippath",None)
        self.subtask_ippath = order_router_config.get("subtask_ippath",None)
        self.brokerType = order_router_config.get("brokerType",None)
        self.waiting_order_info = {}
        
        

    def cross_order(self, order_dict, strategy_id,timestamps,symbols,roll_order_dict = {},back_up_symbols=[]):
        get_logger.get_logger().info(f"{self.__class__.__name__} cross_order dt:{timestamps[Time.YEAR.value:Time.MINUTE.value]} strategy_id:{strategy_id}")

        instrument_id_symbol_dict,rev_instrument_id_symbol_dict,symbol_type_dict = self.translate_symbol(is_before=False,timestamps=timestamps)
        temp_back_up_symbols = [instrument_id_symbol_dict[symbol] for symbol in back_up_symbols]
        temp_order_dict = dict([(instrument_id_symbol_dict[symbol],value) for symbol,value in order_dict.items()])
        temp_order_dict = dict(filter(lambda x: x[1] > 0.5 or x[1] < -0.5 ,temp_order_dict.items()))
        get_logger.get_logger().info(f"{self.__class__.__name__} temp_order_dict :{temp_order_dict}")
        MessageBox.send_msg(text=f"{self.__class__.__name__} temp_order_dict :{temp_order_dict}")
        get_logger.get_logger().info(f"{self.__class__.__name__} roll_order :{roll_order_dict.get('orders',{})}")
        MessageBox.send_msg(text=f"{self.__class__.__name__} roll_order :{roll_order_dict.get('orders',{})}")
        get_logger.get_logger().info(f"{self.__class__.__name__} back_up_symbols :{back_up_symbols}")
        MessageBox.send_msg(text=f"{self.__class__.__name__} back_up_symbols :{back_up_symbols}")

        returndata = checkSubAccountHolding(subAccount=self.account,ippath=self.task_ippath)
        get_logger.get_logger().info(f"{self.__class__.__name__} checkSubAccountHolding returndata:{returndata}")
        MessageBox.send_msg(text=f"{self.__class__.__name__} checkSubAccountHolding returndata:{returndata}")

        holding_dict = dict(map(lambda x:(str(x["instrumentId"]),x["holdingPosition"]),returndata["data"]))
        for key,value in temp_order_dict.items():
            holding_dict[key] = holding_dict.get(key,0) + temp_order_dict[key]

        future_symbols = list(filter(lambda x:symbol_type_dict[x]=="FUTURES",symbols))
        future_symbols_instrument_id = [instrument_id_symbol_dict[symbol] for symbol in future_symbols]

        holding_dict_for_weight = dict(filter(lambda x:x[0] not in future_symbols_instrument_id,holding_dict.items()))
        holding_dict_for_position = dict(filter(lambda x:x[0] in future_symbols_instrument_id,holding_dict.items()))
        for key,value in roll_order_dict.get('orders',{}).items():
            holding_dict_for_position[key] = holding_dict_for_position.get(key,0) + value
        holding_dict_for_position = dict([(key,str(value)) for key,value in holding_dict_for_position.items()])

        execution_current_data = np.copy(self.execution_current_data)
        execution_current_data[np.isnan(execution_current_data)] = 0
        snapshot_dict = dict(zip(self.symbols,execution_current_data))
        snapshot_dict = dict([ (instrument_id_symbol_dict[symbol],value) for symbol,value in snapshot_dict.items()])
        snapshot_dict = dict([ (instrument_id,snapshot_dict[instrument_id]) for instrument_id,value in holding_dict_for_weight.items()])
        snapshot_dict = dict([ (instrument_id,value if value is not None else 0 ) for instrument_id,value in snapshot_dict.items()])

        get_logger.get_logger().info(f"{self.__class__.__name__} snapshot_dict:{snapshot_dict}")
        holding_amount_dict_for_weight = dict([(key,value*snapshot_dict[key]) for key,value in holding_dict_for_weight.items()])
        maxExposure = np.sum(list(holding_amount_dict_for_weight.values()))
        target_weight = {} if maxExposure < 0.1 else dict([(k,str(v/maxExposure)) for (k,v) in holding_amount_dict_for_weight.items()])
        target_weight = json.dumps(target_weight)
        targetPosition = json.dumps(holding_dict_for_position)

        is_success = False
        if len(self.waiting_order_list) == 0 and (len(temp_order_dict) != 0 or len(roll_order_dict) != 0):
            get_logger.get_logger().info(f"{self.__class__.__name__} start place task to oms with target_weight:{target_weight} maxExposure:{maxExposure}")
            returndata = postPortfolioTask(subAccount = self.account, maxExposure = maxExposure, targetPosition = targetPosition, 
                                        targetweight = target_weight, ippath = self.task_ippath,backupSymbols = temp_back_up_symbols)
            get_logger.get_logger().info(f"{self.__class__.__name__} postPortfolioTask returndata:{returndata}")
            MessageBox.send_msg(text=f"{self.__class__.__name__} postPortfolioTask returndata:{returndata}")
            if returndata['status']['ecode'] == 0:
                is_success = True
                task_id = returndata['data']
                returndata = generateSubTasks(taskType="NORMAL", taskId = task_id, ippath = self.task_ippath)
                get_logger.get_logger().info(f"{self.__class__.__name__} generateSubTasks returndata:{returndata}")
                returndata = GetSubTask(taskType="NORMAL", taskId = task_id, ippath = self.task_ippath)
                get_logger.get_logger().info(f"{self.__class__.__name__} GetSubTask returndata:{returndata}")
                self.waiting_order_list.append(task_id)
                self.waiting_order_info[task_id] = (task_id,symbols,timestamps,instrument_id_symbol_dict,rev_instrument_id_symbol_dict,roll_order_dict)
                get_logger.get_logger().info(f"{self.__class__.__name__} waiting_order_list:{self.waiting_order_list}")
                get_logger.get_logger().info(f"{self.__class__.__name__} waiting_order_info:{self.waiting_order_info}")
                subtask_ids = [item["id"] if type(item) == dict and "id" in item else None for item in returndata["data"]]
                subtask_ids = list(filter(lambda x: x is not None,subtask_ids))
                for subtask_id in subtask_ids:
                    returndata = doSubTask(taskType="NORMAL", subTaskId=subtask_id, ippath = self.subtask_ippath)
                    get_logger.get_logger().info(f"{self.__class__.__name__} doSubTask subtask_id:{subtask_id} returndata:{returndata}")
            else:
                self.fail_order_list.append((returndata['status']['ecode'],timestamps))
                get_logger.get_logger().info(f"{self.__class__.__name__} fail_order_list:{self.fail_order_list}")
        else:
            get_logger.get_logger().info(f"{self.__class__.__name__} previous task is still pending,dt:{timestamps[Time.YEAR.value:Time.MINUTE.value]} strategy_id:{strategy_id}")
            MessageBox.send_msg(text=f"{self.__class__.__name__} previous task is still pending,dt:{timestamps[Time.YEAR.value:Time.MINUTE.value]} strategy_id:{strategy_id}")
        if not is_success:
            matrix_msg = np.zeros((6,len(symbols)))
            matrix_msg[OrderFeedback.transaction.value] = np.zeros(len(symbols))
            matrix_msg[OrderFeedback.current_data.value] = np.ones(len(symbols)) * np.nan
            matrix_msg[OrderFeedback.current_fx_data.value] = np.ones(len(symbols))
            matrix_msg[OrderFeedback.commission_fee.value] = np.zeros(len(symbols))
            matrix_msg[OrderFeedback.order_status.value] = np.ones(len(symbols)) * OrderStatus.REJECTED.value
            matrix_msg[OrderFeedback.timestamps.value] = np.ones(len(symbols)) * timestamps[Time.TIMESTAMP.value]
            strategy_id = 0
            task_type = "order_feedback"
            self.order_feedback_queue.put((strategy_id,task_type,matrix_msg,timestamps))


    def run_regular_task(self):
        get_logger.get_logger().info(f"{self.__class__.__name__} run_regular_task")
        is_completed = False
        for task_id in self.waiting_order_list:
            _,symbols,timestamps,instrument_id_symbol_dict,rev_instrument_id_symbol_dict,roll_order_dict = self.waiting_order_info[task_id]
            get_logger.get_logger().info(f"{self.__class__.__name__} start review task status task_id:{task_id}")
            returndata = reviewSingleTask(taskType="NORMAL", taskId=task_id, ippath = self.task_ippath)
            get_logger.get_logger().info(f"{self.__class__.__name__} reviewSingleTask returndata:{returndata}")
            returndata = GetTask(taskType="portfolioTask",taskId=task_id,ippath=self.task_ippath)
            get_logger.get_logger().info(f"{self.__class__.__name__} GetTask returndata:{returndata}")
            if returndata["data"] is None:
                MessageBox.send_msg(text=f"{self.__class__.__name__} GetTask return none data")
                continue
            if returndata["data"]["status"] in ["TASK_COMPLETED","TASK_DEACTIVATED"]:
                MessageBox.send_msg(text=f"{self.__class__.__name__} GetTask returndata:{returndata}")
                is_completed = True
                result_dict = dict(map(lambda x:(x["instrumentId"],x),returndata["data"]["result"])) 
                matrix_msg = self.return_orderfeedback(symbols,timestamps[0],result_dict,roll_order_dict,instrument_id_symbol_dict)
                transaction_dict = dict(zip(symbols,matrix_msg[OrderFeedback.transaction.value]))
                get_logger.get_logger().info(f"{self.__class__.__name__} transaction:{transaction_dict}")
                MessageBox.send_msg(text=f"{self.__class__.__name__} transaction:{transaction_dict}")
                strategy_id = 0
                task_type = "order_feedback"
                self.order_feedback_queue.put((strategy_id,task_type,matrix_msg,timestamps))
        if is_completed:
            self.waiting_order_list.clear()
            self.waiting_order_info.clear()
    
    def check_holding(self,strategy_id,symbols,timestamps):
        if strategy_id != 0:
            raise Exception("strategy_id must be 0")
        returndata = checkSubAccountHolding(subAccount = self.account ,ippath=self.task_ippath)
        get_logger.get_logger().info(f"{self.__class__.__name__} check subaccount holding:{returndata} with strategy_id:{strategy_id} timestamp:{timestamps[Time.YEAR.value:Time.MINUTE.value]}")
        MessageBox.send_msg(text=f"{self.__class__.__name__} check subaccount holding:{returndata} with strategy_id:{strategy_id}")
        
        holding_position_dict = dict(map(lambda x:(x["instrumentId"],x["holdingPosition"]),returndata["data"]))
        holding_price_dict = dict(map(lambda x:(x["instrumentId"],x["holdingPrice"]),returndata["data"]))
        ccy_dict = dict(map(lambda x:(x["instrumentId"],x["currency"]),returndata["data"]))
        msg = self.return_holding_msg(strategy_id,symbols,timestamps,holding_position_dict,holding_price_dict,ccy_dict)

        transaction_dict = dict(zip(symbols,msg[OrderFeedback.transaction.value]))
        get_logger.get_logger().info(f"{self.__class__.__name__} transaction:{transaction_dict}")
        MessageBox.send_msg(text=f"{self.__class__.__name__} transaction:{transaction_dict}")
        return msg

    def check_cash(self,timestamps):
        base_ccy = self.currency
        if base_ccy not in ["HKD","USD","CNY","CNH"]:
            raise Exception(f"base_ccy:{base_ccy} is not support")
        returndata = checkSubAccountBalance(subAccount=self.account,ippath=self.task_ippath)
        
        balance_dict = dict(map(lambda x:(x[0],x[1]["balance"]),returndata["data"].items()))
        get_logger.get_logger().info(f"{self.__class__.__name__} check balance:{balance_dict}")
        MessageBox.send_msg(text=f"{self.__class__.__name__} check balance:{balance_dict}")
        return self.return_cash_msg(timestamps,balance_dict)
