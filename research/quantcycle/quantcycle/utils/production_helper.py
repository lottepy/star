import base64
import configparser
import hashlib
import hmac
import importlib
import json
import os
import time
import traceback
from datetime import datetime
from multiprocessing import Queue,current_process
from threading import Event, Thread, currentThread

import numpy as np
import pandas as pd
import requests
from events import Events
from numba import jit
from quantcycle.app.result_exporter.utils.user_save import UserSave
from quantcycle.utils import get_logger
from quantcycle.utils.production_constant import (GET_SYMBOL_TYPE_BATCH,
                                                  REQUEST_TIMEOUT,
                                                  InstrumentType,
                                                  TerminationType)
from quantcycle.utils.production_mapping import ccy_usd_map, fx_ticker_dm_map
from quantcycle.utils.production_path import quote_engine_path


class ThreadHander():
    def __init__(self,f,thread_name,streaming_queue = Queue(),is_debug = False,need_queue = True):
        self.__f = f
        self.__thread_name = thread_name
        self.__streaming_queue = streaming_queue
        self.__is_debug = is_debug
        self.__need_stop = False
        self.__thread = None
        self.__is_runned = False
        self.__need_queue = need_queue


    def __run(self):
        while True:
            try:
                if self.__need_stop:
                    get_logger.get_logger().info(f"{self.__class__.__name__}|{self.__thread_name} is hard killed")
                    return
                item = None
                if self.__need_queue:
                    item = self.__streaming_queue.get()
                    get_logger.get_logger().info(f"{self.__class__.__name__}|{self.__thread_name}'s item:{item}'")
                    self.__f(item)
                else:
                    self.__f()
                get_logger.get_logger().info(f"{self.__class__.__name__}|{self.__thread_name} end")

                if item == TerminationType.END.value:
                    get_logger.get_logger().info(f"{self.__class__.__name__}|{self.__thread_name} is soft killed")
                    return
            except Exception as e:
                name = f"{self.__class__.__name__}|{self.__thread_name}"
                exception_msg = f"{name}|Exception:{str(e)}"
                traceback_msg = str(traceback.format_exc())
                get_logger.get_logger().info(exception_msg)
                MessageBox.send_msg(text=exception_msg)
                get_logger.get_logger().info(traceback_msg)
                MessageBox.send_msg(text=traceback_msg)
                if self.__is_debug:
                    raise e

    def run(self):
        if self.__is_runned:
            get_logger.get_logger().info(f"{self.__class__.__name__}|{self.__thread_name} is runned before")
            return
        self.__thread = Thread(target = self.__run, daemon = True,name = self.__thread_name)
        self.__thread.start()

    def slow_kill(self):
        get_logger.get_logger().info(f"{self.__class__.__name__}|{self.__thread_name} start slow kill")
        self.__streaming_queue.put(TerminationType.END.value)
    
    def hard_kill(self):
        get_logger.get_logger().info(f"{self.__class__.__name__}|{self.__thread_name} start hard kill")
        self.__need_stop = True

    def join(self):
        if self.__is_runned:
            get_logger.get_logger().info(f"{self.__class__.__name__}|{self.__thread_name} is not running yet")
            return
        get_logger.get_logger().info(f"{self.__class__.__name__}|{self.__thread_name} start join")
        self.__thread.join()
        get_logger.get_logger().info(f"{self.__class__.__name__}|{self.__thread_name} joined")


class TimeSeriesTracker():
    def __init__(self,timestamp_array,time_array,data_array):
        """ time_array should be iterated object with timestamp series """
        self.org_time_array = np.array(time_array)
        self.timestamp_array = np.array(timestamp_array)
        self.data_array = np.array(data_array)
        self.time_index = 0
        self.prev_time_index = 0
        self.select_time_array = None
        self.select_data_array = None
        if len(self.org_time_array) != len(self.data_array) or len(self.org_time_array) != len(self.timestamp_array):
            raise Exception("len of time_array not equal to len of data_array")
    
    def compare_timestamp(self,time_stamp):
        if self.time_index == len(self.timestamp_array):
            self.select_time_array = None
            self.select_data_array = None
            print("no more data")
            return
        if time_stamp < self.timestamp_array[self.time_index]:
            self.select_time_array = None
            self.select_data_array = None
            print("input timestamp is less than next timestamp in time array")
            return 
        self.prev_time_index = self.time_index
        if time_stamp >= self.timestamp_array[-1]:
            self.time_index = len(self.timestamp_array)
        else:
            for index in range(self.time_index,len(self.timestamp_array)):
                if self.timestamp_array[index] <= time_stamp:
                    pass
                else:
                    self.time_index = index
                    break
        self.select_time_array = np.array(self.org_time_array)[self.prev_time_index:self.time_index]
        self.select_data_array = np.array(self.data_array)[self.prev_time_index:self.time_index]

    def return_data(self):
        return self.select_time_array,self.select_data_array

    



class Multi_event():
    def __init__(self,name = "multi_event"):
        self.size = 0
        self.event = Event()
        self.name = name

    def set_event(self,size = 1):
        if size == 0:
            self.event.clear()
            self.event.set()
            get_logger.get_logger().info(f"{self.name} set_event with size:{self.size}")
            return
        elif size < 0:
            raise Exception("size can not be negative or equal to zero")
        else:
            self.size += size
            self.event.clear()
            get_logger.get_logger().info(f"{self.name} set_event with size:{self.size}")
    
    def release(self):
        self.size -= 1
        if self.size == 0:
            self.event.set()
        get_logger.get_logger().info(f"{self.name} release with size:{self.size}")

    def wait(self):
        get_logger.get_logger().info(f"{self.name} block")
        self.event.wait()
        get_logger.get_logger().info(f"{self.name} unblock")

class WindowDataGenerator():
    def __init__(self,window_size):
        self.window_size = window_size
        self.is_start = False
        self.window_data = None
        self.index = window_size
        self.ready = False

    def receive_data(self,data):
        # data can only be np.array
        if not self.is_start:
            self.is_start = True
            shape = np.array(data).shape
            dtype = data.dtype
            self.window_data = np.zeros(tuple([self.window_size]) + shape).astype(dtype)
        self.window_data[:-1]  = self.window_data[1:]
        self.window_data[-1] = data
        self.index -= 1
        return self.return_window_data()
    
    def return_window_data(self):
        if self.index <= 0 :
            self.ready = True
            return np.copy(self.window_data)
        else:
            return None

class WindowDataGeneratorNew(): 
    #TODO: generate fx/rate/tradable table win data (without window size requirement)
    def __init__(self,window_size):
        self.window_size = window_size
        self.is_start = False
        self.window_data = None
        self.index = window_size
        self.ready = False

    def receive_data(self,data):
        # data can only be np.array
        if not self.is_start:
            self.is_start = True
            shape = np.array(data).shape
            dtype = data.dtype
            self.window_data = np.zeros(tuple([self.window_size]) + shape).astype(dtype)
        self.window_data[:-1]  = self.window_data[1:]
        self.window_data[-1] = data
        self.index -= 1
        return self.return_window_data()
    def return_window_data(self):
        if self.window_data is not None :
            self.ready = True
            return np.copy(self.window_data)
        else:
            return None


class WindowSecDataGenerator():
    def __init__(self,window_size_dict):
        self.window_size_dict = window_size_dict
        self.secondary_window_data = None
        self.sec_size = 1

    def receive_secondary_data(self,sec_data):
        self.secondary_window_data={}
        for id,item in sec_data.items():

            self.secondary_window_data[id]={}
            #self.secondary_window_data[id]["ID_symbol_map"] = {}
            for name, data_time in item.items():
                if 'ID_symbol_map' in name or 'id_map' in name:
                    self.secondary_window_data[id][name]= data_time
                
                else:
                    self.secondary_window_data[id][name] = {}
                    if data_time=={}:
                        self.secondary_window_data[id][name]['data'] = []
                        self.secondary_window_data[id][name]['time'] = []
                    else:
                        self.secondary_window_data[id][name]['data'] = data_time['data'][-self.window_size_dict.get(name,self.sec_size):,:,:]
                        self.secondary_window_data[id][name]['time'] = data_time['time'][-self.window_size_dict.get(name,self.sec_size):,:]

        return self.secondary_window_data.copy()

def init_strategy_pms(strategy_dict:dict, cash, symbol2instrument_type_dict,symbol2ccy_dict,ref_data_info):
    from quantcycle.app.pms_manager.portfolio_manager import PorfolioManager
    ccy_matrix = get_symbol2ccy_matrix(strategy_dict["symbol"],symbol2ccy_dict)
    symbols = list(ccy_matrix.columns)
    symbol2instrument_type = np.array([ symbol2instrument_type_dict[symbol] for symbol in symbols]).astype(np.int64)
    symbol_type_array = np.array(list(np.array([ InstrumentType(num ).name for num in symbol2instrument_type]))) 
    pms = PorfolioManager(cash,ccy_matrix.values,symbol2instrument_type)
    symbol_batch = np.array(list(ccy_matrix.columns))
    ccy_list = np.array([ccy_matrix[x][ccy_matrix[x]==1].index[0] for x in ccy_matrix.columns])
    STRATEGY_MODULE = importlib.import_module(strategy_dict["strategy_module"])
    importlib.reload(STRATEGY_MODULE)
    ref_symbol_batch = dict([(key,{"symbols":value["Symbol"]}) for key,value in ref_data_info.items()])
    remark_field_names = []
    id_mapping = {}
    strategy_param = np.array(list(strategy_dict["params"].values()))
    strategy_name = strategy_dict["strategy_name"]
    metadata = {}
    metadata["main"] = {"symbols":symbol_batch,"ccy_list":ccy_list,"symbol_types":symbol_type_array,"fields":["open","high","low","close"]}
    #metadata["remark"] = {}
    metadata.update(ref_symbol_batch)

    base_strategy = getattr(STRATEGY_MODULE, strategy_name)(pms ,strategy_name,strategy_param, metadata,ccy_matrix.values,id_mapping)
    base_strategy.init()
    return base_strategy,pms,ccy_matrix,symbol2instrument_type

@jit(nopython=True, cache=True)
def calc_pnl_base_ccy_numba(current_data_new, current_data_old, current_holding,current_fx_data):
    pnl_local_ccy = (current_data_new - current_data_old) * current_holding
    pnl_base_ccy = (current_fx_data) * pnl_local_ccy
    return pnl_base_ccy    

@jit(nopython=True, cache=True)
def calc_rate_base_ccy(current_holding,current_rate, current_data, current_fx_data):
    #return
    multiple_i = np.copy(current_rate)
    symbol_cash = multiple_i * current_holding * current_data
    pnl_base_ccy = (current_fx_data * symbol_cash)
    return pnl_base_ccy

def get_symbol2ccy_matrix(symbols, symbol2ccy_dict):
    """ 
        returns a DataFrame, as matrix with columns = symbols and index = ccys 
    """
    temp_symbols = sorted(symbols)
    all_ccy = sorted(set(symbol2ccy_dict.values()))
    symbol2ccy_matrix = pd.DataFrame(np.zeros((len(all_ccy), len(temp_symbols))), index=all_ccy, columns=temp_symbols)
    for sym in temp_symbols:
        ccy = symbol2ccy_dict[sym]
        symbol2ccy_matrix.loc[ccy][sym] = 1
    return symbol2ccy_matrix

def checkSubAccountHolding(subAccount, ippath = None):
    agisurl = ippath + "queryHoldingsOfSubAccount?subAccountId=" + subAccount
    print(agisurl)
    headers = {"Accept": "*/*"}
    returnjson = requests.get(agisurl, headers=headers,timeout=REQUEST_TIMEOUT)
    returndata = json.loads(returnjson.text)
    print(returndata)
    return returndata

def checkHolding(brokerType, brokerAccount, ippath = None):
    agisurl = ippath + "queryHoldingsOfAccount?brokerType=" + brokerType + "&brokerAccount=" + brokerAccount
    print(agisurl)
    headers = {"Accept": "*/*"}
    returnjson = requests.get(agisurl, headers=headers,timeout=REQUEST_TIMEOUT)
    returndata = json.loads(returnjson.text)
    print(returndata)
    return returndata
    
def checkSubAccountBalance(subAccount, ippath = None):
    agisurl = ippath + "queryCashOfSubAccount?subAccountId=" + subAccount
    print(agisurl)
    headers = {"Accept": "*/*"}
    returnjson = requests.get(agisurl, headers=headers,timeout=REQUEST_TIMEOUT)
    returndata = json.loads(returnjson.text)
    print(returndata)
    return returndata

def checkBalance(brokerAccount, ippath = None):
    agisurl = ippath + "queryCashOfAccount?brokerAccount=" + brokerAccount
    print(agisurl)
    headers = {"Accept": "*/*"}
    returnjson = requests.get(agisurl, headers=headers,timeout=REQUEST_TIMEOUT)
    returndata = json.loads(returnjson.text)
    print(returndata)
    return returndata

def doSubTask(taskType, subTaskId, ippath = None):
    #agisurl = ippath + "subtask?subTaskId=" + subTaskId + "&taskType=" + taskType
    agisurl = ippath + "subtask?taskType=" + taskType + "&subTaskId=" + str(subTaskId)
    print(agisurl)
    headers = {'Content-Type': 'application/json'}
    returnjson = requests.post(agisurl, headers=headers,timeout=REQUEST_TIMEOUT)
    returndata = json.loads(returnjson.text)
    print(returndata)
    return returndata

    
def deactivate(taskType, taskId, ippath = None):
    agisurl = ippath + "deactivate?taskId=" + str(taskId) + "&taskType=" + taskType
    print(agisurl)
    headers = {'Content-Type': 'application/json'}
    returnjson = requests.post(agisurl, headers=headers,timeout=REQUEST_TIMEOUT)
    returndata = json.loads(returnjson.text)
    print(returndata)
    return returndata


def reviewSingleTask(taskType, taskId, ippath = None):
    agisurl = ippath + "reviewSingleTask?taskId=" + str(taskId) + "&taskType=" + taskType
    print(agisurl)
    headers = {'Content-Type': 'application/json'}
    returnjson = requests.post(agisurl, headers=headers,timeout=REQUEST_TIMEOUT)
    returndata = json.loads(returnjson.text)
    print(returndata)
    return returndata

def generateSubTasks(taskType, taskId, ippath = None):
    agisurl = ippath + "generateSubTasks?taskId=" + str(taskId) + "&taskType=" + taskType
    print(agisurl)
    headers = {'Content-Type': 'application/json'}
    returnjson = requests.post(agisurl, headers=headers,timeout=REQUEST_TIMEOUT)
    returndata = json.loads(returnjson.text)
    print(returndata)
    return returndata


def postTask(taskType=None, externalId=None, brokerAccount=None, currency=None, taskSubType=None, deltaAmount=None,
             availableCashAmount=None, activationDate=None, confirmationTimestamp=None, immediatelyEnabled=None,
             targetweight=None, ippath = None):
    agisurl = ippath + taskType + "/?"
    if (externalId != None): agisurl += "externalId=" + externalId
    if (brokerAccount != None): agisurl += "&brokerAccount=" + brokerAccount
    if (currency != None): agisurl += "&currency=" + currency
    if (taskSubType != None): agisurl += "&taskSubType=" + taskSubType
    if (deltaAmount != None): agisurl += "&deltaAmount=" + deltaAmount
    if (availableCashAmount != None): agisurl += "&availableCashAmount=" + availableCashAmount
    if (activationDate != None): agisurl += "&activationDate=" + activationDate
    if (confirmationTimestamp != None): agisurl += "&confirmationTimestamp=" + confirmationTimestamp
    if (immediatelyEnabled != None): agisurl += "&immediatelyEnabled=" + immediatelyEnabled
    print(agisurl)
    headers = {'Content-Type': 'application/json'}
    returnjson = requests.post(agisurl, headers=headers, json=targetweight,timeout=REQUEST_TIMEOUT)
    returndata = json.loads(returnjson.text)
    print(returndata)
    return returndata

def postPortfolioTask(subAccount = None, maxExposure = None, targetPosition = json.dumps({}), targetweight = json.dumps({}), ippath = None,backupSymbols = []):
    taskType = "portfolioTask"
    agisurl = ippath + taskType + "/?"
    print(agisurl)
    json_msg = {
                "activationDate": "2020-01-01",
                "backupSymbols": backupSymbols,
                "isMarketShare": True,
                "maxExposure": maxExposure,
                "subAccountId": subAccount,
                "targetPosition": targetPosition,
                "targetWeight": targetweight,
                "taskParameters": {
                    "closeThenOpenMultipleDivisor": 1,
                    "totalNumOfBatches": 1
                }
            }
    headers = {'Content-Type': 'application/json'}
    returnjson = requests.post(agisurl, headers=headers, json=json_msg,timeout=REQUEST_TIMEOUT)
    returndata = json.loads(returnjson.text)
    print(returndata)
    return returndata


def GetTask(taskType=None,taskId=None, ippath = None):
    agisurl = ippath + taskType + "/?"
    if (taskId != None): agisurl += "taskId=" + str(taskId)
    print(agisurl)
    headers = {'Content-Type': 'application/json'}
    returnjson = requests.get(agisurl, headers=headers,timeout=REQUEST_TIMEOUT)
    returndata = json.loads(returnjson.text)
    print(returndata)
    return returndata

def GetSubTask(taskType=None,taskId=None, ippath = None):
    agisurl = ippath +"/task/subtasks/?"
    if (taskId != None): agisurl += "taskId=" + str(taskId)
    if (taskType != None): agisurl += "&taskType=" + taskType
    print(agisurl)
    headers = {'Content-Type': 'application/json'}
    returnjson = requests.get(agisurl, headers=headers,timeout=REQUEST_TIMEOUT)
    returndata = json.loads(returnjson.text)
    print(returndata)
    return returndata

def GetFXSnapshot(trading_ccy,base_ccy):
    if trading_ccy == base_ccy:
        return 1
    agisurl = quote_engine_path + "market/snapshot?brokerType=NEUTRAL&"
    #@agisurl = os.path.join(data_master_path,"/api/v1/real_time/snapshot/?")
    trading_ccy2usd = ccy_usd_map[trading_ccy] if trading_ccy != "USD" else None
    base_ccy2usd = ccy_usd_map[base_ccy] if base_ccy != "USD" else None
    print(agisurl)
    if trading_ccy2usd is not None:
        headers = {'Content-Type': 'application/json'}
        returnjson = requests.get(agisurl+f"symbol={trading_ccy2usd}.FX", headers=headers,timeout=REQUEST_TIMEOUT)
        returndata = json.loads(returnjson.text)
        print(returndata)
        trading_ccy2usd_fx = returndata["data"][0]["last"] if trading_ccy2usd[3:] == "USD" else 1/returndata["data"][0]["last"]
    else:
        trading_ccy2usd_fx = 1

    if base_ccy2usd is not None:
        headers = {'Content-Type': 'application/json'}
        returnjson = requests.get(agisurl+f"symbol={base_ccy2usd}.FX", headers=headers,timeout=REQUEST_TIMEOUT)
        returndata = json.loads(returnjson.text)
        print(returndata)
        base_ccy2usd_fx = returndata["data"][0]["last"] if base_ccy2usd[:3] == "USD" else 1/returndata["data"][0]["last"]
    else:
        base_ccy2usd_fx = 1

    return trading_ccy2usd_fx * base_ccy2usd_fx


def GetStocksSnapshot(symbol):
    agisurl = quote_engine_path + f"market/snapshot?brokerType=NEUTRAL&symbol={str(symbol)}"
    print(agisurl)
    headers = {'Content-Type': 'application/json'}
    returnjson = requests.get(agisurl, headers=headers)
    returndata = json.loads(returnjson.text)
    return returndata


def init_user_save_dict(super_strategy_manager, remark_save_dir, remark_save_name):
    user_save_dict = {}
    if remark_save_dir is not None and remark_save_name is not None:

        fname_main, fname_ext = os.path.splitext(remark_save_name)
        fname_ext = '.csv'
        if not os.path.exists(remark_save_dir):
            get_logger.get_logger().info(f'Warning: Signal remark存储路径不存在，创建路径:{remark_save_dir}')
            os.makedirs(remark_save_dir)
        else:
            from quantcycle.utils.backtest_helper import remove_files_matching
            get_logger.get_logger().info("Warning: Signal remark路径非空，新结果将覆盖原结果")
            remove_files_matching(remark_save_dir, f'^{fname_main}.*')
        
        for sid, _ in super_strategy_manager.strategies.items():
            filename = f'{fname_main}-{sid}{fname_ext}'

            user_save_dict[sid] = UserSave(remark_save_dir)
            user_save_dict[sid].open_signal_remark(filename, buffer_size=0)
            # when buffer size = 0, UserSave don't have to call close_signal_remark;
            # otherwise, the engine needs to call close_signal_remark
        return user_save_dict

class MessageBox:
    name = "MessageBox"
    #methods = []
    is_send_msg = True
    events = Events("on_msg")

    
    @staticmethod
    def update_name(name):
        MessageBox.name = str(name)

    @staticmethod
    def update_status(is_send_msg):
        MessageBox.is_send_msg = is_send_msg
    
    @staticmethod
    def add_method(f):
        MessageBox.events.on_msg += f
        #MessageBox.methods.append(f)

    @staticmethod
    def send_msg(text):
        if not MessageBox.is_send_msg:
            return
        thread_name = currentThread().getName()
        process_name = current_process().name
        current_time = datetime.now().strftime("%H:%M:%S")
        msg_text = f"{str(current_time)}|{MessageBox.name}|{process_name}|{thread_name}|{str(text)}"
        MessageBox.events.on_msg(msg_text)
        #for f in MessageBox.methods:
        #    f.send_msg(text=msg_text)

class BaseMessage:
    def send_msg(self,text):
        raise NotImplementedError

class DingDingMessage(BaseMessage):
    def __init__(self,dingding_addr):
        self.dingding_addr = dingding_addr


    def send_msg(self,text):
        return requests.post(self.dingding_addr,
                    headers={"content-type":"application/json"},
                    data=('{"msgtype":"text", "text":{"content":"'+text+'"}}').encode('utf-8'),timeout=REQUEST_TIMEOUT)

class LarkMessage(BaseMessage):
    def __init__(self,webhook_url):
        self.url = webhook_url
        self.secret = "secret"

    def send_msg(self,text):
        current_timestamp_in_sec = int(time.time())
        secret_key = str(current_timestamp_in_sec) + '\n' + self.secret
        sign_data = hmac.new(secret_key.encode('utf-8'), msg="".encode("utf-8"), digestmod=hashlib.sha256).digest()
        sign = base64.b64encode(sign_data).decode('utf-8')
        body = {
            "sign": sign,
            "timestamp": current_timestamp_in_sec,
            "msg_type": "text",
            "content": {"text": str(text)}
        }
        r = requests.post(self.url, data=json.dumps(body),timeout=REQUEST_TIMEOUT)
        return r




""" def msg2dd(text):
    return
    return requests.post(dingding_addr,
                    headers={"content-type":"application/json"},
                    data=('{"msgtype":"text", "text":{"content":"'+text+'"}}').encode('utf-8')) """


class Configuration:
    config = {}

    @staticmethod
    def read(config_path):
        if not os.path.exists(config_path):
            exception_msg = f"config_path:{config_path} doesn't exist"
            raise Exception(exception_msg)
        Configuration.config = configparser.ConfigParser()
        Configuration.config.read(config_path)
