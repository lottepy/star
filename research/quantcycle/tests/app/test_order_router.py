import json
import os
import unittest
from multiprocessing import Process, Queue
from unittest import mock

import numpy as np
from quantcycle.app.order_crosser.order_router import (
    GetSubTask, PortfolioTaskEngineOrderRouter, checkSubAccountHolding,
    generateSubTasks, postPortfolioTask)
from quantcycle.app.pms_manager.portfolio_manager import PorfolioManager
from quantcycle.app.settle_manager.settlement import Settler
from quantcycle.utils.production_constant import (InstrumentType,
                                                  OrderFeedback, OrderStatus,
                                                  PmsStatus, TradeStatus)
from quantcycle.utils.production_data_loader import DataLoader


def mock_checkSubAccountHolding_1(subAccount,ippath):
    return {"data":{}}
def mock_generateSubTasks_1(taskType, taskId, ippath):
    assert taskType == "NORMAL"
    assert taskId==0
def mock_GetSubTask_1(taskType, taskId, ippath):
    assert taskType == "NORMAL"
    assert taskId==0
    return {"data":[]}
def mock_reviewSingleTask_1(taskType, taskId, ippath):
    assert taskType == "NORMAL"
    assert taskId==0



def mock_postPortfolioTask_1(subAccount, maxExposure, targetPosition,targetweight, ippath,backupSymbols):
    assert maxExposure == 0
    assert json.loads(targetPosition) == {'1601004125': '200'}
    assert json.loads(targetweight) == {}
    assert len(backupSymbols) == 0
    return {"data":0 , 'status' :{'ecode':0}}
def mock_GetTask_1(taskType, taskId, ippath):
    assert taskType == "portfolioTask"
    assert taskId==0
    returndata = {"data":{}}
    returndata["data"]["result"] = [{"instrumentId":"1601004125","volumeFilled":100 ,"priceFilled":10000 ,"commission":10 ,"direction":"BUY"}]
    returndata["data"]["status"] = "TASK_COMPLETED"
    return returndata

def mock_postPortfolioTask_2(subAccount, maxExposure, targetPosition,targetweight, ippath,backupSymbols):
    assert maxExposure == 0
    assert json.loads(targetPosition) == {"1601004126":'200'}
    assert json.loads(targetweight) == {}
    assert len(backupSymbols) == 0
    return {"data":0 , 'status' :{'ecode':0}}
def mock_postPortfolioTask_3(subAccount, maxExposure, targetPosition,targetweight, ippath,backupSymbols):
    assert maxExposure == 0
    assert json.loads(targetPosition) == {"1601004125":'100',"1601004126":'100'}
    assert json.loads(targetweight) == {}
    assert len(backupSymbols) == 0
    return {"data":0 , 'status' :{'ecode':0}}
def mock_GetTask_3(taskType, taskId, ippath):
    assert taskType == "portfolioTask"
    assert taskId==0
    returndata = {"data":{}}
    returndata["data"]["result"] = []
    returndata["data"]["result"].append({"instrumentId":"1601004125","volumeFilled":100 ,"priceFilled":10000 ,"commission":10 ,"direction":"BUY"})
    returndata["data"]["result"].append({"instrumentId":"1601004126","volumeFilled":100 ,"priceFilled":11000 ,"commission":10 ,"direction":"BUY"})
    returndata["data"]["status"] = "TASK_COMPLETED"
    return returndata
def mock_postPortfolioTask_4(subAccount, maxExposure, targetPosition,targetweight, ippath,backupSymbols):
    assert maxExposure == 100000
    assert json.loads(targetPosition) == {}
    assert json.loads(targetweight) == {"1201016032":'1.0'}
    assert len(backupSymbols) == 0
    return {"data":0 , 'status' :{'ecode':0}}
def mock_GetTask_4(taskType, taskId, ippath):
    assert taskType == "portfolioTask"
    assert taskId==0
    returndata = {"data":{}}
    returndata["data"]["result"] = []
    #returndata["data"]["result"].append({"instrumentId":"1105000700","volumeFilled":100 ,"priceFilled":10000 ,"commission":10 ,"direction":"BUY"})
    returndata["data"]["result"].append({"instrumentId":"1201016032","volumeFilled":100 ,"priceFilled":11000 ,"commission":10 ,"direction":"BUY"})
    returndata["data"]["status"] = "TASK_COMPLETED"
    return returndata
def mock_postPortfolioTask_5(subAccount, maxExposure, targetPosition,targetweight, ippath,backupSymbols):
    assert maxExposure == 200000
    assert json.loads(targetPosition) == {}
    assert json.loads(targetweight) == {"1201016032":'0.5','1105000700':'0.5'}
    assert backupSymbols == ["1105000001"]
    return {"data":0 , 'status' :{'ecode':0}}
def mock_GetTask_5(taskType, taskId, ippath):
    assert taskType == "portfolioTask"
    assert taskId==0
    returndata = {"data":{}}
    returndata["data"]["result"] = []
    returndata["data"]["result"].append({"instrumentId":"1105000700","volumeFilled":100 ,"priceFilled":10000 ,"commission":10 ,"direction":"BUY"})
    returndata["data"]["result"].append({"instrumentId":"1201016032","volumeFilled":100 ,"priceFilled":11000 ,"commission":10 ,"direction":"BUY"})
    returndata["data"]["result"].append({"instrumentId":"1105000001","volumeFilled":100 ,"priceFilled":12000 ,"commission":10 ,"direction":"BUY"})
    returndata["data"]["status"] = "TASK_COMPLETED"
    return returndata
def mock_postPortfolioTask_6(subAccount, maxExposure, targetPosition,targetweight, ippath,backupSymbols):
    assert maxExposure == 200000
    assert json.loads(targetPosition) == {"1601004125":'100'}
    assert json.loads(targetweight) == {"1201016032":'0.5','1105000700':'0.5'}
    assert backupSymbols == ["1105000001"]
    return {"data":0 , 'status' :{'ecode':0}}
def mock_GetTask_6(taskType, taskId, ippath):
    assert taskType == "portfolioTask"
    assert taskId==0
    returndata = {"data":{}}
    returndata["data"]["result"] = []
    returndata["data"]["result"].append({"instrumentId":"1601004125","volumeFilled":100 ,"priceFilled":9999 ,"commission":10 ,"direction":"BUY"})
    returndata["data"]["result"].append({"instrumentId":"1105000700","volumeFilled":100 ,"priceFilled":10000 ,"commission":10 ,"direction":"BUY"})
    returndata["data"]["result"].append({"instrumentId":"1201016032","volumeFilled":100 ,"priceFilled":11000 ,"commission":10 ,"direction":"BUY"})
    returndata["data"]["result"].append({"instrumentId":"1105000001","volumeFilled":100 ,"priceFilled":12000 ,"commission":10 ,"direction":"BUY"})
    returndata["data"]["status"] = "TASK_COMPLETED"
    return returndata

class TestOrderRouter(unittest.TestCase):

    @mock.patch('quantcycle.app.order_crosser.order_router.postPortfolioTask',new=mock_postPortfolioTask_1)
    @mock.patch('quantcycle.app.order_crosser.order_router.GetTask',new=mock_GetTask_1)
    @mock.patch('quantcycle.app.order_crosser.order_router.checkSubAccountHolding',new=mock_checkSubAccountHolding_1)
    @mock.patch('quantcycle.app.order_crosser.order_router.generateSubTasks',new=mock_generateSubTasks_1)
    @mock.patch('quantcycle.app.order_crosser.order_router.GetSubTask',new=mock_GetSubTask_1)
    @mock.patch('quantcycle.app.order_crosser.order_router.reviewSingleTask',new=mock_reviewSingleTask_1)
    def test_order_router_place_order_01(self):
        symbols = ["CN.EXP7.0.SGX"]
        data_info = {
                        "FUTURES": {
                        "DataCenter": "DataMaster",
                        "SymbolArgs": {
                            "DMAdjustType": 0
                        },
                        "Fields": "OHLC",
                        "Frequency": "DAILY"
                        }
                    }
        base_ccy = "USD"
        data_loader = DataLoader(data_info, base_ccy, {"FUTURES":symbols},{}, {}, {})

        order_router = PortfolioTaskEngineOrderRouter(order_router_config={"currency":"USD"})
        order_router.update_symbol(symbols)
        order_router.update_data_loader(data_loader)
        order_feedback_queue = Queue()
        order_router.update_order_feedback_queue(order_feedback_queue)
        order_router.update_data(np.array([1000]),np.array([1]))
        timestamps = np.array([0,0,0,2020,11,16,0,0,0])
        order_router.cross_order(dict(zip(symbols,[200])), 0,timestamps,symbols)
        order_router.run_regular_task()
        strategy_id,task_type,matrix_msg,time_array = order_feedback_queue.get()
        assert strategy_id == 0
        assert task_type == "order_feedback"
        assert (timestamps==time_array).all()
        assert (matrix_msg[OrderFeedback.transaction.value]==np.array([100])).all()
        assert (matrix_msg[OrderFeedback.current_data.value]==np.array([10000])).all()
        assert (matrix_msg[OrderFeedback.current_fx_data.value]==np.array([1])).all()
        assert (matrix_msg[OrderFeedback.commission_fee.value]==np.array([10])).all()
        assert (matrix_msg[OrderFeedback.order_status.value]==np.array([OrderStatus.FILLED.value])).all()
        assert (matrix_msg[OrderFeedback.timestamps.value]==np.array([0])).all()


    @mock.patch('quantcycle.app.order_crosser.order_router.postPortfolioTask',new=mock_postPortfolioTask_2)
    @mock.patch('quantcycle.app.order_crosser.order_router.checkSubAccountHolding',new=mock_checkSubAccountHolding_1)
    @mock.patch('quantcycle.app.order_crosser.order_router.generateSubTasks',new=mock_generateSubTasks_1)
    @mock.patch('quantcycle.app.order_crosser.order_router.GetSubTask',new=mock_GetSubTask_1)
    def test_order_router_place_order_02(self):
        symbols = ["CN.EXP7.0.SGX"]
        data_info = {
                        "FUTURES": {
                        "DataCenter": "DataMaster",
                        "SymbolArgs": {
                            "DMAdjustType": 0
                        },
                        "Fields": "OHLC",
                        "Frequency": "DAILY"
                        }
                    }
        base_ccy = "USD"
        data_loader = DataLoader(data_info, base_ccy, {"FUTURES":symbols},{}, {}, {})

        order_router = PortfolioTaskEngineOrderRouter(order_router_config={"currency":"USD"})
        order_router.update_symbol(symbols)
        order_router.update_data_loader(data_loader)
        order_feedback_queue = Queue()
        order_router.update_order_feedback_queue(order_feedback_queue)
        order_router.update_data(np.array([1000]),np.array([1]))
        timestamps = np.array([0,0,0,2020,11,17,0,0,0])
        order_router.cross_order(dict(zip(symbols,[200])), 0,timestamps,symbols)

    @mock.patch('quantcycle.app.order_crosser.order_router.postPortfolioTask',new=mock_postPortfolioTask_3)
    @mock.patch('quantcycle.app.order_crosser.order_router.GetTask',new=mock_GetTask_3)
    @mock.patch('quantcycle.app.order_crosser.order_router.checkSubAccountHolding',new=mock_checkSubAccountHolding_1)
    @mock.patch('quantcycle.app.order_crosser.order_router.generateSubTasks',new=mock_generateSubTasks_1)
    @mock.patch('quantcycle.app.order_crosser.order_router.GetSubTask',new=mock_GetSubTask_1)
    @mock.patch('quantcycle.app.order_crosser.order_router.reviewSingleTask',new=mock_reviewSingleTask_1)
    def test_order_router_place_order_03(self):
        symbols = ["CN.EXP7.0.SGX"]
        data_info = {
                        "FUTURES": {
                        "DataCenter": "DataMaster",
                        "SymbolArgs": {
                            "DMAdjustType": 0
                        },
                        "Fields": "OHLC",
                        "Frequency": "DAILY"
                        }
                    }
        base_ccy = "USD"
        data_loader = DataLoader(data_info, base_ccy, {"FUTURES":symbols},{}, {}, {})

        order_router = PortfolioTaskEngineOrderRouter(order_router_config={"currency":"USD"})
        order_router.update_symbol(symbols)
        order_router.update_data_loader(data_loader)
        order_feedback_queue = Queue()
        order_router.update_order_feedback_queue(order_feedback_queue)
        order_router.update_data(np.array([1000]),np.array([1]))
        timestamps = np.array([0,0,0,2020,11,17,0,0,0])
        roll_order_dict = {"orders":{"1601004125":100,"1601004126":-100} ,"main2ind_ticker":{symbols[0]:{"before_ticker":"1601004125","after_ticker":"1601004126"}}}
        order_router.cross_order(dict(zip(symbols,[200])), 0,timestamps,symbols,roll_order_dict = roll_order_dict)
        order_router.run_regular_task()
        strategy_id,task_type,matrix_msg,time_array = order_feedback_queue.get()
        assert strategy_id == 0
        assert task_type == "order_feedback"
        assert (timestamps==time_array).all()
        assert (matrix_msg[OrderFeedback.transaction.value]==np.array([200])).all()
        assert (matrix_msg[OrderFeedback.current_data.value]==np.array([11000])).all()
        assert (matrix_msg[OrderFeedback.current_fx_data.value]==np.array([1])).all()
        assert (matrix_msg[OrderFeedback.commission_fee.value]==np.array([1010])).all()
        assert (matrix_msg[OrderFeedback.order_status.value]==np.array([OrderStatus.FILLED.value])).all()
        assert (matrix_msg[OrderFeedback.timestamps.value]==np.array([0])).all()


    @mock.patch('quantcycle.app.order_crosser.order_router.postPortfolioTask',new=mock_postPortfolioTask_4)
    @mock.patch('quantcycle.app.order_crosser.order_router.GetTask',new=mock_GetTask_4)
    @mock.patch('quantcycle.app.order_crosser.order_router.checkSubAccountHolding',new=mock_checkSubAccountHolding_1)
    @mock.patch('quantcycle.app.order_crosser.order_router.generateSubTasks',new=mock_generateSubTasks_1)
    @mock.patch('quantcycle.app.order_crosser.order_router.GetSubTask',new=mock_GetSubTask_1)
    @mock.patch('quantcycle.app.order_crosser.order_router.reviewSingleTask',new=mock_reviewSingleTask_1)
    def test_order_router_place_order_04(self):
        symbols = ["HK_10_2800"]
        data_info = {
                        "STOCKS": {
                            "DataCenter":"DataMaster",
                            "SymbolArgs":{
                                    "DMAdjustType": 0,
                                    "AccountCurrency": "LOCAL"
                                },
                            "Fields": "OHLC",
                            "Frequency": "DAILY"}
                    }
        base_ccy = "HKD"
        data_loader = DataLoader(data_info, base_ccy, {"STOCKS":symbols},{}, {}, {})

        order_router = PortfolioTaskEngineOrderRouter(order_router_config={"currency":base_ccy})
        order_router.update_symbol(symbols)
        order_router.update_data_loader(data_loader)
        order_feedback_queue = Queue()
        order_router.update_order_feedback_queue(order_feedback_queue)
        order_router.update_data(np.array([1000]),np.array([1]))
        timestamps = np.array([0,0,0,2020,11,17,0,0,0])
        order_router.cross_order(dict(zip(symbols,[100])), 0,timestamps,symbols)
        order_router.run_regular_task()
        strategy_id,task_type,matrix_msg,time_array = order_feedback_queue.get()
        assert strategy_id == 0
        assert task_type == "order_feedback"
        assert (timestamps==time_array).all()
        assert (matrix_msg[OrderFeedback.transaction.value]==np.array([100])).all()
        assert (matrix_msg[OrderFeedback.current_data.value]==np.array([11000])).all()
        assert (matrix_msg[OrderFeedback.current_fx_data.value]==np.array([1])).all()
        assert (matrix_msg[OrderFeedback.commission_fee.value]==np.array([10])).all()
        assert (matrix_msg[OrderFeedback.order_status.value]==np.array([OrderStatus.FILLED.value])).all()
        assert (matrix_msg[OrderFeedback.timestamps.value]==np.array([0])).all()

    @mock.patch('quantcycle.app.order_crosser.order_router.postPortfolioTask',new=mock_postPortfolioTask_5)
    @mock.patch('quantcycle.app.order_crosser.order_router.GetTask',new=mock_GetTask_5)
    @mock.patch('quantcycle.app.order_crosser.order_router.checkSubAccountHolding',new=mock_checkSubAccountHolding_1)
    @mock.patch('quantcycle.app.order_crosser.order_router.generateSubTasks',new=mock_generateSubTasks_1)
    @mock.patch('quantcycle.app.order_crosser.order_router.GetSubTask',new=mock_GetSubTask_1)
    @mock.patch('quantcycle.app.order_crosser.order_router.reviewSingleTask',new=mock_reviewSingleTask_1)
    def test_order_router_place_order_05(self):
        symbols = ["HK_10_2800","HK_10_700","HK_10_1"]
        data_info = {
                        "STOCKS": {"DataCenter":"DataMaster",
                        "SymbolArgs":{
                            "DMAdjustType": 0,
                            "AccountCurrency": "LOCAL"},
                        "Fields": "OHLC",
                        "Frequency": "DAILY"}
                    }
        base_ccy = "HKD"
        data_loader = DataLoader(data_info, base_ccy, {"STOCKS":symbols},{}, {}, {})

        order_router = PortfolioTaskEngineOrderRouter(order_router_config={"currency":base_ccy})
        order_router.update_symbol(symbols)
        order_router.update_data_loader(data_loader)
        order_feedback_queue = Queue()
        order_router.update_order_feedback_queue(order_feedback_queue)
        order_router.update_data(np.array([1000,1000,1000]),np.array([1,1]))
        timestamps = np.array([0,0,0,2020,11,17,0,0,0])
        order_router.cross_order(dict(zip(symbols,[100,100,0])), 0,timestamps,symbols,back_up_symbols=["HK_10_1"])
        order_router.run_regular_task()
        strategy_id,task_type,matrix_msg,time_array = order_feedback_queue.get()
        assert strategy_id == 0
        assert task_type == "order_feedback"
        assert (timestamps==time_array).all()
        assert (matrix_msg[OrderFeedback.transaction.value]==np.array([100,100,100])).all()
        assert (matrix_msg[OrderFeedback.current_data.value]==np.array([11000,10000,12000])).all()
        assert (matrix_msg[OrderFeedback.current_fx_data.value]==np.array([1,1,1])).all()
        assert (matrix_msg[OrderFeedback.commission_fee.value]==np.array([10,10,10])).all()
        assert (matrix_msg[OrderFeedback.order_status.value]==np.array([OrderStatus.FILLED.value,OrderStatus.FILLED.value,OrderStatus.FILLED.value])).all()
        assert (matrix_msg[OrderFeedback.timestamps.value]==np.array([0])).all()

    @mock.patch('quantcycle.app.order_crosser.order_router.postPortfolioTask',new=mock_postPortfolioTask_6)
    @mock.patch('quantcycle.app.order_crosser.order_router.GetTask',new=mock_GetTask_6)
    @mock.patch('quantcycle.app.order_crosser.order_router.checkSubAccountHolding',new=mock_checkSubAccountHolding_1)
    @mock.patch('quantcycle.app.order_crosser.order_router.generateSubTasks',new=mock_generateSubTasks_1)
    @mock.patch('quantcycle.app.order_crosser.order_router.GetSubTask',new=mock_GetSubTask_1)
    @mock.patch('quantcycle.app.order_crosser.order_router.reviewSingleTask',new=mock_reviewSingleTask_1)
    def test_order_router_place_order_06(self):
        stock_symbols = ["HK_10_2800","HK_10_700","HK_10_1"]
        future_symbols = ["1603000205"]

        symbols = stock_symbols + future_symbols
        data_info = {
                        "STOCKS": {"DataCenter":"DataMaster",
                        "SymbolArgs":{
                            "DMAdjustType": 0,
                            "AccountCurrency": "LOCAL"},
                        "Fields": "OHLC",
                        "Frequency": "DAILY"},

                        "FUTURES": {
                            "DataCenter": "DataMaster",
                            "SymbolArgs": {
                                    "DMAdjustType": 0
                                },
                            "Fields": "OHLC",
                            "Frequency": "DAILY"
                            }
                    }
        base_ccy = "HKD"
        data_loader = DataLoader(data_info, base_ccy, {"STOCKS":stock_symbols,"FUTURES":future_symbols},{}, {}, {})

        order_router = PortfolioTaskEngineOrderRouter(order_router_config={"currency":base_ccy})
        order_router.update_symbol(symbols)
        order_router.update_data_loader(data_loader)
        order_feedback_queue = Queue()
        order_router.update_order_feedback_queue(order_feedback_queue)
        order_router.update_data(np.array([1000,1000,1000,1000]),np.array([1,1]))
        timestamps = np.array([0,0,0,2020,11,17,0,0,0])

        order_router.receive_pending_order(dict(zip(symbols,[100,100,0,0])), 0,timestamps,symbols,back_up_symbols=["HK_10_1"])
        order_router.receive_pending_order(dict(zip(symbols,[0,0,0,100])), 0,timestamps,symbols,back_up_symbols=[])
        order_router.handle_pending_order()

        order_router.run_regular_task()
        strategy_id,task_type,matrix_msg,time_array = order_feedback_queue.get()
        assert strategy_id == 0
        assert task_type == "order_feedback"
        assert (timestamps==time_array).all()
        assert (matrix_msg[OrderFeedback.transaction.value]==np.array([100,100,100,100])).all()
        assert (matrix_msg[OrderFeedback.current_data.value]==np.array([11000,10000,12000,9999])).all()
        assert (matrix_msg[OrderFeedback.current_fx_data.value]==np.array([1,1,1,1])).all()
        assert (matrix_msg[OrderFeedback.commission_fee.value]==np.array([10,10,10,10])).all()
        assert (matrix_msg[OrderFeedback.order_status.value]==np.array([OrderStatus.FILLED.value]*4)).all()
        assert (matrix_msg[OrderFeedback.timestamps.value]==np.array([0])).all()





