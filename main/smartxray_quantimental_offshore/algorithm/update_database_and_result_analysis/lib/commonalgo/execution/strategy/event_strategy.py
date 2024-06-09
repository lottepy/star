# -*- coding: utf-8 -*-
# @Time    : 9/5/2019 11:05 AM
# @Author  : Joseph Chen
# @Email   : joseph.chen@magnumwm.com
# @FileName: event_strategy.py
# @Software: PyCharm

import argparse
import collections
import copy
import logging
import threading
import time
from queue import Queue

from ...data import RQClient, socket_client
from ..config.constant import OrderCompletionStatus, OrderStatus
from ..log.get_logger import get_logger
from ..trade.order_handler import OrderSubmitter

# from multiprocessing import

# TODO: gevent?


class EventStrategyBase:
    def __init__(self, iuids,config,logger=get_logger()):
        self.logger = logger
        self.iuids = iuids
        self.config = config
        self.logger.info('EventStrategyBase init finished')


    def run(self):
        #self.executor = TestExecutor(self._opts, self.iuids, self.on_tick, self.on_trade, self.on_info,self.logger)
        self.executor = OrderSubmitter(self.config, self.iuids, self.on_tick, self.on_trade, self.on_info,self.logger)
        self.executor.run()

    def on_tick(self, data):
        """Callback function for tick and orderbook data. Some message may
        contains no tick or orderbook data in the begining, as it has not been
        received yet.
        Args
            data: dict with fields {'b1', 'bv1', 'av1', 'a1', 
                'last', 'vol', 'last_vol', 'iuid', 'timestamp'}
        """
        # self.tick_data.update(data)
        pass

    def on_trade(self, data):
        """Callback function for trading message.
        Args:
            data: dict with fields {'id', 'createTime', 'updateTime', 'localId',
                'batchId', 'extOrderId', 'brokerId', 'brokerAccount',
                'subAccount', 'iaccountId', 'portfolioId', 'direction',
                'offsetFlag', 'hedgeFlag', 'iuid', 'price', 'quantity',
                'amount', 'priceType', 'status', 'remark', 'filledPrice',
                'filledQuantity', 'filledAmount', 'commission', 'source',
                'placeTime', 'advisingType', 'completed', 'unfillQuantity',
                'flatPositionType', 'brokerAccountKey'}
        """
        pass

    def on_info(self, data):
        pass

    def run_forever(self):
        self.logger.info("Run forever")
        while True:
            time.sleep(10)


class TestEventExecutor:
    def __init__(self, opts, iuid_list, on_tick, on_trade, on_info,logger=get_logger()):
        self._logger = logger  # logging.getLogger(__name__)
        self._logger.debug(f"Init {self.__class__.__name__}")
        self._strategy_on_tick = on_tick
        self._strategy_on_trade = on_trade
        self._strategy_on_info = on_info
        self.order_queue = Queue()
        self._iuid_list = set(iuid_list) if not isinstance(
            iuid_list, str) else set([iuid_list])
        t = threading.Thread(target=self.run_tick_server,)
        t.daemon = True
        t.start()
        t = threading.Thread(target=self.run_order_server,)
        t.daemon = True
        t.start()
        self.on_info()
        self.execution_status = "Idle"

    def run_tick_server(self):
        while True:
            for iuid in self._iuid_list:
                msg = {'b1': 100, 'bv1': 100, 'a1': 101, 'av1': 100, 'timestamp': time.time(),
                       'Type': 'Update', 'OnMsgTime': time.time(), 'PublishTime': time.time(),
                       'iuid': iuid, 'MsgType': 'orderbook'}
                self._strategy_on_tick(msg)
                time.sleep(3/len(self._iuid_list))

    def run_order_server(self):
        while True:
            order_dict = self.order_queue.get()
            self._logger.info(f"TestExcutorServer receive order:{order_dict}")
            iuid = order_dict["symbol"]
            data = {}
            data["status"] = "FILLED"
            data["filledQuantity"] = order_dict["quantity"]
            data["filledPrice"] = order_dict["price"]
            data["direction"] = order_dict["side"]
            data["iuid"] = order_dict["symbol"]
            self._logger.info(f"TestExcutorServer return msg:{data}")
            self._strategy_on_trade(data)
            self.on_info()
            time.sleep(1)
            self.execution_status = "Busy"

    def submit_order(self, order_dict):
        self.order_queue.put(order_dict)

    def on_info(self):
        data = {"purches_power": 10000000, "current_holding": {},"short_volume": {},"base_volume": {}}
        self._strategy_on_info(data)



""" Sample Trade response:
{
'status': {
'ecode': 0, 'message': 'success'}, 'data': [{
'id': 10112,
 'createTime': 1568346903557,
 'updateTime': 1568346903557,
 'localId': 'string',
 'batchId': 'qhhr0c683w34',
 'extOrderId': None,
 'brokerId': 1,
 'brokerAccount': 'DU1526700',
 'subAccount': None,
 'iaccountId': 0,
 'portfolioId': 0,
 'direction': 'SELL',
 'offsetFlag': 'UNKONWN',
 'hedgeFlag': 'UNKONWN',
 'iuid': 'HK_10_1',
 'price': 72.4,
 'quantity': 500,
 'amount': 36200.0,
 'priceType': 'LIMIT',
 'status': 'CREATE',
 'remark': None,
 'filledPrice': 0,
 'filledQuantity': 0,
 'filledAmount': 0,
 'commission': 0,
 'source': 'string',
 'placeTime': 1568346903570,
 'advisingType': 2,
 'completed': False,
 'unfillQuantity': 500,
 'brokerAccountKey': '1.DU1526700',
 'flatPositionType': 'UNKONWN'}]}
"""
