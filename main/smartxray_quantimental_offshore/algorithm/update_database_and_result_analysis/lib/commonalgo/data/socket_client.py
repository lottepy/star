# -*- coding: utf-8 -*-
# @Time    : 8/5/2019 9:05 AM
# @Author  : Joseph Chen
# @Email   : joseph.chen@magnumwm.com
# @FileName: socket_client.py
# @Software: PyCharm

import json
import threading
import socket
import time
import ast
import uuid
from functools import partial
import requests

class SocketClient():
    """
    params: iuid_list, broker_id, account_id, tick_callback, orderbook_callback, order_callback, execution_callback
    pass the ones you need

    E.g.

        c = SocketClient(iuid_list=['CN_10_000001','CN_10_600000'],
                 broker_id='101',
                 account_id='1888000385',
                 tick_callback=lambda x: print(f"I receive tick: {x}"),
                 orderbook_callback=lambda x: print(f"I receive orderbook: {x}"),
                 order_callback=lambda x: print(f"I receive order: {x}"),
                 execution_callback=lambda x: print(f"I receive execution: {x}"),)
        time.sleep(120)
        print(c.tick_data)
        print(c.orderbook_data)
        print(c.order_data)
        print(c.execution_data)
    """
    HOST = '192.168.8.79'
    PORT_MDS = 15566
    PORT_OES = 15565

    def __init__(self, **kwargs):
        """Pass the parameters you need.

        Parameters
        ----------
        iuid_list : list, optional
            A list of iuids
        broker_id : str, optional
            Has to be with account_id if passed
        account_id : int, optional
            Has to be with broker_id if passed
        tick_callback : function, optional
        orderbook_callback : function, optional
        order_callback : function, optional
        execution_callback : function, optional
        """

        # Overwrite host and ports if parameters passed
        if kwargs.get('HOST'):
            self.HOST = kwargs.get('HOST')
        if kwargs.get('PORT_OES'):
            self.PORT_OES = kwargs.get('PORT_OES')
        if kwargs.get('PORT_MDS'):
            self.PORT_MDS = kwargs.get('PORT_MDS')

        assert bool(kwargs.get('iuid_list')) >= bool(
            kwargs.get('tick_callback')), "You provided tick_callback without proving iuid_list."
        assert bool(kwargs.get('iuid_list')) >= bool(
            kwargs.get('orderbook_callback')), "You provided orderbook_callback without proving iuid_list."
        assert bool(kwargs.get('broker_id')) == bool(
            kwargs.get('account_id')), "You should provide both / none of broker_id and account_id."
        assert bool(kwargs.get('broker_id')) >= bool(
            kwargs.get('order_callback')), "You provided order_callback without proving broker_id & account_id."
        assert bool(kwargs.get('broker_id')) >= bool(
            kwargs.get('execution_callback')), "You provided execution_callback without proving broker_id & account_id."

        self.tick_args = {
            'name': 'tick',
            'exchange': 'market.tick',
            'routing_keys': [
                id.replace("_", ".") if not "full" in id else id.replace("_", ".").replace(".full", "_full") for id in
                kwargs.get('iuid_list')],
            'callback': kwargs.get('tick_callback') if kwargs.get('tick_callback') else (lambda x: x)
        } if kwargs.get('iuid_list') else None

        self.orderbook_args = {
            'name': 'orderbook',
            'exchange': 'market.orderbook',
            'routing_keys': self.tick_args['routing_keys'],
            'callback': kwargs.get('orderbook_callback') if kwargs.get('orderbook_callback') else (lambda x: x)
        } if kwargs.get('iuid_list') else None

        self.order_args = {
            'name': 'order',
            'exchange': 'trading',
            'routing_keys': ["order.{}.{}".format(kwargs.get('broker_id'), kwargs.get('account_id'))],
            'callback': kwargs.get('order_callback') if kwargs.get('order_callback') else (lambda x: x)
        } if kwargs.get('broker_id') else None

        self.execution_args = {
            'name': 'execution',
            'exchange': 'trading',
            'routing_keys': ["execution.{}.{}".format(kwargs.get('broker_id'), kwargs.get('account_id'))],
            'callback': kwargs.get('execution_callback') if kwargs.get('execution_callback') else (lambda x: x)
        } if kwargs.get('broker_id') else None

        self.args = {
            k: v for k, v in (
            ('tick', self.tick_args),
            ('orderbook', self.orderbook_args),
            ('order', self.order_args),
            ('execution', self.execution_args)
        ) if v is not None
        }

        self.data = {
            'tick': {},
            'orderbook': {},
            'order': {},
            'execution': {}
        }

        if kwargs.get('iuid_list'):
            self.iuid_list = kwargs.get('iuid_list')

        self.callback = partial(self._callback, ch=None, properties=None)
        self.running = True
        self._init_socket()

    def _init_socket(self):
        if hasattr(self, 'iuid_list'):
            self.update_subscribe_list(self.iuid_list)  # 先向服务器订阅

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.socket.connect((self.HOST, self.PORT_MDS))
            for iuid in self.iuid_list:
                self.socket.sendall(f'{iuid}\n'.encode())  # 再请后端发送数据给自己
        except ConnectionRefusedError as e:
            raise ConnectionError(f"Socket client fails to start because {e}.")
        t = threading.Thread(target=self.start_consuming, name='Socket Client Thread')
        t.setDaemon(True)
        t.start()

    def start_consuming(self):
        data_all = ''
        while self.running:
            try:
                streamBytes = self.socket.recv(1024)
            except socket.error:
                if not self.running:  # 使退出程序时不报错
                    return
            data_all += streamBytes.decode()  # streamBytes是可能被任意截断后的byte stream  需转为str
            _index = data_all.find('\n')
            while _index != -1 and len(data_all) > 1:  # len(data_all)>1 是因为避免data_all='\n'时导致报错
                data, others = data_all[:_index], data_all[_index+1:]  # 如果包含换行符 换行符前的data数据必然是完整的  
                data_dict = ast.literal_eval(data)
                if data_dict.get('quoteId'):
                    iuid = data_dict.get('iuid').replace("_", ".")
                    # TODO: socket client don't need routing key
                    Method = type('Method', (object,), dict(routing_key=iuid))
                    if data_dict.get("last") and (data_dict.get("a1") > 0 or data_dict.get("b1") > 0): # it's a snapshot  注意涨跌停的情况
                        self.callback(method=Method(), body=data, message_type='tick')
                        # self.callback(method=Method(), body=data, message_type='orderbook')
                    elif (data_dict.get("a1") or data_dict.get("b1")): # it's an order
                        self.callback(method=Method(), body=data, message_type='orderbook')
                    elif data_dict.get("last"): # it's a trade
                        self.callback(method=Method(), body=data, message_type='tick')
                if data_dict.get('localId'):
                    broker_id = data_dict.get('brokerId')
                    broker_account = data_dict.get('brokerAccount')
                    if data_dict.get('extExecutionId'):
                        Method = type('Method', (object,), dict(routing_key=f"execution.{broker_id}.{broker_account}"))
                        self.callback(method=Method(), body=data, message_type='execution')
                    elif data_dict.get('extOrderId'):
                        Method = type('Method', (object,), dict(routing_key=f"order.{broker_id}.{broker_account}"))
                        self.callback(method=Method(), body=data, message_type='order')
                data_all = others  # 处理后续数据
                _index = data_all.find('\n')

    def _callback(self, ch, method, properties, body, message_type=None):
        data = json.loads(body) # ast.literal_eval(data)
        # print(method.routing_key, message_type)
        # print(data)
        if data.get('quoteId'):  # tick and orderbook
            iuid = method.routing_key.replace(".", "_")
            self.data[message_type][iuid] = data
            data['iuid'] = iuid
        else:                    # order and execution
            self.data[message_type][method.routing_key.replace(".", "_") + "_" + str(data.get('id'))] = data
        self.args[message_type]['callback'](data)

    def update_subscribe_list(self, iuid_list):
        """subscribe the needed info (i.e., orderbook, tick, trades) from data vendors (i.e., futu, Bloomberg) before using them by `self.callback` function.
        """
        assert all('CN_10_' in iuid or 'CN_60_' in iuid for iuid in iuid_list), "Only `CN_10_` or `CN_60_` can be subscribed."
        url = f"http://{self.HOST}:{self.PORT_OES}/api/v3/market/subscribe"
        if all('CN_10_' in iuid for iuid in iuid_list):
            channel = "KR"
        elif all('CN_60_' in iuid for iuid in iuid_list):
            channel = "CTP"
        else:
            raise ValueError("Currently do not support mixed subscription!")
        params = {'iuids': ",".join(iuid_list), 'channel': channel}
        req_id = uuid.uuid4().hex
        headers = {'Content-Type': 'application/json', 'x-request-id': req_id}
        with requests.Session() as sess:
            print(f"If you find the subscription is not successful, please send the following string to the person in charge: {req_id}")
            resp = sess.post(url, params=params, headers=headers).json()
            assert resp['status']['ecode'] == 0, 'subscribe error!'

    def disconnect_client(self):
        self.running = False
        close_res = self.socket.close()
        return close_res

    @property
    def tick_data(self):
        return self.data['tick']

    @property
    def orderbook_data(self):
        return self.data['orderbook']

    @property
    def order_data(self):
        return self.data['order']

    @property
    def execution_data(self):
        return self.data['execution']


if __name__ == "__main__":
    c = SocketClient(iuid_list=['CN_10_000002'],  # 'CN_60_MA2002', 'CN_10_600000'
                     # ['CN_10_000007', 'CN_10_159901', 'CN_10_600009', 'CN_10_600028', 'CN_10_000002',
                     #            'CN_10_600006', 'CN_10_600007', 'CN_10_510050', 'CN_10_000006', 'CN_10_600000',
                     #            'CN_10_000004', 'CN_10_600004', 'CN_10_600010', 'CN_10_000005', 'CN_10_000008', 'CN_10_000001'],
                broker_id='10',
                account_id='1888000385',
                tick_callback=lambda x: print(f"I receive tick: {x}"),
                orderbook_callback=lambda x: print(f"I receive orderbook: {x}"),
                order_callback=lambda x: print(f"I receive order: {x}"),
                execution_callback=lambda x: print(f"I receive execution: {x}"),)
    time.sleep(30)
    print(c.tick_data)
    # print(c.orderbook_data)  # same data with tick_data
    print(c.order_data)
    print(c.execution_data)
