#!/usr/bin/env python
# date: 06/11/2018
# author: Xiandong QI, MC,
# copyright: AQUMON
# description: a basic wrapper for algorithm team to subscribe/use the real time data （market.orderbook, personal.trades viw rabbitMQ.

import json
import threading
from functools import partial

import pika
from requests import Session

_session = Session()


class MarketConsumerClient(object):
    """A basic wrapper for algorithm team to subscribe/use the real time data （market.orderbook, personal.trades viw rabbitMQ.

    # Test:  (To exit press CTRL+C')
    line[1]: from market_consumer_client import MarketConsumerClient
    line[2]: m = MarketConsumerClient(['HK_60_MHIX8', ], "2", "111111")
    """

    def __init__(self, iuid_list, broker_id, account_id, on_order=None):
        """
        Initialize the MarketConsumerClient class with:
        1. iuid_list (list): ['HK_10_700', 'HK_10_390', ... ] or ['HK.10.700', 'HK.10.390', ... ]
        2. broker_id (string): "1" (IB), "2" (test)
        3. account_id (string): "111111"

        Notice: the class instance would `subscribe` the info (i.e., orderbook, tick, trading.order, trading.execution) of each iuid automatically.

        Output example:
        {'order.2.111111': {'id': 190, 'createTime': 1541584417975, 'updateTime': 1541584417975, 'localId': '0', 'batchId': '1111', 'extOrderId': None, 'brokerId': 2, 'brokerAccount': '111111', 'iaccountId': 1, 'portfolioId': 1, 'direction': 'Buy', 'iuid': 'US_ETF_700',
            'price': 100, 'quantity': 100, 'amount': 10000, 'priceType': 'Limit', 'status': 'Created', 'remark': None, 'filledPrice': 0, 'filledQuantity': 0, 'filledAmount': 0, 'commission': 0, 'currency': 'USD', 'source': 'test', 'placeTime': None, 'brokerAccountKey': '2.111111'}}
        {'execution.2.111111': {'id': 124, 'createTime': 1541584418722, 'updateTime': 1541584418722, 'orderId': 190, 'localId': '0', 'batchId': '1111', 'brokerId': 2, 'brokerAccount': '111111', 'extExecutionId': '892c1faf-1921-4e43-95c6-a378512fc8da',
            'extOrderId': None, 'iaccountId': 1, 'portfolioId': 1, 'iuid': 'US_ETF_700', 'direction': 'Buy', 'filledPrice': 100, 'filledQuantity': 100, 'filledAmount': 10000, 'currency': 'USD', 'brokerAccountKey': '2.111111'}}
        """
        # ['HK_10_700', 'HK_10_390', ... ] -> ['HK.10.700', 'HK.10.390', ... ]
        self.iuid_list = [id.replace("_", ".") for id in iuid_list]
        self.broker_id = broker_id
        self.account_id = account_id  # your trading account
        self.orderbook_data = {}
        self.tick_data = {}
        self.trading_order_data = {}
        self.trading_execution_data = {}
        self.on_order = on_order
        # `subscribe` the info before using with (standard format) iuid = ['HK_10_700', 'HK_10_390', ... ]
        self.update_subscribe_list(iuid_list)

        # begin the preparation of RabbitMQ
        credentials = pika.PlainCredentials('aqumon_algo', 'aqumon2050')
        connection_param = pika.ConnectionParameters(
            host='rabbitmq.aqumon.com',
            credentials=credentials,
            virtual_host='/v3'
        )
        self.connection = pika.BlockingConnection(connection_param)
        channel = self.connection.channel()

        # 4 queues
        result = channel.queue_declare(exclusive=True, auto_delete=True)
        queue_name_orderbook = result.method.queue
        result = channel.queue_declare(exclusive=True, auto_delete=True)
        queue_name_tick = result.method.queue
        result = channel.queue_declare(exclusive=True, auto_delete=True)
        queue_name_trading_order = result.method.queue
        result = channel.queue_declare(exclusive=True, auto_delete=True)
        queue_name_trading_execution = result.method.queue

        # bind 4 queues to 4 exchanges(data sources)
        for binding_key in self.iuid_list:
            channel.queue_bind(
                exchange='market.orderbook',
                queue=queue_name_orderbook,
                routing_key=binding_key
            )
            channel.queue_bind(
                exchange='market.tick',
                queue=queue_name_tick,
                routing_key=binding_key
            )

        channel.queue_bind(
            exchange='trading',
            queue=queue_name_trading_order,
            routing_key="order.{}.{}".format(self.broker_id, self.account_id)
        )
        channel.queue_bind(
            exchange='trading',
            queue=queue_name_trading_execution,
            routing_key="execution.{}.{}".format(self.broker_id, self.account_id)
        )

        # finish preparation
        # print(' [*] Waiting for logs. To exit press CTRL+C')

        # 4 basic_consume for 4 queues
        orderbook_consume = partial(self.callback, message_type='orderbook')
        channel.basic_consume(
            orderbook_consume,
            queue=queue_name_orderbook,
            no_ack=True
        )

        tick_consume = partial(self.callback, message_type='tick')
        channel.basic_consume(
            tick_consume,
            queue=queue_name_tick,
            no_ack=True
        )

        trading_consume = partial(self.callback, message_type='trading.order')
        channel.basic_consume(
            trading_consume,
            queue=queue_name_trading_order,
            no_ack=True
        )

        trading_consume = partial(self.callback, message_type='trading.execution')
        channel.basic_consume(
            trading_consume,
            queue=queue_name_trading_execution,
            no_ack=True
        )

        t = threading.Thread(target=channel.start_consuming)
        t.start()

    def callback(self, ch, method, properties, body, message_type=None):
        """Algorithm team has to define and implement their own requirement

        Explanation:
        - "ch" is the "channel" over which the communication is happening.
        - "method" is meta information regarding the message delivery
        - "properties" of the message are user-defined properties on the message.
        - "body" == "message"
        """
        # print("%r:%r" % (method.routing_key, json.loads(body)))
        data = json.loads(body)
        if message_type == 'orderbook':
            self.orderbook_data[method.routing_key] = data
            # print("orderbook_data: ", data)
        elif message_type == 'tick':
            self.tick_data[method.routing_key] = data
            # print("tick_data: ", data)
        elif message_type == 'trading.order':
            self.trading_order_data[method.routing_key] = data
            self.on_order(self.trading_order_data)
            # print(self.trading_order_data)
        elif message_type == 'trading.execution':
            self.trading_execution_data[method.routing_key] = data
            # print(self.trading_execution_data)

    def update_subscribe_list(self, iuid_list):
        """subscribe the needed info (i.e., orderbook, tick, trades) from data vendors (i.e., futu, Bloomberg) before using them by `self.callback` function.
        """
        url = 'https://market.aqumon.com/v1/market/realtime/subscription'
        params = {
            'iuids': ','.join(iuid_list),
        }
        response = _session.post(url, json=params).json()
        return response
    def disconnect_client(self):
        close_res = self.connection.close()
        return close_res


# if __name__ == '__main__':
    # client = MarketConsumerClient(['HK_10_2822', 'HK_10_3140', 'GB_40_USDHKD'], 0, 0, on_order=lambda x: print(x))
#     client = MarketConsumerClient(['CN_10_600000', 'CN_10_000001'], 0, 0, on_order=lambda x: print(x))
