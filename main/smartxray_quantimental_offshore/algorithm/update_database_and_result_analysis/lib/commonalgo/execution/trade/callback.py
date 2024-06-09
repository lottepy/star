#!/usr/bin/env python
import queue

class TradingCallback:
    def __init__(self, msg_queue: queue.Queue):
        self.msg_queue = msg_queue

    def __call__(self, msg):

        self.meg_queue.put(msg, block=False)

        # self.order_status[msg.get('id')] = msg

        # # Remove `REJECTED` & `CANCELLED` as we don't want to track orders that have been rejected or cancelled.
        # if msg.get('status') in {'REJECTED', 'CANCELLED'}:
        #     logger.warning('status of oid {} is {}, see details below:'.format(msg['id'], msg['status']))
        #     self.order_status.pop('msgid', None)

        logger.info('callback: {}'.format(msg))

 ### Example message:
 # {'id': 44,
 # 'createTime': 1564544724471,
 # 'updateTime': 1564544727636,
 # 'iuid': 'HK_10_2202',
 # 'price': 29.6,
 # 'quantity': 128100.0,
 # 'amount': 3791760.0,
 # 'status': 'FILLING',
 # 'remark': None,
 # 'filledPrice': 29.6,
 # 'filledQuantity': 47400.0,
 # 'filledAmount': 1403040.0,
 # 'commission': 0,
 # 'source': 'string',
 # 'placeTime': None,
 # 'advisingType': 0,
 # 'completed': False,
 # 'unfillQuantity': 80700.0,
 # 'brokerAccountKey': '1.DU1526700',
 # 'flatPositionType': 'UNKONWN'}
