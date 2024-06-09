import time

import numpy as np

from ..config import constant
from ..log import get_logger
from ..symbol.base import Symbols


class Order:
    def __init__(self, iuid, quantity, price=-1, direction=None, uid=None,source=None):
        self.iuid = iuid
        self.logger = get_logger.logger
        self.price = float(price)
        self.direction = np.sign(quantity)
        if direction:
            if isinstance(direction, int):
                self.direction = direction
            else:
                self.direction = 1 if direction == "BUY" else -1
        self.quantity = abs(quantity)  # np.int64 is not serializable
        self.trading_type = "LIMIT"
        self.source = source

        self.is_future = Symbols.is_future_iuid(iuid)

        self.oid = None  # order id
        self.create_time = time.time()
        self.submit_time = -1
        self.status = ""
        self.msg_update_time = -1

        self.filledQuantity = 0
        self.filledAmount = 0
        self.commission = 0
        # if not uid:
        #     uid = trading_utils.get_uuid_by_thread()
        # self.uid = uid

    def __str__(self):
        d = self.get_order_dict()
        d.update({'oid': self.oid,
                  'filledQuantity': self.filledQuantity,})
        return str(d)

    @property
    def order_dict(self):
        return self.get_order_dict()

    @property
    def side(self):
        return self.get_direction()

    @property
    def cash_reserved(self):
        """The cash needed to be reserved.
        """
        if self.is_future:
            return 0
        if self.side == constant.TradingDirection.SELL:
            return 0
        else:
            return self.quantity*self.price

    @property
    def cash_spent(self):
        amount = 0
        if not self.is_future:
            if self.side == constant.TradingDirection.SELL:
                amount -= self.filledAmount
            else:
                amount += self.filledAmount
            
        amount += self.commission
        return amount
                

    def get_direction(self):
        if self.direction > 0:
            return constant.TradingDirection.BUY
        elif self.direction < 0:
            return constant.TradingDirection.SELL
        else:
            return None

    def get_order_dict(self):
        dic = {
            "symbol": self.iuid,
            "side": self.get_direction(),
            "type": self.trading_type,
            "price": self.price,
            "quantity": self.quantity,
            "source": self.source,
        }
        return dic

    def submit(self):
        self.submit_time = time.time()

    def update_message(self, msg):
        if not msg:
            self.logger.info(f"Message is None for {self.order_dict}")
            return False
        if self.oid and self.oid != msg['id']:
            self.logger.warning(f"Msg id mismatch: {msg['id']} {self.oid} {msg} {self.iuid}")
            return False

        status = msg['status']
        new_update_time = msg['updateTime'] / 1000

        if self.msg_update_time > new_update_time:
            return

        self.msg_update_time = new_update_time
        self.status = status
        self.oid = msg['id']
        self.filledQuantity = msg['filledQuantity']
        self.filledAmount = msg['filledAmount']
        self.commission = msg['commission']
        return True

    def get_cash_delta(self):
        """Deprecated.
        """
        if self.is_future:
            return 0
        if self.get_direction() == 'BUY':
            cash = self.price * self.quantity - self.filledAmount
        else:
            cash = self.filledAmount
        cash -= self.commission
        return cash

    def update_status(self, status):
        self.status = status

    @staticmethod
    def create_order(order_dict):
        order = Order(order_dict['symbol'], order_dict['quantity'],
                        order_dict['price'], order_dict['side'], source=order_dict['source'])
        return order
