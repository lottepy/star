#!/usr/bin/env python
import threading
import time
import queue

from ..log import get_logger
from ..market.market import market
from .order import Order


class OrderManager:
    def __init__(self, market,  trading_ctrl, paper,
                 directions, cash=0, timeout=60):
        # if directions is None:
        #     directions = pd.Series(dtype=int)
        self.logger = get_logger.logger
        self.directions = directions
        self.market = market
        self.cash = cash
        self.timeout = timeout

        self.under_submission = queue.Queue()
        self.discarded = {} # key: iuid
        self.submitting = {} # key: iuid
        self.inactive = {}  # key: order id, could have duplicated iuid
        self.trading_ctrl = trading_ctrl
        self.data_lock = threading.Lock()

    def create_orders(self, quantities):
        for iuid, v in quantities.items():
            if v == 0: # v == 0
                continue

            order = Order(iuid, v)
            self.under_submission.put(order)

            # self.update_direction(order)

    def update_direction(self, order):
        iuid = order.iuid
        if order.filledAmount <= 0:
            return
        if iuid not in self.directions or self.directions[iuid] == 0:
            self.trading_directions[iuid] = int(order.direction)

    def schedule_orders(self):
        pass

    def submit_orders(self):
        if self.under_submission.empty():
            return
        # Trading.cancel_all_active_orders(self.trading_ctrl, self.paper)
        self.cancel_submitting_orders()

        # self.discarded.update(self.submitting,)
        self.submitting = {}

        wait_for_cash_orders = []

        real_time_data = market.get_real_time_data()
        buyprice = real_time_data['BID_1'].astype('float64')
        sellprice = real_time_data['ASK_1'].astype('float64')

        while not self.under_submission.empty():
            order = self.under_submission.get()
            if order.get_direction() == 'SELL':
                price = buyprice[order.iuid]
            else: # "BUY"
                price = sellprice[order.iuid]
                if not order.is_future:
                    if order.quantity * price < self.cash:
                        wait_for_cash_orders.append(order)
                        continue
                    else:
                        self.update_cash(price * order.quantity)
            order.price = price
            self.submitting[orders.iuid] = orders

        for order in wait_for_cash_orders:
            self.under_submission.put(order)

        orders = [o.get_order_dict() for o in self.submitting.values()]

        submition_time = time.time()
        submition_result = self.trading_ctrl.submit_orders(baccount=paper['account'],
                                                            brokerid=paper['broker'],
                                                            subaccount=paper.get('subaccount'),
                                                            orders=orders_all)

        if submition_result['status']['ecode'] != 0:
            logger.error("Submitted orders failed.\n{}".format(submition_result))
            raise RuntimeError('Submitted orders failed, read log for more infomation')
        # submition_result = submition_result['data']

        while self.submitting:
            logger.info("Waiting for submission result, ",
                         f"submitting {len(self.submitting)}, ",
                         f"incativated {len(self.inactive)}.")
            if time.time() - submition_time > self.timeout:
                self.logger.info("Submission time out, resubmitting")
                self.cancel_submitting_orders()
                self.submit_orders()
            else:
                time.sleep(2)

    def update_cash(self, value):
        self.cash += value

    def set_cash(self, cash):
        self.cash = cash

    def is_waiting_for_submission(self):
        return not self.under_submission.empty()

    def is_avaiable_for_submission(self):
        return not self.submitting

    def cancel_order(self, order):
        cancel_result = self.trading_controller.cancel_order(order.oid)
        if cancel_result.get('status').get('ecode')==0:
            logger.info("Successfully cancelled unfilled orderid {}".format(oid))
        else:
            if 'status = FILLED' in cancel_result['status'].get('message'):
                self.move_order(self.submitting, self.inactive)
            else:
                logger.error("Unfilled orderid {} unable to cancel, as: {}.".format(
                    oid, cancel_result['status'].get('message')))

    def cancel_submitting_orders(self):
        if self.submitting:
            for order in self.submitting:
                self.cancel_order(order)
            wait_count = 20
            while self.submitting:
                time.sleep(1)
                log.debug("Waiting for cancel result")
                wait_count -= 1
                if wait_count <= 0:
                    raise("Error, orders cancel failed")

    def move_order(self, source, target, order):
        self.data_lock.acquire()
        if source is self.submitting:
            source.pop(order.iuid)
            self.update_cash(order.get_cash_delta())
            tid = order.oid if target is self.inactive else order.iuid
            target[tid] = order
        self.data_lock.release()

    def update_message(self, msg):
        iuid = msg['iuid']
        if iuid not in self.submitting:
            self.logger.debug(f"Order not found: {msg}")
            return
        else:
            order = self.submitting[iuid]
            order.update_message(msg)

            t = time.time()
            status = msg['status']
            self.logger.info(f"Order {iuid} {status}")

            self.update_direction(order)

            if status in {'FILLED'}:
                self.move_order(self.submitting, self.inactive)

            elif status in {'REJECTED', 'CANCELLED'}:
                # resubmit
                self.move_order(self.submitting, self.inactive)

                self.logger.info(f"Order is {status}, schedule to resubmit: {order}")

                new_quantity = order.quantity - order.filledQuantity
                o = Order(iuid, new_quantity)
                log.info(f"New created order: {order}")
                self.under_submission.put(o)
            elif t - order.submit_time > self.timeout:
                self.logger.info(f"Order {order} timeout, cancel & resubmitting")
                self.cancel_order(order)
