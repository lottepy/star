from datetime import datetime

import numba as nb
import numpy as np
from numba.experimental import jitclass

from quantcycle.utils.production_constant import (MAX_SELL_AMOUNT,
                                                  InstrumentType,
                                                  OrderFeedback, OrderStatus,
                                                  PmsRejectReason, PmsStatus,
                                                  TradeStatus)


@jitclass({
    'ccy_matrix': nb.float64[:, :],
    'n_security': nb.int64,
    'n_ccy': nb.int64,
    'commission': nb.float64[:],
    'instrument_type_array': nb.int64[:],
    'trade_status': nb.int64[:],
    'pending_order': nb.int64[:],
})

class OrderManager:

    def __init__(self, commission, ccy_matrix, instrument_type_array):

        self.ccy_matrix = np.copy(ccy_matrix)
        self.n_security = self.ccy_matrix.shape[1]
        self.n_ccy = self.ccy_matrix.shape[0]
        self.commission = np.copy(commission)
        self.instrument_type_array = np.copy(instrument_type_array)
        self.trade_status = ( np.ones(self.n_security) * TradeStatus.NORMAL.value).astype(np.int64)
        self.pending_order = np.zeros(self.n_security).astype(np.int64)

    def reset_trade_status(self, trade_status):
        valid_status = {TradeStatus.NORMAL.value, TradeStatus.STOP.value, TradeStatus.STOPBUY.value,
                        TradeStatus.STOPSELL.value}
        if False in [x in valid_status for x in trade_status]:
            print(trade_status)
            print('^')
            raise Exception("trade_status not in TradeStatus enum")
        self.trade_status = np.copy(trade_status)

    def finish_pending_order(self, execution_current_data, execution_current_fx_data, strategy_id, timestamps, price):
        order = np.zeros(self.n_security).astype(np.int64)
        strategy_id, matrix_msg = self.cross_order(order, execution_current_data, execution_current_fx_data, strategy_id, timestamps, price,delay = None)
        return strategy_id, matrix_msg

    def cross_order(self, order, execution_current_data, execution_current_fx_data, strategy_id, timestamps, price,delay = None):
        
        # select pending order which is normal for trade in momery
        normal_identifier = self.trade_status == TradeStatus.NORMAL.value
        execution_pending_order = np.zeros(self.n_security).astype(np.int64)
        execution_pending_order[normal_identifier] = self.pending_order[normal_identifier]
        # del selected pending order in momery
        self.pending_order[normal_identifier] = 0

        temp_order = np.zeros(self.n_security).astype(np.int64)
        if delay is None:
            temp_order = np.copy(order)
        else:
            delay_identifier = delay != 0
            no_delay_identifier = ~delay_identifier
            temp_order[no_delay_identifier] = order[no_delay_identifier]
            pending_order = np.zeros(self.n_security).astype(np.int64)
            pending_order[delay_identifier] = order[delay_identifier]
            self.pending_order += pending_order

        temp_order = temp_order + execution_pending_order

        price_nan_idenfiter = np.isnan(price)
        execution_current_data_nan_idenfiter = np.isnan(execution_current_data)
        execution_current_fx_data_current_data_nan_idenfiter = np.isnan(execution_current_fx_data)
        order_nan_idenfiter = np.isnan(temp_order)

        transaction = np.copy(temp_order)

        buy_reject_transaction = (transaction > 0) & (price < execution_current_data)
        sell_reject_transaction = (transaction < 0) & (price > execution_current_data)
        transaction[buy_reject_transaction] = 0
        transaction[sell_reject_transaction] = 0
        transaction[price_nan_idenfiter] = 0
        transaction[execution_current_data_nan_idenfiter] = 0
        transaction[execution_current_fx_data_current_data_nan_idenfiter] = 0
        transaction[order_nan_idenfiter] = 0

        zero_transaction_idenfiter = transaction == 0


        commission_fee = np.abs(transaction) * self.commission * execution_current_fx_data * execution_current_data
        commission_fee[zero_transaction_idenfiter] = 0

        order_status = ( (transaction==temp_order)*OrderStatus.FILLED.value ) \
                            + ( ((np.absolute(transaction) < np.absolute(temp_order)) & (transaction != 0)) * OrderStatus.PARTLY_FILLED.value ) \
                            + ( ( (transaction == 0) & (temp_order != 0) ) *OrderStatus.REJECTED.value )

        matrix_msg = np.zeros((6, len(transaction)))
        matrix_msg[OrderFeedback.transaction.value] = np.copy(transaction)
        matrix_msg[OrderFeedback.current_data.value] = np.copy(execution_current_data)
        matrix_msg[OrderFeedback.current_fx_data.value] = np.copy(execution_current_fx_data)
        matrix_msg[OrderFeedback.commission_fee.value] = np.copy(commission_fee)
        matrix_msg[OrderFeedback.order_status.value] = np.copy(order_status)
        matrix_msg[OrderFeedback.timestamps.value, :] = timestamps
        return strategy_id, matrix_msg

if __name__ == "__main__":
    ccy_matrix = np.array([[1, 1, 0, 1], [0, 0, 1, 0]], dtype=np.float64)
    comission = np.array([0, 0, 0, 0], dtype=np.float64)
    instrument_type = np.array([InstrumentType.FX.value, InstrumentType.HK_STOCK.value, InstrumentType.CN_STOCK.value,
                                InstrumentType.US_STOCK.value])
    order_manager = OrderManager(comission, ccy_matrix, instrument_type)

    current_data = np.array([20, 40, 100, 120], dtype=np.float64)
    current_fx_data = np.array([2, 4], dtype=np.float64) @ ccy_matrix
    ts = 1591325065
    order = np.array([100, 100, 100, 100], dtype=np.float64)
    price = np.array([21, 41, 101, 121], dtype=np.float64)
    id = 0


    test_count = 10000

    t_s = datetime.now()
    for i in range(test_count):
        feedback_id, order_feedback_msg = order_manager.cross_order(order, current_data, current_fx_data, id, ts,
                                                                    price)
    t_e = datetime.now()
    print((t_e-t_s)/10000)
