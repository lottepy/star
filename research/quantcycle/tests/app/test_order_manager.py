import os
import unittest

import numpy as np

from quantcycle.app.order_crosser.order_manager import OrderManager
from quantcycle.utils.production_constant import InstrumentType, OrderStatus,OrderFeedback,TradeStatus


class TestOrderManager(unittest.TestCase):

    def test_order_feedback_msg_01(self):
        ccy_matrix = np.array([[1, 1, 0, 1], [0, 0, 1, 0]], dtype=np.float64)
        comission = np.array([0, 0, 0, 0], dtype=np.float64)
        instrument_type = np.array([InstrumentType.FX.value, InstrumentType.HK_STOCK.value,InstrumentType.CN_STOCK.value, InstrumentType.US_STOCK.value])
        order_manager = OrderManager(comission, ccy_matrix, instrument_type)
        

        current_data = np.array([20, 40, 100, 120], dtype=np.float64)
        current_fx_data = np.array([2, 4], dtype=np.float64) @ ccy_matrix
        ts = 1591325065
        order = np.array([100, 100, 100, 100], dtype=np.float64)
        id = 0
        feedback_id,order_feedback_msg = order_manager.cross_order(order,current_data,current_fx_data,id,ts, current_data)

        assert (order_feedback_msg[OrderFeedback.transaction.value]== np.array([100, 100, 100, 100], dtype=np.float64)).all()
        assert (order_feedback_msg[OrderFeedback.current_data.value] == np.array([20, 40, 100, 120], dtype=np.float64)).all()
        assert (order_feedback_msg[OrderFeedback.commission_fee.value] == np.array([0, 0, 0, 0], dtype=np.float64)).all()
        assert (order_feedback_msg[OrderFeedback.current_fx_data.value] == np.array([2, 2, 4, 2], dtype=np.float64)).all()
        assert (order_feedback_msg[OrderFeedback.order_status.value] == np.array([OrderStatus.FILLED.value, OrderStatus.FILLED.value, OrderStatus.FILLED.value, OrderStatus.FILLED.value],
                                                                                 dtype=np.float64)).all()
        assert (feedback_id == id  )
        self.assertTrue(True)

    def test_order_feedback_msg_02(self):
        ccy_matrix = np.array([[1, 1, 0, 1], [0, 0, 1, 0]], dtype=np.float64)
        comission = np.array([0, 0, 0, 0], dtype=np.float64)
        instrument_type = np.array([InstrumentType.FX.value, InstrumentType.HK_STOCK.value,InstrumentType.CN_STOCK.value, InstrumentType.US_STOCK.value])
        order_manager = OrderManager(comission, ccy_matrix, instrument_type)
        

        current_data = np.array([20, 40, 100, 120], dtype=np.float64)
        current_fx_data = np.array([2, 4], dtype=np.float64) @ ccy_matrix
        ts = 1591325065
        order = np.array([100, 100, 100, 100], dtype=np.float64)
        price = np.array([21, 41, 101, 121], dtype=np.float64)
        id = 0
        feedback_id,order_feedback_msg = order_manager.cross_order(order,current_data,current_fx_data,id,ts,price)


        assert (order_feedback_msg[OrderFeedback.transaction.value]== np.array([100, 100, 100, 100], dtype=np.float64)).all()
        assert (order_feedback_msg[OrderFeedback.current_data.value] == np.array([20, 40, 100, 120], dtype=np.float64)).all()
        assert (order_feedback_msg[OrderFeedback.commission_fee.value] == np.array([0, 0, 0, 0], dtype=np.float64)).all()
        assert (order_feedback_msg[OrderFeedback.current_fx_data.value] == np.array([2, 2, 4, 2], dtype=np.float64)).all()
        assert (order_feedback_msg[OrderFeedback.order_status.value] == np.array([OrderStatus.FILLED.value, OrderStatus.FILLED.value, OrderStatus.FILLED.value, OrderStatus.FILLED.value],
                                                                                 dtype=np.float64)).all()
        assert (feedback_id == id  )
        self.assertTrue(True)

    def test_order_feedback_msg_03(self):
        ccy_matrix = np.array([[1, 1, 0, 1], [0, 0, 1, 0]], dtype=np.float64)
        comission = np.array([0, 0, 0, 0], dtype=np.float64)
        instrument_type = np.array([InstrumentType.FX.value, InstrumentType.HK_STOCK.value,InstrumentType.CN_STOCK.value, InstrumentType.US_STOCK.value])
        order_manager = OrderManager(comission, ccy_matrix, instrument_type)
        
        current_data = np.array([20, 40, 100, 120], dtype=np.float64)
        current_fx_data = np.array([2, 4], dtype=np.float64)  @ ccy_matrix
        ts = 1591325065
        order = np.array([100, 100, 100, 100], dtype=np.float64)
        price = np.array([19, 39, 99, 119], dtype=np.float64)
        id = 0
        feedback_id,order_feedback_msg = order_manager.cross_order(order,current_data,current_fx_data,id,ts,price)

        assert (order_feedback_msg[OrderFeedback.transaction.value]== np.array([0, 0, 0, 0], dtype=np.float64)).all()
        assert (order_feedback_msg[OrderFeedback.current_data.value] == np.array([20, 40, 100, 120], dtype=np.float64)).all()
        assert (order_feedback_msg[OrderFeedback.commission_fee.value] == np.array([0, 0, 0, 0], dtype=np.float64)).all()
        assert (order_feedback_msg[OrderFeedback.current_fx_data.value] == np.array([2, 2, 4, 2], dtype=np.float64)).all()
        assert (order_feedback_msg[OrderFeedback.order_status.value] == np.array([OrderStatus.REJECTED.value, OrderStatus.REJECTED.value, OrderStatus.REJECTED.value, OrderStatus.REJECTED.value],
                                                                                 dtype=np.float64)).all()
        assert (feedback_id == id  )
        self.assertTrue(True)

    def test_order_feedback_msg_04(self):
        ccy_matrix = np.array([[1, 1, 0, 1], [0, 0, 1, 0]], dtype=np.float64)
        comission = np.array([0, 0, 0, 0], dtype=np.float64)
        instrument_type = np.array([InstrumentType.FX.value, InstrumentType.HK_STOCK.value,InstrumentType.CN_STOCK.value, InstrumentType.US_STOCK.value])
        order_manager = OrderManager(comission, ccy_matrix, instrument_type)
        
        current_data = np.array([20, 40, 100, 120], dtype=np.float64)
        current_fx_data = np.array([2, 4], dtype=np.float64)  @ ccy_matrix
        ts = 1591325065
        order = np.array([100, 100, 100, 100], dtype=np.float64)
        price = np.array([20, 40, 100, 120], dtype=np.float64)
        id = 0
        feedback_id,order_feedback_msg = order_manager.cross_order(order,current_data,current_fx_data,id,ts,price,delay=np.array([1, 1, 0, 0], dtype=np.int64))
        

        assert (order_feedback_msg[OrderFeedback.transaction.value]== np.array([0, 0, 100, 100], dtype=np.float64)).all()
        assert (order_feedback_msg[OrderFeedback.current_data.value] == np.array([20, 40, 100, 120], dtype=np.float64)).all()
        assert (order_feedback_msg[OrderFeedback.commission_fee.value] == np.array([0, 0, 0, 0], dtype=np.float64)).all()
        assert (order_feedback_msg[OrderFeedback.current_fx_data.value] == np.array([2, 2, 4, 2], dtype=np.float64)).all()
        assert (order_feedback_msg[OrderFeedback.order_status.value] == np.array([OrderStatus.FILLED.value, OrderStatus.FILLED.value, OrderStatus.FILLED.value, OrderStatus.FILLED.value],
                                                                                 dtype=np.float64)).all()
        assert (feedback_id == id  )


        feedback_id,order_feedback_msg = order_manager.finish_pending_order(current_data, current_fx_data, id, ts, price)
        assert (order_feedback_msg[OrderFeedback.transaction.value]== np.array([100, 100, 0, 0], dtype=np.float64)).all()
        assert (order_feedback_msg[OrderFeedback.current_data.value] == np.array([20, 40, 100, 120], dtype=np.float64)).all()
        assert (order_feedback_msg[OrderFeedback.commission_fee.value] == np.array([0, 0, 0, 0], dtype=np.float64)).all()
        assert (order_feedback_msg[OrderFeedback.current_fx_data.value] == np.array([2, 2, 4, 2], dtype=np.float64)).all()
        assert (order_feedback_msg[OrderFeedback.order_status.value] == np.array([OrderStatus.FILLED.value, OrderStatus.FILLED.value, OrderStatus.FILLED.value, OrderStatus.FILLED.value],
                                                                                 dtype=np.float64)).all()
        assert (feedback_id == id  )


        self.assertTrue(True)

    def test_order_feedback_msg_04(self):
        ccy_matrix = np.array([[1, 1, 0, 1], [0, 0, 1, 0]], dtype=np.float64)
        comission = np.array([0, 0, 0, 0], dtype=np.float64)
        instrument_type = np.array([InstrumentType.FX.value, InstrumentType.HK_STOCK.value,InstrumentType.CN_STOCK.value, InstrumentType.US_STOCK.value])
        order_manager = OrderManager(comission, ccy_matrix, instrument_type)
        
        current_data = np.array([20, 40, 100, 120], dtype=np.float64)
        current_fx_data = np.array([2, 4], dtype=np.float64)  @ ccy_matrix
        ts = 1591325065
        order = np.array([100, 100, 100, 100], dtype=np.float64)
        price = np.array([20, 40, 100, 120], dtype=np.float64)
        id = 0
        feedback_id,order_feedback_msg = order_manager.cross_order(order,current_data,current_fx_data,id,ts,price,delay=np.array([1, 1, 0, 0], dtype=np.int64))
        

        assert (order_feedback_msg[OrderFeedback.transaction.value]== np.array([0, 0, 100, 100], dtype=np.float64)).all()
        assert (order_feedback_msg[OrderFeedback.current_data.value] == np.array([20, 40, 100, 120], dtype=np.float64)).all()
        assert (order_feedback_msg[OrderFeedback.commission_fee.value] == np.array([0, 0, 0, 0], dtype=np.float64)).all()
        assert (order_feedback_msg[OrderFeedback.current_fx_data.value] == np.array([2, 2, 4, 2], dtype=np.float64)).all()
        assert (order_feedback_msg[OrderFeedback.order_status.value] == np.array([OrderStatus.FILLED.value, OrderStatus.FILLED.value, OrderStatus.FILLED.value, OrderStatus.FILLED.value],
                                                                                 dtype=np.float64)).all()
        assert (feedback_id == id  )

        trade_status = np.array([TradeStatus.STOP.value,TradeStatus.STOP.value,TradeStatus.NORMAL.value,TradeStatus.NORMAL.value])
        order_manager.reset_trade_status(trade_status)
        feedback_id,order_feedback_msg = order_manager.finish_pending_order(current_data, current_fx_data, id, ts, price)
        assert (order_feedback_msg[OrderFeedback.transaction.value]== np.array([0, 0, 0, 0], dtype=np.float64)).all()

        trade_status = np.array([TradeStatus.NORMAL.value,TradeStatus.NORMAL.value,TradeStatus.NORMAL.value,TradeStatus.NORMAL.value])
        order_manager.reset_trade_status(trade_status)
        feedback_id,order_feedback_msg = order_manager.finish_pending_order(current_data, current_fx_data, id, ts, price)
        assert (order_feedback_msg[OrderFeedback.transaction.value]== np.array([100, 100, 0, 0], dtype=np.float64)).all()
        assert (order_feedback_msg[OrderFeedback.current_data.value] == np.array([20, 40, 100, 120], dtype=np.float64)).all()
        assert (order_feedback_msg[OrderFeedback.commission_fee.value] == np.array([0, 0, 0, 0], dtype=np.float64)).all()
        assert (order_feedback_msg[OrderFeedback.current_fx_data.value] == np.array([2, 2, 4, 2], dtype=np.float64)).all()
        assert (order_feedback_msg[OrderFeedback.order_status.value] == np.array([OrderStatus.FILLED.value, OrderStatus.FILLED.value, OrderStatus.FILLED.value, OrderStatus.FILLED.value],
                                                                                 dtype=np.float64)).all()
        assert (feedback_id == id  )
        self.assertTrue(True)

    def test_order_feedback_msg_05(self):
        ccy_matrix = np.array([[1, 1, 0, 1], [0, 0, 1, 0]], dtype=np.float64)
        comission = np.array([0, 0, 0, 0], dtype=np.float64)
        instrument_type = np.array([InstrumentType.FX.value, InstrumentType.HK_STOCK.value,InstrumentType.CN_STOCK.value, InstrumentType.US_STOCK.value])
        order_manager = OrderManager(comission, ccy_matrix, instrument_type)
        
        trade_status = np.array([TradeStatus.NORMAL.value,TradeStatus.NORMAL.value,TradeStatus.NORMAL.value,TradeStatus.NORMAL.value])
        current_data = np.array([20, 40, 100, 120], dtype=np.float64)
        current_fx_data = np.array([2, 4], dtype=np.float64)  @ ccy_matrix
        ts = 1591325065
        order = np.array([100, 100, 100, 100], dtype=np.float64)
        price = np.array([20, 40, 100, 120], dtype=np.float64)
        id = 0

        #first order with delay
        order_manager.reset_trade_status(trade_status)
        feedback_id,order_feedback_msg = order_manager.cross_order(order,current_data,current_fx_data,id,ts,price,delay=np.array([1, 1, 0, 0], dtype=np.int64))
        assert (order_feedback_msg[OrderFeedback.transaction.value]== np.array([0, 0, 100, 100], dtype=np.float64)).all()
        assert (order_feedback_msg[OrderFeedback.current_data.value] == np.array([20, 40, 100, 120], dtype=np.float64)).all()
        assert (order_feedback_msg[OrderFeedback.commission_fee.value] == np.array([0, 0, 0, 0], dtype=np.float64)).all()
        assert (order_feedback_msg[OrderFeedback.current_fx_data.value] == np.array([2, 2, 4, 2], dtype=np.float64)).all()
        assert (order_feedback_msg[OrderFeedback.order_status.value] == np.array([OrderStatus.FILLED.value, OrderStatus.FILLED.value, OrderStatus.FILLED.value, OrderStatus.FILLED.value],dtype=np.float64)).all()
        assert (feedback_id == id  )
        assert (order_manager.pending_order == np.array(np.array([100,100,0,0]))).all()

        #handle pending order
        trade_status = np.array([TradeStatus.STOP.value,TradeStatus.STOP.value,TradeStatus.STOP.value,TradeStatus.STOP.value])
        order_manager.reset_trade_status(trade_status)
        feedback_id,order_feedback_msg = order_manager.finish_pending_order(current_data, current_fx_data, id, ts, price)
        assert (order_feedback_msg[OrderFeedback.transaction.value]== np.array([0, 0, 0, 0], dtype=np.float64)).all()
        assert (order_manager.pending_order == np.array(np.array([100,100,0,0]))).all()

        #second order with delay
        feedback_id,order_feedback_msg = order_manager.cross_order(order,current_data,current_fx_data,id,ts,price,delay=np.array([1, 1, 0, 0], dtype=np.int64))
        assert (order_feedback_msg[OrderFeedback.transaction.value]== np.array([0, 0, 100, 100], dtype=np.float64)).all()
        assert (order_manager.pending_order == np.array(np.array([200,200,0,0]))).all()

        #handle pending order tmr
        trade_status = np.array([TradeStatus.NORMAL.value,TradeStatus.NORMAL.value,TradeStatus.NORMAL.value,TradeStatus.NORMAL.value])
        order_manager.reset_trade_status(trade_status)
        feedback_id,order_feedback_msg = order_manager.finish_pending_order(current_data, current_fx_data, id, ts, price)
        assert (order_feedback_msg[OrderFeedback.transaction.value]== np.array([200, 200, 0, 0])).all()
        assert (order_manager.pending_order == np.array(np.array([0,0,0,0]))).all()

        

    

    