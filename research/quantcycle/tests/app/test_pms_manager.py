import os
import unittest

import numpy as np

from quantcycle.app.pms_manager.portfolio_manager import PorfolioManager
from quantcycle.utils.production_constant import (InstrumentType,
                                                  OrderFeedback, PmsStatus,
                                                  TradeStatus)


class TestPorfolioManager(unittest.TestCase):

    def test_profolio_manager_field_01(self):
        cash = 1000000
        ccy_matrix = np.array([[1, 1, 0, 1], [0, 0, 1, 0]], dtype=np.float64)
        instrument_type = np.array([InstrumentType.FX.value, InstrumentType.HK_STOCK.value,InstrumentType.CN_STOCK.value, InstrumentType.US_STOCK.value])
        portfolio_manager = PorfolioManager(cash, ccy_matrix, instrument_type)

        msg = np.zeros((5,4))
        msg[OrderFeedback.transaction.value] = np.array([100, 100, 100, 100], dtype=np.float64)
        msg[OrderFeedback.current_data.value] = np.array([20, 40, 100, 120], dtype=np.float64) 
        msg[OrderFeedback.commission_fee.value] = np.array([0, 0, 0, 0], dtype=np.float64)
        msg[OrderFeedback.current_fx_data.value] = np.array([2, 4], dtype=np.float64) @ ccy_matrix
        current_fx_data = msg[OrderFeedback.current_fx_data.value]

        portfolio_manager.receive_order_feedback(msg)

        assert (portfolio_manager.holding_value == (current_fx_data) * msg[OrderFeedback.current_data.value] * msg[OrderFeedback.transaction.value] * np.array([0, 1, 1, 1]) ).all()
        assert (portfolio_manager.pnl == np.array([0, 0, 0, 0]) ).all()
        assert (portfolio_manager.symbol_cash == -(current_fx_data) * msg[OrderFeedback.current_data.value] * msg[OrderFeedback.transaction.value] * np.array([0, 1, 1, 1]) ).all()
        assert (portfolio_manager.cash == cash - sum((current_fx_data) * msg[OrderFeedback.current_data.value] * msg[OrderFeedback.transaction.value] * np.array([0, 1, 1, 1])) )
        assert (portfolio_manager.pv == cash )
        assert (portfolio_manager.open_price == msg[OrderFeedback.current_data.value] ).all()

        

    def test_profolio_manager_field_02(self):
        cash = 1000000
        ccy_matrix = np.array([[1, 1, 0, 1], [0, 0, 1, 0]], dtype=np.float64)
        instrument_type = np.array([InstrumentType.FX.value, InstrumentType.HK_STOCK.value,InstrumentType.CN_STOCK.value, InstrumentType.US_STOCK.value])
        portfolio_manager = PorfolioManager(cash, ccy_matrix, instrument_type)

        msg = np.zeros((5,4))
        msg[OrderFeedback.transaction.value] = np.array([100, 100, 100, 100], dtype=np.float64)
        msg[OrderFeedback.current_data.value] = np.array([20, 40, 100, 120], dtype=np.float64) 
        msg[OrderFeedback.commission_fee.value] = np.array([0, 0, 0, 0], dtype=np.float64)
        msg[OrderFeedback.current_fx_data.value] = np.array([2, 4], dtype=np.float64) @ ccy_matrix
        current_fx_data = msg[OrderFeedback.current_fx_data.value]
        portfolio_manager.receive_order_feedback(msg)

        new_current_data = np.array([21, 41, 101, 121], dtype=np.float64)
        new_current_fx_data = np.array([3, 5], dtype=np.float64) @ ccy_matrix
        portfolio_manager.calculate_spot(new_current_data,new_current_fx_data)

        equity_pnl = ( (new_current_fx_data)* new_current_data - (current_fx_data) * msg[OrderFeedback.current_data.value] )* msg[OrderFeedback.transaction.value] * np.array([0, 1, 1, 1])
        non_equity_pnl = (new_current_fx_data)* (new_current_data - msg[OrderFeedback.current_data.value]) * msg[OrderFeedback.transaction.value] * np.array([1, 0, 0, 0])
        transaction_amount = (current_fx_data) * msg[OrderFeedback.current_data.value] * msg[OrderFeedback.transaction.value] * np.array([0, 1, 1, 1])
        open_price = np.array([21, 40*2/3, 100*4/5, 120*2/3], dtype=np.float64)

        assert (portfolio_manager.holding_value == (new_current_fx_data) * new_current_data * msg[OrderFeedback.transaction.value] * np.array([0, 1, 1, 1]) ).all()
        assert (portfolio_manager.pnl == equity_pnl + non_equity_pnl ).all()
        assert (portfolio_manager.cash == cash - sum(transaction_amount) + sum(non_equity_pnl) )
        assert (portfolio_manager.pv == cash + sum(equity_pnl + non_equity_pnl) ).all()
        assert np.allclose(portfolio_manager.open_price , open_price)

        

    def test_profolio_manager_field_03(self):
        cash = 1000000
        ccy_matrix = np.array([[1, 1, 0, 1], [0, 0, 1, 0]], dtype=np.float64)
        instrument_type = np.array([InstrumentType.FX.value, InstrumentType.HK_STOCK.value,InstrumentType.CN_STOCK.value, InstrumentType.US_STOCK.value])
        portfolio_manager = PorfolioManager(cash, ccy_matrix, instrument_type)

        msg = np.zeros((5,4))
        msg[OrderFeedback.transaction.value] = np.array([100, 100, 100, 100], dtype=np.float64)
        msg[OrderFeedback.current_data.value] = np.array([20, 40, 100, 120], dtype=np.float64) 
        msg[OrderFeedback.commission_fee.value] = msg[OrderFeedback.transaction.value] * np.array([0.01, 0.02, 0.03, 0.04], dtype=np.float64)
        msg[OrderFeedback.current_fx_data.value] = np.array([2, 4], dtype=np.float64) @ ccy_matrix
        current_fx_data = msg[OrderFeedback.current_fx_data.value]
        portfolio_manager.receive_order_feedback(msg)

        new_current_data = np.array([21, 41, 101, 121], dtype=np.float64)
        new_current_fx_data = np.array([3, 5], dtype=np.float64) @ ccy_matrix
        portfolio_manager.calculate_spot(new_current_data,new_current_fx_data)

        equity_pnl = ( (new_current_fx_data)* new_current_data - (current_fx_data) * msg[OrderFeedback.current_data.value] )* msg[OrderFeedback.transaction.value] * np.array([0, 1, 1, 1])
        non_equity_pnl = (new_current_fx_data)* (new_current_data - msg[OrderFeedback.current_data.value]) * msg[OrderFeedback.transaction.value] * np.array([1, 0, 0, 0])
        transaction_amount = (current_fx_data) * msg[OrderFeedback.current_data.value] * msg[OrderFeedback.transaction.value] * np.array([0, 1, 1, 1])
        open_price = np.array([21, 40*2/3, 100*4/5, 120*2/3], dtype=np.float64)

        assert (portfolio_manager.holding_value == (new_current_fx_data) * new_current_data * msg[OrderFeedback.transaction.value] * np.array([0, 1, 1, 1]) ).all()
        assert (portfolio_manager.pnl == equity_pnl + non_equity_pnl -msg[OrderFeedback.commission_fee.value]  ).all()
        assert (portfolio_manager.cash == cash - sum(transaction_amount) + sum(non_equity_pnl) -sum(msg[OrderFeedback.commission_fee.value]) )
        assert (portfolio_manager.pv == cash + sum(equity_pnl + non_equity_pnl)-sum(msg[OrderFeedback.commission_fee.value]) ).all()
        assert np.allclose(portfolio_manager.open_price , open_price)

        

    def test_profolio_manager_field_04(self):
        cash = 1000000
        ccy_matrix = np.array([[1, 1, 0, 1], [0, 0, 1, 0]], dtype=np.float64)
        instrument_type = np.array([InstrumentType.FX.value, InstrumentType.HK_STOCK.value,InstrumentType.CN_STOCK.value, InstrumentType.US_STOCK.value])
        portfolio_manager = PorfolioManager(cash, ccy_matrix, instrument_type)

        msg = np.zeros((5,4))
        msg[OrderFeedback.transaction.value] = np.array([100, 100, 100, 100], dtype=np.float64)
        msg[OrderFeedback.current_data.value] = np.array([20, 40, 100, 120], dtype=np.float64) 
        msg[OrderFeedback.commission_fee.value] = np.array([0, 0, 0, 0], dtype=np.float64)
        msg[OrderFeedback.current_fx_data.value] = np.array([2, 4], dtype=np.float64) @ ccy_matrix
        current_fx_data = msg[OrderFeedback.current_fx_data.value]

        portfolio_manager.receive_order_feedback(msg)
        current_rate_data = np.array([0.02, 0.02, 0.02, 0.02], dtype=np.float64)
        portfolio_manager.calculate_rate(current_rate_data, msg[OrderFeedback.current_data.value], current_fx_data)
        pnl_fx_type =  (current_fx_data)* (instrument_type==InstrumentType.FX.value) * msg[OrderFeedback.current_data.value] * msg[OrderFeedback.transaction.value] * current_rate_data
        pnl_non_fx_type =  (current_fx_data)* (instrument_type!=InstrumentType.FX.value) * msg[OrderFeedback.transaction.value] * current_rate_data
        pnl = pnl_fx_type + pnl_non_fx_type

        transaction_amount = (current_fx_data) * msg[OrderFeedback.current_data.value] * msg[OrderFeedback.transaction.value] * np.array([0, 1, 1, 1])

        assert (portfolio_manager.holding_value == transaction_amount ).all()
        assert (portfolio_manager.pnl == pnl  ).all()
        assert (portfolio_manager.cash == cash - sum(transaction_amount) + sum(pnl) )
        assert (portfolio_manager.pv == cash + sum(pnl) ).all()
        assert np.allclose(portfolio_manager.open_price , msg[OrderFeedback.current_data.value])

        

    def test_profolio_manager_field_05(self):
        cash = 1000000
        ccy_matrix = np.array([[1, 1, 0, 1], [0, 0, 1, 0]], dtype=np.float64)
        instrument_type = np.array([InstrumentType.FX.value, InstrumentType.HK_STOCK.value,InstrumentType.CN_STOCK.value, InstrumentType.US_STOCK.value])
        portfolio_manager = PorfolioManager(cash, ccy_matrix, instrument_type)

        msg = np.zeros((5,4))
        msg[OrderFeedback.transaction.value] = np.array([100, 100, 100, 100], dtype=np.float64)
        msg[OrderFeedback.current_data.value] = np.array([20, 40, 100, 120], dtype=np.float64) 
        msg[OrderFeedback.commission_fee.value] = msg[OrderFeedback.transaction.value]*0.01
        msg[OrderFeedback.current_fx_data.value] = np.array([2, 4], dtype=np.float64) @ ccy_matrix
        current_fx_data = msg[OrderFeedback.current_fx_data.value]
        portfolio_manager.receive_order_feedback(msg)

        pnl = np.array([0, 0, 0, 0], dtype=np.float64)
        transaction_amount = (current_fx_data) * msg[OrderFeedback.current_data.value] * msg[OrderFeedback.transaction.value] * np.array([0, 1, 1, 1])

        new_msg = np.zeros((5,4))
        new_msg[OrderFeedback.transaction.value] = np.array([200, 200, 200, 200], dtype=np.float64)
        new_msg[OrderFeedback.current_data.value] = np.array([21, 41, 101, 121], dtype=np.float64) 
        new_current_fx_data = np.array([3, 5], dtype=np.float64)  @ ccy_matrix
        new_msg[OrderFeedback.commission_fee.value] = new_msg[OrderFeedback.transaction.value]*0.02
        new_msg[OrderFeedback.current_fx_data.value] = new_current_fx_data
        portfolio_manager.receive_order_feedback(new_msg)

        new_equity_pnl = ( (new_current_fx_data)* new_msg[OrderFeedback.current_data.value] - (current_fx_data) * msg[OrderFeedback.current_data.value] )* msg[OrderFeedback.transaction.value] * np.array([0, 1, 1, 1])
        new_non_equity_pnl = (new_current_fx_data) * (new_msg[OrderFeedback.current_data.value] - msg[OrderFeedback.current_data.value])* msg[OrderFeedback.transaction.value] * np.array([1, 0, 0, 0])
        new_transaction_amount = (new_current_fx_data) * new_msg[OrderFeedback.current_data.value] * new_msg[OrderFeedback.transaction.value] * np.array([0, 1, 1, 1])
        open_price = (1/new_current_fx_data) *  (transaction_amount + new_transaction_amount)/(msg[OrderFeedback.transaction.value]+new_msg[OrderFeedback.transaction.value]) \
                     + new_msg[OrderFeedback.current_data.value] * np.array([1, 0, 0, 0])

        assert (portfolio_manager.holding_value == transaction_amount + new_transaction_amount + new_equity_pnl ).all()
        assert (portfolio_manager.pnl == new_equity_pnl + new_non_equity_pnl- msg[OrderFeedback.commission_fee.value] - new_msg[OrderFeedback.commission_fee.value]  ).all()
        assert (portfolio_manager.cash == cash - sum(transaction_amount+new_transaction_amount) + sum(new_non_equity_pnl) - sum(msg[OrderFeedback.commission_fee.value] + new_msg[OrderFeedback.commission_fee.value]))
        assert (portfolio_manager.pv == cash + sum(new_equity_pnl+new_non_equity_pnl) - sum(msg[OrderFeedback.commission_fee.value] + new_msg[OrderFeedback.commission_fee.value])).all()
        assert np.allclose(portfolio_manager.open_price , open_price)

        


    def test_profolio_manager_field_06(self):
        cash = 1000000
        ccy_matrix = np.array([[1, 1, 0, 1], [0, 0, 1, 0]], dtype=np.float64)
        instrument_type = np.array([InstrumentType.FX.value, InstrumentType.HK_STOCK.value,InstrumentType.CN_STOCK.value, InstrumentType.US_STOCK.value])
        portfolio_manager = PorfolioManager(cash, ccy_matrix, instrument_type)

        msg = np.zeros((5,4))
        msg[OrderFeedback.transaction.value] = np.array([100, 100, 100, 100], dtype=np.float64)
        msg[OrderFeedback.current_data.value] = np.array([20, 40, 100, 120], dtype=np.float64) 
        current_fx_data_1 = np.array([2, 4], dtype=np.float64)  @ ccy_matrix
        msg[OrderFeedback.commission_fee.value] = msg[OrderFeedback.transaction.value]*0.01
        msg[OrderFeedback.current_fx_data.value] = current_fx_data_1
        portfolio_manager.receive_order_feedback(msg)


        pnl = np.array([0, 0, 0, 0], dtype=np.float64)

        new_msg = np.zeros((5,4))
        new_msg[OrderFeedback.transaction.value] = np.array([200, 200, 200, 200], dtype=np.float64)
        new_msg[OrderFeedback.current_data.value] = np.array([21, 41, 101, 121], dtype=np.float64) 
        new_current_fx_data_1 = np.array([3, 5], dtype=np.float64)  @ ccy_matrix
        new_msg[OrderFeedback.commission_fee.value] = new_msg[OrderFeedback.transaction.value]*0.02
        new_msg[OrderFeedback.current_fx_data.value] = new_current_fx_data_1
        portfolio_manager.receive_order_feedback(new_msg)

        current_data = np.array([22, 42, 102, 122], dtype=np.float64)
        current_fx_data = np.array([4, 6], dtype=np.float64) @ ccy_matrix
        portfolio_manager.calculate_spot(current_data,current_fx_data)


        equity_holding_value = (current_fx_data ) * current_data * (new_msg[OrderFeedback.transaction.value] + msg[OrderFeedback.transaction.value]) * np.array([0, 1, 1, 1])
        non_equity_pnl_1 = (new_current_fx_data_1) * ( (new_msg[OrderFeedback.current_data.value] - msg[OrderFeedback.current_data.value]) * msg[OrderFeedback.transaction.value] ) * np.array([1, 0, 0, 0])
        non_equity_pnl_2 = (current_fx_data ) * ( (current_data - new_msg[OrderFeedback.current_data.value]) * (new_msg[OrderFeedback.transaction.value] + msg[OrderFeedback.transaction.value] )) * np.array([1, 0, 0, 0])
        non_equity_pnl = non_equity_pnl_1 + non_equity_pnl_2
        transaction_amount = (current_fx_data_1 ) * msg[OrderFeedback.current_data.value]* msg[OrderFeedback.transaction.value] * np.array([0, 1, 1, 1])
        new_transaction_amount = (new_current_fx_data_1 ) * new_msg[OrderFeedback.current_data.value] * new_msg[OrderFeedback.transaction.value] * np.array([0, 1, 1, 1])
        open_price = (1/current_fx_data ) *  (transaction_amount + new_transaction_amount)/(msg[OrderFeedback.transaction.value]+new_msg[OrderFeedback.transaction.value]) \
                     + current_data * np.array([1, 0, 0, 0])

        assert (portfolio_manager.holding_value == equity_holding_value ).all()
        assert (portfolio_manager.pnl == equity_holding_value + non_equity_pnl - transaction_amount - new_transaction_amount - new_msg[OrderFeedback.commission_fee.value] - msg[OrderFeedback.commission_fee.value]  ).all()
        assert (portfolio_manager.cash == cash - sum(transaction_amount+new_transaction_amount) + sum(non_equity_pnl) - sum(msg[OrderFeedback.commission_fee.value] + new_msg[OrderFeedback.commission_fee.value]))
        assert (portfolio_manager.pv == portfolio_manager.cash + sum(portfolio_manager.holding_value) )
        assert np.allclose(portfolio_manager.open_price , open_price)

        


    def test_profolio_manager_field_07(self):
        cash = 1000000
        ccy_matrix = np.array([[1, 1, 0, 1], [0, 0, 1, 0]], dtype=np.float64)
        instrument_type = np.array([InstrumentType.FX.value, InstrumentType.HK_STOCK.value,InstrumentType.CN_STOCK.value, InstrumentType.US_STOCK.value])
        portfolio_manager = PorfolioManager(cash, ccy_matrix, instrument_type)

        msg = np.zeros((5,4))
        msg[OrderFeedback.transaction.value] = np.array([200, 200, 200, 200], dtype=np.float64)
        msg[OrderFeedback.current_data.value] = np.array([20, 40, 100, 120], dtype=np.float64) 
        current_fx_data = np.array([2, 4], dtype=np.float64) @ ccy_matrix
        msg[OrderFeedback.current_fx_data.value] = current_fx_data
        msg[OrderFeedback.commission_fee.value] = msg[OrderFeedback.transaction.value]*0.01
        portfolio_manager.receive_order_feedback(msg)

        new_msg = np.zeros((5,4))
        new_msg[OrderFeedback.transaction.value] = np.array([-100, -100, -100, -100], dtype=np.float64)
        new_msg[OrderFeedback.current_data.value] = np.array([21, 41, 101, 121], dtype=np.float64) 
        new_current_fx_data = np.array([3, 5], dtype=np.float64) @ ccy_matrix
        new_msg[OrderFeedback.current_fx_data.value] = new_current_fx_data
        new_msg[OrderFeedback.commission_fee.value] = new_msg[OrderFeedback.transaction.value]*0.02
        portfolio_manager.receive_order_feedback(new_msg)

        equity_holding_value = (new_current_fx_data ) * new_msg[OrderFeedback.current_data.value] * (new_msg[OrderFeedback.transaction.value] + msg[OrderFeedback.transaction.value]) * np.array([0, 1, 1, 1])
        non_equity_pnl = (new_current_fx_data ) * ( (new_msg[OrderFeedback.current_data.value] - msg[OrderFeedback.current_data.value]) * msg[OrderFeedback.transaction.value] ) * np.array([1, 0, 0, 0])
        transaction_amount = (current_fx_data ) * msg[OrderFeedback.current_data.value]* msg[OrderFeedback.transaction.value] * np.array([0, 1, 1, 1])
        new_transaction_amount = (new_current_fx_data ) * new_msg[OrderFeedback.current_data.value] * new_msg[OrderFeedback.transaction.value] * np.array([0, 1, 1, 1])
        open_price = (1/new_current_fx_data ) *  (transaction_amount + new_transaction_amount)/(msg[OrderFeedback.transaction.value]+new_msg[OrderFeedback.transaction.value]) \
                     + new_msg[OrderFeedback.current_data.value] * np.array([1, 0, 0, 0])

        assert (portfolio_manager.holding_value == equity_holding_value ).all()
        assert (portfolio_manager.pnl == equity_holding_value + non_equity_pnl - transaction_amount - new_transaction_amount - new_msg[OrderFeedback.commission_fee.value] - msg[OrderFeedback.commission_fee.value]  ).all()
        assert (portfolio_manager.cash == cash - sum(transaction_amount+new_transaction_amount) + sum(non_equity_pnl) - sum(msg[OrderFeedback.commission_fee.value] + new_msg[OrderFeedback.commission_fee.value]))
        assert (portfolio_manager.pv == portfolio_manager.cash + sum(portfolio_manager.holding_value) )
        assert np.allclose(portfolio_manager.open_price , open_price)
        
        

    def test_profolio_manager_field_08(self):
        cash = 1000000
        ccy_matrix = np.array([[1, 1, 0, 1], [0, 0, 1, 0]], dtype=np.float64)
        instrument_type = np.array([InstrumentType.FX.value, InstrumentType.HK_STOCK.value,InstrumentType.CN_STOCK.value, InstrumentType.US_STOCK.value])
        portfolio_manager = PorfolioManager(cash, ccy_matrix, instrument_type)

        order = np.array([100, 100, 100, 100], dtype=np.float64)
        revised_order , order_status = portfolio_manager.check_order(order)
        status = np.array([PmsStatus.LEGAL.value, PmsStatus.LEGAL.value, PmsStatus.LEGAL.value, PmsStatus.LEGAL.value], dtype=np.float64)
        assert (order == revised_order ).all()
        assert (order_status == status ).all()
        
        

    def test_profolio_manager_field_09(self):
        cash = 1000000
        ccy_matrix = np.array([[1, 1, 0, 1], [0, 0, 1, 0]], dtype=np.float64)
        instrument_type = np.array([InstrumentType.FX.value, InstrumentType.HK_STOCK.value,InstrumentType.CN_STOCK.value, InstrumentType.US_STOCK.value])
        portfolio_manager = PorfolioManager(cash, ccy_matrix, instrument_type)

        order = np.array([-100, -100, -100, -100], dtype=np.float64)
        revised_order , order_status = portfolio_manager.check_order(order)
        target_revised_order = np.array([-100,0, 0, -100], dtype=np.float64)
        status = np.array([PmsStatus.LEGAL.value, PmsStatus.ILLEGAL.value, PmsStatus.ILLEGAL.value, PmsStatus.LEGAL.value], dtype=np.float64)
        assert (target_revised_order == revised_order ).all()
        assert (order_status == status ).all()
        
        

    def test_profolio_manager_field_10(self):
        cash = 100
        ccy_matrix = np.array([[1, 1, 0, 1], [0, 0, 1, 0]], dtype=np.float64)
        instrument_type = np.array([InstrumentType.FX.value, InstrumentType.HK_STOCK.value,InstrumentType.CN_STOCK.value, InstrumentType.US_STOCK.value])
        portfolio_manager = PorfolioManager(cash, ccy_matrix, instrument_type)

        current_data = np.array([21, 41, 101, 121], dtype=np.float64)
        current_fx_data = np.array([3, 5], dtype=np.float64) @ ccy_matrix
        portfolio_manager.calculate_spot(current_data,current_fx_data)

        order = np.array([100, 100, 100, 100], dtype=np.float64)
        revised_order , order_status = portfolio_manager.check_order(order)
        target_revised_order = np.array([0,0, 0, 0], dtype=np.float64)
        status = np.array([PmsStatus.ILLEGAL.value, PmsStatus.ILLEGAL.value, PmsStatus.ILLEGAL.value, PmsStatus.ILLEGAL.value], dtype=np.float64)
        assert (target_revised_order == revised_order ).all()
        assert (order_status == status ).all()
        
        

    def test_profolio_manager_field_11(self):
        cash = 100
        ccy_matrix = np.array([[1, 1, 0, 1], [0, 0, 1, 0]], dtype=np.float64)
        instrument_type = np.array([InstrumentType.FX.value, InstrumentType.HK_STOCK.value,InstrumentType.CN_STOCK.value, InstrumentType.US_STOCK.value])
        portfolio_manager = PorfolioManager(cash, ccy_matrix, instrument_type)

        current_data = np.array([21, 41, 101, 121], dtype=np.float64)
        current_fx_data = np.array([3, 5], dtype=np.float64) @ ccy_matrix
        portfolio_manager.calculate_spot(current_data,current_fx_data)
        
        order = np.array([100, 0, 0, 0], dtype=np.float64)
        revised_order , order_status = portfolio_manager.check_order(order)
        target_revised_order = np.array([100,0, 0, 0], dtype=np.float64)
        status = np.array([PmsStatus.LEGAL.value, PmsStatus.LEGAL.value, PmsStatus.LEGAL.value, PmsStatus.LEGAL.value], dtype=np.float64)
        assert (target_revised_order == revised_order ).all()
        assert (order_status == status ).all()
        
        

    def test_profolio_manager_field_12(self):
        cash = 1000000
        ccy_matrix = np.array([[1, 1, 0, 1], [0, 0, 1, 0]], dtype=np.float64)
        instrument_type = np.array([InstrumentType.FX.value, InstrumentType.HK_STOCK.value,InstrumentType.CN_STOCK.value, InstrumentType.US_STOCK.value])
        portfolio_manager = PorfolioManager(cash, ccy_matrix, instrument_type)

        msg = np.zeros((5,4))
        msg[OrderFeedback.transaction.value] = np.array([100, 100, 100, 100], dtype=np.float64)
        msg[OrderFeedback.current_data.value] = np.array([20, 40, 100, 120], dtype=np.float64) 
        current_fx_data_1 = np.array([2, 4], dtype=np.float64) @ ccy_matrix
        msg[OrderFeedback.current_fx_data.value] = current_fx_data_1
        msg[OrderFeedback.commission_fee.value] = np.array([0, 0, 0, 0], dtype=np.float64)
        portfolio_manager.receive_order_feedback(msg)

        current_data = np.array([21, 41, 101, 121], dtype=np.float64)
        current_fx_data = np.array([3, 5], dtype=np.float64) @ ccy_matrix
        portfolio_manager.calculate_spot(current_data,current_fx_data)
        

        order = np.array([100, 100, 100, 100], dtype=np.float64)
        revised_order , order_status = portfolio_manager.check_order(order)
        target_revised_order = np.array([100,100, 100, 100], dtype=np.float64)
        status = np.array([PmsStatus.LEGAL.value, PmsStatus.LEGAL.value, PmsStatus.LEGAL.value, PmsStatus.LEGAL.value], dtype=np.float64)
        assert (target_revised_order == revised_order ).all()
        assert (order_status == status ).all()
        
        

    def test_profolio_manager_field_13(self):
        cash = 1000000
        ccy_matrix = np.array([[1, 1, 0, 1], [0, 0, 1, 0]], dtype=np.float64)
        instrument_type = np.array([InstrumentType.FX.value, InstrumentType.HK_STOCK.value,InstrumentType.CN_STOCK.value, InstrumentType.US_STOCK.value])
        portfolio_manager = PorfolioManager(cash, ccy_matrix, instrument_type)

        msg = np.zeros((5,4))
        msg[OrderFeedback.transaction.value] = np.array([100, 100, 100, 100], dtype=np.float64)
        msg[OrderFeedback.current_data.value] = np.array([20, 40, 100, 120], dtype=np.float64) 
        current_fx_data_1 = np.array([2, 4], dtype=np.float64) @ ccy_matrix
        msg[OrderFeedback.current_fx_data.value] = current_fx_data_1
        msg[OrderFeedback.commission_fee.value] = np.array([0, 0, 0, 0], dtype=np.float64)
        portfolio_manager.receive_order_feedback(msg)

        current_data = np.array([21, 41, 101, 121], dtype=np.float64)
        current_fx_data = np.array([3, 5], dtype=np.float64) @ ccy_matrix
        portfolio_manager.calculate_spot(current_data,current_fx_data)
        

        order = np.array([-100, -100, -100, -100], dtype=np.float64)
        revised_order , order_status = portfolio_manager.check_order(order)
        target_revised_order = np.array([-100,-100, 0, -100], dtype=np.float64)
        status = np.array([PmsStatus.LEGAL.value, PmsStatus.LEGAL.value, PmsStatus.ILLEGAL.value, PmsStatus.LEGAL.value], dtype=np.float64)
        assert (target_revised_order == revised_order ).all()
        assert (order_status == status ).all()
        
        

    def test_profolio_manager_field_14(self):
        cash = 1000000
        ccy_matrix = np.array([[1, 1, 0, 1], [0, 0, 1, 0]], dtype=np.float64)
        instrument_type = np.array([InstrumentType.FX.value, InstrumentType.HK_STOCK.value,InstrumentType.CN_STOCK.value, InstrumentType.US_STOCK.value])
        portfolio_manager = PorfolioManager(cash, ccy_matrix, instrument_type)

        msg = np.zeros((5,4))
        msg[OrderFeedback.transaction.value] = np.array([100, 100, 100, 100], dtype=np.float64)
        msg[OrderFeedback.current_data.value] = np.array([20, 40, 100, 120], dtype=np.float64) 
        current_fx_data_1 = np.array([2, 4], dtype=np.float64) @ ccy_matrix
        msg[OrderFeedback.current_fx_data.value] = current_fx_data_1
        msg[OrderFeedback.commission_fee.value] = np.array([0, 0, 0, 0], dtype=np.float64)
        portfolio_manager.receive_order_feedback(msg)

        portfolio_manager.reset_field_rollover_day()

        current_data = np.array([21, 41, 101, 121], dtype=np.float64)
        current_fx_data = np.array([3, 5], dtype=np.float64) @ ccy_matrix
        portfolio_manager.calculate_spot(current_data,current_fx_data)
        

        order = np.array([-100, -100, -100, -100], dtype=np.float64)
        revised_order , order_status = portfolio_manager.check_order(order)
        target_revised_order = np.array([-100,-100, -100, -100], dtype=np.float64)
        status = np.array([PmsStatus.LEGAL.value, PmsStatus.LEGAL.value, PmsStatus.LEGAL.value, PmsStatus.LEGAL.value], dtype=np.float64)
        assert (target_revised_order == revised_order ).all()
        assert (order_status == status ).all()
        
        

    def test_profolio_manager_field_15(self):
        cash = 1000000
        ccy_matrix = np.array([[1, 1, 0, 1], [0, 0, 1, 0]], dtype=np.float64)
        instrument_type = np.array([InstrumentType.FX.value, InstrumentType.HK_STOCK.value,InstrumentType.CN_STOCK.value, InstrumentType.US_STOCK.value])
        portfolio_manager = PorfolioManager(cash, ccy_matrix, instrument_type)

        msg = np.zeros((5,4))
        msg[OrderFeedback.transaction.value] = np.array([100, 100, 100, 100], dtype=np.float64)
        msg[OrderFeedback.current_data.value] = np.array([20, 40, 100, 120], dtype=np.float64) 
        current_fx_data_1 = np.array([2, 4], dtype=np.float64) @ ccy_matrix
        msg[OrderFeedback.current_fx_data.value] = current_fx_data_1
        msg[OrderFeedback.commission_fee.value] = np.array([0, 0, 0, 0], dtype=np.float64)
        portfolio_manager.receive_order_feedback(msg)

        portfolio_manager.reset_field_rollover_day()

        current_data = np.array([21, 41, 101, 121], dtype=np.float64)
        current_fx_data = np.array([3, 5], dtype=np.float64)  @ ccy_matrix
        portfolio_manager.calculate_spot(current_data,current_fx_data)
        

        order = np.array([-200, -200, -200, -200], dtype=np.float64)
        revised_order , order_status = portfolio_manager.check_order(order)
        target_revised_order = np.array([-200,0, 0, -200], dtype=np.float64)
        status = np.array([PmsStatus.LEGAL.value, PmsStatus.ILLEGAL.value, PmsStatus.ILLEGAL.value, PmsStatus.LEGAL.value], dtype=np.float64)
        assert (target_revised_order == revised_order ).all()
        assert (order_status == status ).all()
        
        


    def test_profolio_manager_field_16(self):
        cash = 1000000
        ccy_matrix = np.array([[1, 1, 0, 1], [0, 0, 1, 0]], dtype=np.float64)
        instrument_type = np.array([InstrumentType.FX.value, InstrumentType.HK_STOCK.value,InstrumentType.CN_STOCK.value, InstrumentType.US_STOCK.value])
        portfolio_manager = PorfolioManager(cash, ccy_matrix, instrument_type)

        trade_status = [TradeStatus.NORMAL.value,TradeStatus.NORMAL.value,TradeStatus.NORMAL.value,TradeStatus.NORMAL.value]
        portfolio_manager.reset_trade_status(trade_status)

        order = np.array([np.nan, 100, 100, 100], dtype=np.float64)
        target_revised_order = np.array([0, 100, 100, 100], dtype=np.float64)

        revised_order , order_status = portfolio_manager.check_order(order)
        status = np.array([PmsStatus.ILLEGAL.value, PmsStatus.LEGAL.value, PmsStatus.LEGAL.value, PmsStatus.LEGAL.value], dtype=np.float64)
        assert (target_revised_order == revised_order ).all()
        assert (order_status == status ).all()
        
        

    def test_profolio_manager_field_17(self):
        cash = 1000000
        ccy_matrix = np.array([[1, 1, 0, 1], [0, 0, 1, 0]], dtype=np.float64)
        instrument_type = np.array([InstrumentType.FX.value, InstrumentType.HK_STOCK.value,InstrumentType.CN_STOCK.value, InstrumentType.US_STOCK.value])
        portfolio_manager = PorfolioManager(cash, ccy_matrix, instrument_type)

        trade_status = [TradeStatus.STOP.value,TradeStatus.NORMAL.value,TradeStatus.NORMAL.value,TradeStatus.NORMAL.value]
        portfolio_manager.reset_trade_status(trade_status)

        order = np.array([100, 100, 100, 100], dtype=np.float64)
        target_revised_order = np.array([0, 100, 100, 100], dtype=np.float64)

        revised_order , order_status = portfolio_manager.check_order(order)
        status = np.array([PmsStatus.ILLEGAL.value, PmsStatus.LEGAL.value, PmsStatus.LEGAL.value, PmsStatus.LEGAL.value], dtype=np.float64)
        assert (target_revised_order == revised_order ).all()
        assert (order_status == status ).all()
        
        

    def test_profolio_manager_field_18(self):
        cash = 1000000
        ccy_matrix = np.array([[1, 1, 0, 1], [0, 0, 1, 0]], dtype=np.float64)
        instrument_type = np.array([InstrumentType.FX.value, InstrumentType.HK_STOCK.value,InstrumentType.CN_STOCK.value, InstrumentType.US_STOCK.value])
        portfolio_manager = PorfolioManager(cash, ccy_matrix, instrument_type)

        trade_status = np.array([TradeStatus.STOPBUY.value,TradeStatus.NORMAL.value,TradeStatus.NORMAL.value,TradeStatus.NORMAL.value])
        portfolio_manager.reset_trade_status(trade_status)

        order = np.array([100, 100, 100, 100], dtype=np.float64)
        target_revised_order = np.array([0, 100, 100, 100], dtype=np.float64)

        revised_order , order_status = portfolio_manager.check_order(order)
        status = np.array([PmsStatus.ILLEGAL.value, PmsStatus.LEGAL.value, PmsStatus.LEGAL.value, PmsStatus.LEGAL.value], dtype=np.float64)
        assert (target_revised_order == revised_order ).all()
        assert (order_status == status ).all()
        
        

    def test_profolio_manager_field_19(self):
        cash = 1000000
        ccy_matrix = np.array([[1, 1, 0, 1], [0, 0, 1, 0]], dtype=np.float64)
        instrument_type = np.array([InstrumentType.FX.value, InstrumentType.HK_STOCK.value,InstrumentType.CN_STOCK.value, InstrumentType.US_STOCK.value])
        portfolio_manager = PorfolioManager(cash, ccy_matrix, instrument_type)

        trade_status = np.array([TradeStatus.STOPSELL.value,TradeStatus.NORMAL.value,TradeStatus.NORMAL.value,TradeStatus.NORMAL.value])
        portfolio_manager.reset_trade_status(trade_status)

        order = np.array([100, 100, 100, 100], dtype=np.float64)
        target_revised_order = np.array([100, 100, 100, 100], dtype=np.float64)

        revised_order , order_status = portfolio_manager.check_order(order)
        status = np.array([PmsStatus.LEGAL.value, PmsStatus.LEGAL.value, PmsStatus.LEGAL.value, PmsStatus.LEGAL.value], dtype=np.float64)
        assert (target_revised_order == revised_order ).all()
        assert (order_status == status ).all()
        
        

    def test_profolio_manager_field_20(self):
        cash = 1000000
        ccy_matrix = np.array([[1, 1, 0, 1], [0, 0, 1, 0]], dtype=np.float64)
        instrument_type = np.array([InstrumentType.FX.value, InstrumentType.HK_STOCK.value,InstrumentType.CN_STOCK.value, InstrumentType.US_STOCK.value])
        portfolio_manager = PorfolioManager(cash, ccy_matrix, instrument_type)

        trade_status = np.array([TradeStatus.STOPSELL.value,TradeStatus.NORMAL.value,TradeStatus.NORMAL.value,TradeStatus.NORMAL.value])
        portfolio_manager.reset_trade_status(trade_status)

        order = np.array([100, 100, 100, 100], dtype=np.float64)
        target_revised_order = np.array([100, 100, 100, 100], dtype=np.float64)

        revised_order , order_status = portfolio_manager.check_order(order)
        status = np.array([PmsStatus.LEGAL.value, PmsStatus.LEGAL.value, PmsStatus.LEGAL.value, PmsStatus.LEGAL.value], dtype=np.float64)
        assert (target_revised_order == revised_order ).all()
        assert (order_status == status ).all()
        
        

    def test_profolio_manager_field_21(self):
        cash = 1000000
        ccy_matrix = np.array([[1, 1, 0, 1], [0, 0, 1, 0]], dtype=np.float64)
        instrument_type = np.array([InstrumentType.FX.value, InstrumentType.HK_STOCK.value,InstrumentType.CN_STOCK.value, InstrumentType.US_STOCK.value])
        portfolio_manager = PorfolioManager(cash, ccy_matrix, instrument_type)

        trade_status = np.array([TradeStatus.STOPSELL.value,TradeStatus.NORMAL.value,TradeStatus.NORMAL.value,TradeStatus.NORMAL.value])
        portfolio_manager.reset_trade_status(trade_status)

        order = np.array([-100, 100, 100, 100], dtype=np.float64)
        target_revised_order = np.array([0, 100, 100, 100], dtype=np.float64)

        revised_order , order_status = portfolio_manager.check_order(order)
        status = np.array([PmsStatus.ILLEGAL.value, PmsStatus.LEGAL.value, PmsStatus.LEGAL.value, PmsStatus.LEGAL.value], dtype=np.float64)
        assert (target_revised_order == revised_order ).all()
        assert (order_status == status ).all()
        
        

    def test_profolio_manager_field_22(self):
        cash = 1000000
        ccy_matrix = np.array([[1, 1, 0, 1], [0, 0, 1, 0]], dtype=np.float64)
        instrument_type = np.array([InstrumentType.FX.value, InstrumentType.HK_STOCK.value,InstrumentType.CN_STOCK.value, InstrumentType.US_STOCK.value])
        portfolio_manager = PorfolioManager(cash, ccy_matrix, instrument_type)

        portfolio_manager.sync_cash(120000)
        target_cash = 120000
        target_cash_residual = target_cash - cash

        assert (portfolio_manager.cash == target_cash )
        assert (portfolio_manager.cash_residual == target_cash_residual )

    def test_profolio_manager_field_23(self):
        cash = 1000000
        ccy_matrix = np.array([[1, 1, 0, 1], [0, 0, 1, 0]], dtype=np.float64)
        instrument_type = np.array([InstrumentType.FX.value, InstrumentType.HK_STOCK.value,InstrumentType.CN_STOCK.value, InstrumentType.US_STOCK.value])
        portfolio_manager = PorfolioManager(cash, ccy_matrix, instrument_type)

        msg = np.zeros((5,4))
        msg[OrderFeedback.transaction.value] = np.array([100, 100, 100, 100], dtype=np.float64)
        msg[OrderFeedback.current_data.value] = np.array([20, 40, 100, 120], dtype=np.float64) 
        msg[OrderFeedback.commission_fee.value] = np.array([0, 0, 0, 0], dtype=np.float64)
        msg[OrderFeedback.current_fx_data.value] = np.array([2, 4], dtype=np.float64) @ ccy_matrix
        current_fx_data = msg[OrderFeedback.current_fx_data.value]
        portfolio_manager.receive_order_feedback(msg)

        new_current_data = np.array([21, 41, 101, 121], dtype=np.float64)
        new_current_fx_data = np.array([3, 5], dtype=np.float64) @ ccy_matrix
        portfolio_manager.calculate_spot(new_current_data,new_current_fx_data)

        equity_pnl = ( (new_current_fx_data)* new_current_data - (current_fx_data) * msg[OrderFeedback.current_data.value] )* msg[OrderFeedback.transaction.value] * np.array([0, 1, 1, 1])
        non_equity_pnl = (new_current_fx_data)* (new_current_data - msg[OrderFeedback.current_data.value]) * msg[OrderFeedback.transaction.value] * np.array([1, 0, 0, 0])
        transaction_amount = (current_fx_data) * msg[OrderFeedback.current_data.value] * msg[OrderFeedback.transaction.value] * np.array([0, 1, 1, 1])
        open_price = np.array([21, 40*2/3, 100*4/5, 120*2/3], dtype=np.float64)

        assert (portfolio_manager.holding_value == (new_current_fx_data) * new_current_data * msg[OrderFeedback.transaction.value] * np.array([0, 1, 1, 1]) ).all()
        assert (portfolio_manager.pnl == equity_pnl + non_equity_pnl ).all()
        assert (portfolio_manager.cash == cash - sum(transaction_amount) + sum(non_equity_pnl) )
        assert (portfolio_manager.pv == cash + sum(equity_pnl + non_equity_pnl) ).all()
        assert np.allclose(portfolio_manager.open_price , open_price)

        new_current_data_1 = np.array([np.nan, np.nan, 101, 121], dtype=np.float64)
        new_current_fx_data_1 = np.array([3, 5], dtype=np.float64) @ ccy_matrix
        portfolio_manager.calculate_spot(new_current_data_1,new_current_fx_data_1)

        assert (portfolio_manager.holding_value == (new_current_fx_data) * new_current_data * msg[OrderFeedback.transaction.value] * np.array([0, 1, 1, 1]) ).all()
        assert (portfolio_manager.pnl == equity_pnl + non_equity_pnl ).all()
        assert (portfolio_manager.cash == cash - sum(transaction_amount) + sum(non_equity_pnl) )
        assert (portfolio_manager.pv == cash + sum(equity_pnl + non_equity_pnl) ).all()
        assert np.allclose(portfolio_manager.open_price , open_price)
        assert np.allclose(portfolio_manager.current_data , new_current_data)


    def test_profolio_manager_field_24(self):
        cash = 1000000
        ccy_matrix = np.array([[1, 1, 0, 1], [0, 0, 1, 0]], dtype=np.float64)
        instrument_type = np.array([InstrumentType.FX.value, InstrumentType.HK_STOCK.value,InstrumentType.CN_STOCK.value, InstrumentType.US_STOCK.value])
        portfolio_manager = PorfolioManager(cash, ccy_matrix, instrument_type)

        msg = np.zeros((5,4))
        msg[OrderFeedback.transaction.value] = np.array([100, 100, 100, 100], dtype=np.float64)
        msg[OrderFeedback.current_data.value] = np.array([20, 40, 100, 120], dtype=np.float64) 
        msg[OrderFeedback.commission_fee.value] = np.array([0, 0, 0, 0], dtype=np.float64)
        msg[OrderFeedback.current_fx_data.value] = np.array([2, 4], dtype=np.float64) @ ccy_matrix
        current_fx_data = msg[OrderFeedback.current_fx_data.value]
        portfolio_manager.receive_order_feedback(msg)

        new_current_data = np.array([21, 41, 101, 121], dtype=np.float64)
        new_current_fx_data = np.array([3, 5], dtype=np.float64) @ ccy_matrix
        portfolio_manager.calculate_spot(new_current_data,new_current_fx_data)

        equity_pnl = ( (new_current_fx_data)* new_current_data - (current_fx_data) * msg[OrderFeedback.current_data.value] )* msg[OrderFeedback.transaction.value] * np.array([0, 1, 1, 1])
        non_equity_pnl = (new_current_fx_data)* (new_current_data - msg[OrderFeedback.current_data.value]) * msg[OrderFeedback.transaction.value] * np.array([1, 0, 0, 0])
        transaction_amount = (current_fx_data) * msg[OrderFeedback.current_data.value] * msg[OrderFeedback.transaction.value] * np.array([0, 1, 1, 1])
        open_price = np.array([21, 40*2/3, 100*4/5, 120*2/3], dtype=np.float64)

        assert (portfolio_manager.holding_value == (new_current_fx_data) * new_current_data * msg[OrderFeedback.transaction.value] * np.array([0, 1, 1, 1]) ).all()
        assert (portfolio_manager.pnl == equity_pnl + non_equity_pnl ).all()
        assert (portfolio_manager.cash == cash - sum(transaction_amount) + sum(non_equity_pnl) )
        assert (portfolio_manager.pv == cash + sum(equity_pnl + non_equity_pnl) ).all()
        assert np.allclose(portfolio_manager.open_price , open_price)

        new_msg = np.zeros((5,4))
        new_msg[OrderFeedback.transaction.value] = np.array([0, 0, 0, 0], dtype=np.float64)
        new_msg[OrderFeedback.current_data.value] = np.array([np.nan, 41, 101, 121], dtype=np.float64) 
        new_msg[OrderFeedback.commission_fee.value] = np.array([0, 0, 0, 0], dtype=np.float64)
        new_msg[OrderFeedback.current_fx_data.value] = np.array([3, 5], dtype=np.float64) @ ccy_matrix
        current_fx_data = new_msg[OrderFeedback.current_fx_data.value]
        portfolio_manager.receive_order_feedback(new_msg)

        assert (portfolio_manager.holding_value == (new_current_fx_data) * new_current_data * msg[OrderFeedback.transaction.value] * np.array([0, 1, 1, 1]) ).all()
        assert (portfolio_manager.pnl == equity_pnl + non_equity_pnl ).all()
        assert (portfolio_manager.cash == cash - sum(transaction_amount) + sum(non_equity_pnl) )
        assert (portfolio_manager.pv == cash + sum(equity_pnl + non_equity_pnl) ).all()
        assert np.allclose(portfolio_manager.open_price , open_price)

    def test_profolio_manager_field_25(self):
        cash = 1000000
        ccy_matrix = np.array([[1, 1, 0, 1], [0, 0, 1, 0]], dtype=np.float64)
        instrument_type = np.array([InstrumentType.FX.value, InstrumentType.HK_STOCK.value,InstrumentType.CN_STOCK.value, InstrumentType.FUTURES.value])
        portfolio_manager = PorfolioManager(cash, ccy_matrix, instrument_type)

        msg = np.zeros((5,4))
        msg[OrderFeedback.transaction.value] = np.array([100, 100, 100, 100], dtype=np.float64)
        msg[OrderFeedback.current_data.value] = np.array([20, 40, 100, 120], dtype=np.float64) 
        msg[OrderFeedback.commission_fee.value] = np.array([0, 0, 0, 0], dtype=np.float64)
        msg[OrderFeedback.current_fx_data.value] = np.array([2, 4], dtype=np.float64) @ ccy_matrix
        current_fx_data = msg[OrderFeedback.current_fx_data.value]
        portfolio_manager.receive_order_feedback(msg)

        new_current_data = np.array([21, 41, 101, 121], dtype=np.float64)
        new_current_fx_data = np.array([3, 5], dtype=np.float64) @ ccy_matrix
        portfolio_manager.calculate_spot(new_current_data,new_current_fx_data)

        equity_pnl = ( (new_current_fx_data)* new_current_data - (current_fx_data) * msg[OrderFeedback.current_data.value] )* msg[OrderFeedback.transaction.value] * np.array([0, 1, 1, 0])
        non_equity_pnl = (new_current_fx_data)* (new_current_data - msg[OrderFeedback.current_data.value]) * msg[OrderFeedback.transaction.value] * np.array([1, 0, 0, 1])
        transaction_amount = (current_fx_data) * msg[OrderFeedback.current_data.value] * msg[OrderFeedback.transaction.value] * np.array([0, 1, 1, 0])
        open_price = np.array([21, 40*2/3, 100*4/5, 121], dtype=np.float64)

        assert (portfolio_manager.holding_value == (new_current_fx_data) * new_current_data * msg[OrderFeedback.transaction.value] * np.array([0, 1, 1, 0]) ).all()
        assert (portfolio_manager.pnl == equity_pnl + non_equity_pnl ).all()
        assert (portfolio_manager.cash == cash - sum(transaction_amount) + sum(non_equity_pnl) )
        assert (portfolio_manager.pv == cash + sum(equity_pnl + non_equity_pnl) ).all()
        assert np.allclose(portfolio_manager.open_price , open_price)

        current_rate_data = np.array([0.02, 10, 20, 30], dtype=np.float64)
        portfolio_manager.calculate_rate(current_rate_data, new_current_data, new_current_fx_data)
        pnl_fx_type =  (new_current_fx_data)* (instrument_type==InstrumentType.FX.value) * new_current_data * portfolio_manager.current_holding * current_rate_data
        pnl_non_fx_type =  (new_current_fx_data)* (instrument_type!=InstrumentType.FX.value) * portfolio_manager.current_holding * current_rate_data
        interest_pnl = pnl_fx_type + pnl_non_fx_type

        assert (portfolio_manager.holding_value == (new_current_fx_data) * new_current_data * msg[OrderFeedback.transaction.value] * np.array([0, 1, 1, 0])).all()
        assert (portfolio_manager.pnl == equity_pnl + non_equity_pnl + interest_pnl  ).all()
        assert (portfolio_manager.cash == cash - sum(transaction_amount) + sum(non_equity_pnl) + sum(interest_pnl) )
        assert (portfolio_manager.pv == cash + sum(equity_pnl + non_equity_pnl + interest_pnl) ).all()

    def test_profolio_manager_field_26(self):
        cash = 1000000
        ccy_matrix = np.array([[1, 1, 0, 1], [0, 0, 1, 0]], dtype=np.float64)
        instrument_type = np.array([InstrumentType.FX.value, InstrumentType.HK_STOCK.value,InstrumentType.CN_STOCK.value, InstrumentType.FUTURES.value])
        portfolio_manager = PorfolioManager(cash, ccy_matrix, instrument_type)

        trade_status = [TradeStatus.NORMAL.value,TradeStatus.NORMAL.value,TradeStatus.NORMAL.value,TradeStatus.NORMAL.value]
        portfolio_manager.reset_trade_status(trade_status)

        order = np.array([100, 100, 100, 100], dtype=np.float64)
        revised_order , order_status = portfolio_manager.check_order(order)
        assert (order == revised_order).all()

        msg = np.zeros((5,4))
        msg[OrderFeedback.transaction.value] = np.array([100, 100, 100, 100], dtype=np.float64)
        msg[OrderFeedback.current_data.value] = np.array([20, 40, 100, 120], dtype=np.float64) 
        msg[OrderFeedback.commission_fee.value] = np.array([0, 0, 0, 0], dtype=np.float64)
        msg[OrderFeedback.current_fx_data.value] = np.array([2, 4], dtype=np.float64) @ ccy_matrix
        current_fx_data = msg[OrderFeedback.current_fx_data.value]
        portfolio_manager.receive_order_feedback(msg)

        new_current_data = np.array([21, 41, 101, 121], dtype=np.float64)
        new_current_fx_data = np.array([3, 5], dtype=np.float64) @ ccy_matrix
        portfolio_manager.calculate_spot(new_current_data,new_current_fx_data)

        equity_pnl = ( (new_current_fx_data)* new_current_data - (current_fx_data) * msg[OrderFeedback.current_data.value] )* msg[OrderFeedback.transaction.value] * np.array([0, 1, 1, 0])
        non_equity_pnl = (new_current_fx_data)* (new_current_data - msg[OrderFeedback.current_data.value]) * msg[OrderFeedback.transaction.value] * np.array([1, 0, 0, 1])
        transaction_amount = (current_fx_data) * msg[OrderFeedback.current_data.value] * msg[OrderFeedback.transaction.value] * np.array([0, 1, 1, 0])
        open_price = np.array([21, 40*2/3, 100*4/5, 121], dtype=np.float64)

        assert (portfolio_manager.holding_value == (new_current_fx_data) * new_current_data * msg[OrderFeedback.transaction.value] * np.array([0, 1, 1, 0]) ).all()
        assert (portfolio_manager.pnl == equity_pnl + non_equity_pnl ).all()
        assert (portfolio_manager.cash == cash - sum(transaction_amount) + sum(non_equity_pnl) )
        assert (portfolio_manager.pv == cash + sum(equity_pnl + non_equity_pnl) ).all()
        assert np.allclose(portfolio_manager.open_price , open_price)

        current_rate_data = np.array([0.02, 10, 20, 30], dtype=np.float64)
        portfolio_manager.calculate_rate(current_rate_data, new_current_data, new_current_fx_data)
        pnl_fx_type =  (new_current_fx_data)* (instrument_type==InstrumentType.FX.value) * new_current_data * portfolio_manager.current_holding * current_rate_data
        pnl_non_fx_type =  (new_current_fx_data)* (instrument_type!=InstrumentType.FX.value) * portfolio_manager.current_holding * current_rate_data
        interest_pnl = pnl_fx_type + pnl_non_fx_type

        assert (portfolio_manager.holding_value == (new_current_fx_data) * new_current_data * msg[OrderFeedback.transaction.value] * np.array([0, 1, 1, 0])).all()
        assert (portfolio_manager.pnl == equity_pnl + non_equity_pnl + interest_pnl  ).all()
        assert (portfolio_manager.cash == cash - sum(transaction_amount) + sum(non_equity_pnl) + sum(interest_pnl) )
        assert (portfolio_manager.pv == cash + sum(equity_pnl + non_equity_pnl + interest_pnl) ).all()


        trade_status = [TradeStatus.STOP.value,TradeStatus.STOPSELL.value,TradeStatus.STOPBUY.value,TradeStatus.NORMAL.value]
        portfolio_manager.reset_trade_status(trade_status)
        order = np.array([-100, -100, -100, -100], dtype=np.float64)
        revised_order , order_status = portfolio_manager.check_order(order)
        expect_order = np.array([0, 0, 0, -100], dtype=np.float64)
        assert (expect_order == revised_order).all()

        portfolio_manager.reset_field_rollover_day()
        trade_status = [TradeStatus.STOP.value,TradeStatus.STOPSELL.value,TradeStatus.STOPBUY.value,TradeStatus.NORMAL.value]
        portfolio_manager.reset_trade_status(trade_status)
        order = np.array([-100, -100, -100, -100], dtype=np.float64)
        revised_order , order_status = portfolio_manager.check_order(order)
        expect_order = np.array([0, 0, -100, -100], dtype=np.float64)
        assert (expect_order == revised_order).all()

        portfolio_manager.reset_field_rollover_day()
        trade_status = [TradeStatus.STOP.value,TradeStatus.STOPSELL.value,TradeStatus.STOPBUY.value,TradeStatus.NORMAL.value]
        portfolio_manager.reset_trade_status(trade_status)
        order = np.array([100, 100, 100, 100], dtype=np.float64)
        revised_order , order_status = portfolio_manager.check_order(order)
        expect_order = np.array([0, 100, 0, 100], dtype=np.float64)
        assert (expect_order == revised_order).all()

    def test_profolio_manager_field_27(self):
        cash = 1000000
        ccy_matrix = np.array([[1, 1, 0, 1], [0, 0, 1, 0]], dtype=np.float64)
        instrument_type = np.array([InstrumentType.FX.value, InstrumentType.HK_STOCK.value,InstrumentType.CN_STOCK.value, InstrumentType.US_STOCK.value])
        portfolio_manager = PorfolioManager(cash, ccy_matrix, instrument_type)

        msg = np.zeros((5,4))
        msg[OrderFeedback.transaction.value] = np.array([100, 100, 100, 100], dtype=np.float64)
        msg[OrderFeedback.current_data.value] = np.array([20, 40, 100, 120], dtype=np.float64) 
        msg[OrderFeedback.commission_fee.value] = np.array([0, 0, 0, 0], dtype=np.float64)
        msg[OrderFeedback.current_fx_data.value] = np.array([2, 4], dtype=np.float64) @ ccy_matrix
        current_fx_data = msg[OrderFeedback.current_fx_data.value]

        portfolio_manager.receive_order_feedback(msg)

        assert (portfolio_manager.holding_value == (current_fx_data) * msg[OrderFeedback.current_data.value] * msg[OrderFeedback.transaction.value] * np.array([0, 1, 1, 1]) ).all()
        assert (portfolio_manager.pnl == np.array([0, 0, 0, 0]) ).all()
        assert (portfolio_manager.symbol_cash == -(current_fx_data) * msg[OrderFeedback.current_data.value] * msg[OrderFeedback.transaction.value] * np.array([0, 1, 1, 1]) ).all()
        assert (portfolio_manager.cash == cash - sum((current_fx_data) * msg[OrderFeedback.current_data.value] * msg[OrderFeedback.transaction.value] * np.array([0, 1, 1, 1])) )
        assert (portfolio_manager.pv == cash )
        assert (portfolio_manager.open_price == msg[OrderFeedback.current_data.value] ).all()

        split_ratio = np.array([2, 3, 4, 5])
        portfolio_manager.calculate_split(split_ratio)
        assert (portfolio_manager.current_holding == msg[OrderFeedback.transaction.value]*split_ratio).all()
        assert (portfolio_manager.current_data  == msg[OrderFeedback.current_data.value]/split_ratio).all()
        #holding value should be the same
        assert (portfolio_manager.holding_value == (current_fx_data) * msg[OrderFeedback.current_data.value] * msg[OrderFeedback.transaction.value] * np.array([0, 1, 1, 1]) ).all()

        


