import numba as nb
import numpy as np
from numba import types
from numba.experimental import jitclass
from numba.typed import Dict, List

from quantcycle.utils.production_constant import (MAX_SELL_AMOUNT,
                                                  InstrumentType,
                                                  OrderFeedback,
                                                  PmsRejectReason, PmsStatus,
                                                  TradeStatus)
from quantcycle.utils.production_helper import (calc_pnl_base_ccy_numba,
                                                calc_rate_base_ccy)

accepted_error = 0.1

float64_1d_array_type = nb.float64[:]
@jitclass({
    'ccy_matrix': nb.float64[:, :],
    'n_security': nb.int64,
    'n_ccy': nb.int64,
    'current_holding': nb.float64[:],
    'instrument_type_array': nb.int64[:],
    'commission_fee': nb.float64[:],
    'acc_commission_fee': nb.float64[:],
    'is_cn_equity_array': nb.boolean[:],
    'is_hk_equity_array': nb.boolean[:],
    'is_us_equity_array': nb.boolean[:],
    'is_equity_array': nb.boolean[:],
    'non_equity_array': nb.boolean[:],
    'allow_sell_amount': nb.float64[:],
    'init_cash': nb.float64,
    'cash_residual': nb.float64,
    'symbol_cash': nb.float64[:],
    'dividends': nb.float64[:],
    'current_data': nb.float64[:],
    'current_fx_data': nb.float64[:],
    'empty_array': nb.float64[:],
    'trade_status': nb.int64[:],
    'historial_pnl': types.ListType(float64_1d_array_type),
    'historial_position': types.ListType(float64_1d_array_type),
    'historial_commission_fee': types.ListType(float64_1d_array_type),
    'historial_time': types.ListType(nb.int64),
    'historial_cash_residual': types.ListType(nb.float64),
    'check_order_log': types.DictType(nb.int64, types.unicode_type)
})
class PorfolioManager:
    def __init__(self, CASH, ccy_matrix, instrument_type_array):
        self.ccy_matrix = np.copy(ccy_matrix)
        self.n_security = self.ccy_matrix.shape[1]
        self.n_ccy = self.ccy_matrix.shape[0]
        self.current_holding = np.zeros(self.n_security)
        self.instrument_type_array = np.copy(instrument_type_array)
        self.commission_fee = np.zeros(self.n_security)
        self.acc_commission_fee = np.zeros(self.n_security)

        self.is_cn_equity_array = (self.instrument_type_array == InstrumentType.CN_STOCK.value)
        self.is_hk_equity_array = (self.instrument_type_array == InstrumentType.HK_STOCK.value)
        self.is_us_equity_array = (self.instrument_type_array == InstrumentType.US_STOCK.value)
        self.is_equity_array = self.is_cn_equity_array | self.is_hk_equity_array | self.is_us_equity_array
        self.non_equity_array = ~self.is_equity_array
        self.allow_sell_amount = self.current_holding * (self.is_cn_equity_array | self.is_hk_equity_array) + \
                                 MAX_SELL_AMOUNT * ~(self.is_cn_equity_array | self.is_hk_equity_array)

        self.init_cash = np.float(CASH)
        self.symbol_cash = np.zeros(self.n_security)
        self.dividends = np.zeros(self.n_security)
        self.cash_residual = 0
        self.current_data = np.zeros(self.n_security)
        self.current_fx_data = np.zeros(self.n_security)
        self.empty_array = np.zeros(self.n_security)
        self.trade_status = ( np.ones(self.n_security) * TradeStatus.NORMAL.value).astype(np.int64)

        self.historial_pnl = List.empty_list(float64_1d_array_type)
        self.historial_position = List.empty_list(float64_1d_array_type)
        self.historial_commission_fee = List.empty_list(float64_1d_array_type)
        self.historial_cash_residual = List.empty_list(nb.float64)
        self.historial_time = List.empty_list(nb.int64)
        self.check_order_log = Dict.empty(nb.int64, types.unicode_type)

    # check order legality with cash and instrument_type and holding
    def check_order(self, order, ts=0):
        transaction = np.copy(order)
        transaction_amount = (self.current_fx_data) * self.current_data * transaction * self.is_equity_array
        transaction_status = (np.ones(self.n_security * PmsStatus.LEGAL.value)).astype(np.int64)
        pms_reject_reason = np.array(['--' for _ in range(len(transaction))])
        if self.cash + accepted_error < np.sum(transaction_amount):
            transaction = np.zeros(self.n_security)
            transaction_status = ( np.ones(self.n_security) * PmsStatus.ILLEGAL.value).astype(np.int64)
            pms_reject_reason = np.array([PmsRejectReason.NO_CASH.value for _ in range(len(transaction))])
            self.check_order_log[ts] = ' '.join(pms_reject_reason)
        else:
            # check nan in order
            is_nan_transaction_identifer = np.isnan(transaction)
            transaction_status[is_nan_transaction_identifer] = PmsStatus.ILLEGAL.value
            pms_reject_reason[is_nan_transaction_identifer] = PmsRejectReason.NAN_ORDER.value
            transaction[is_nan_transaction_identifer] = 0

            # check trade status

            # stop trade
            stop_trade_identifier = self.trade_status == TradeStatus.STOP.value
            transaction_status[stop_trade_identifier] = PmsStatus.ILLEGAL.value
            pms_reject_reason[stop_trade_identifier] = PmsRejectReason.STOP_TRADE.value

            # other case
            stop_buy_trade_filter_identifier = (self.trade_status == TradeStatus.STOPBUY.value) & (transaction > 0)
            transaction_status[stop_buy_trade_filter_identifier] = PmsStatus.ILLEGAL.value
            pms_reject_reason[stop_buy_trade_filter_identifier] = PmsRejectReason.STOP_BUY.value

            stop_sell_trade_filter_identifier = (self.trade_status == TradeStatus.STOPSELL.value) & (transaction < 0)
            transaction_status[stop_sell_trade_filter_identifier] = PmsStatus.ILLEGAL.value
            pms_reject_reason[stop_buy_trade_filter_identifier] = PmsRejectReason.STOP_SELL.value

            cannot_sell_trade_filter_identifier = (-1 * transaction > self.allow_sell_amount)
            transaction_status[cannot_sell_trade_filter_identifier] = PmsStatus.ILLEGAL.value
            pms_reject_reason[cannot_sell_trade_filter_identifier] = PmsRejectReason.LARGE_THAN_SELL_LIMIT.value

            reject_identifier = stop_trade_identifier | stop_buy_trade_filter_identifier | stop_sell_trade_filter_identifier | cannot_sell_trade_filter_identifier
            if len(transaction_status[reject_identifier]) > 0:
                self.check_order_log[ts] = ' '.join(pms_reject_reason)

            transaction[transaction_status == PmsStatus.ILLEGAL.value] = 0

        return transaction, transaction_status

    # reset field like base position
    # call per day start
    def reset_field_rollover_day(self):
        self.allow_sell_amount = self.current_holding * (self.is_cn_equity_array | self.is_hk_equity_array) + \
                                 MAX_SELL_AMOUNT * ~(self.is_cn_equity_array | self.is_hk_equity_array)
        #if len(trade_status) == len(self.trade_status):
        #    self.trade_status = np.copy(trade_status)
    
    def reset_trade_status(self, trade_status):
        valid_status = {TradeStatus.NORMAL.value, TradeStatus.STOP.value, TradeStatus.STOPBUY.value,
                        TradeStatus.STOPSELL.value}
        if False in [x in valid_status for x in trade_status]:
            print(trade_status)
            print('^')
            raise Exception("trade_status not in TradeStatus enum")
        self.trade_status = np.copy(trade_status)

    # update field with order feedback
    def receive_order_feedback(self, msg):
        transaction = msg[OrderFeedback.transaction.value]
        current_data = msg[OrderFeedback.current_data.value]
        current_fx_data = msg[OrderFeedback.current_fx_data.value]
        self.commission_fee = msg[OrderFeedback.commission_fee.value]
        self.acc_commission_fee += msg[OrderFeedback.commission_fee.value]

        self.allow_sell_amount += np.minimum(self.empty_array, transaction) * self.is_cn_equity_array + transaction * self.is_hk_equity_array
        self.calculate_spot(current_data, current_fx_data)
        transaction_amount = self.current_fx_data * self.current_data * transaction * self.is_equity_array
        self.symbol_cash -= transaction_amount
        self.current_holding += transaction
        #self.current_data = np.copy(current_data)
        #self.current_fx_data = np.copy(current_fx_data)

    def sync_cash(self, cash):
        cash_residual = cash - self.cash
        self.cash_residual = cash_residual
        return self.cash_residual

    def sync_holding(self, holding):
        if len(holding) != len(self.current_holding):
            print(holding)
            print('^')
            raise Exception("holding: length does not match")
        self.current_holding = np.copy(holding)

    def sync_current_data(self, current_data):
        if len(current_data) != len(self.current_data):
            print(current_data)
            print('^')
            raise Exception("current_data: length does not match")
        #self.capture(current_time)
        temp_current_data = np.copy(current_data)
        nan_indicator = np.isnan(temp_current_data)
        temp_current_data[nan_indicator] = self.current_data[nan_indicator]
        self.current_data = np.copy(temp_current_data)

    def sync_current_fx_data(self, current_fx_data):
        if len(current_fx_data) != len(self.current_fx_data):
            print(current_fx_data)
            print('^')
            raise Exception("current_fx_data: length does not match")
        #self.capture(current_time)
        temp_current_fx_data = np.copy(current_fx_data)
        nan_indicator = np.isnan(temp_current_fx_data)
        temp_current_fx_data[nan_indicator] = self.current_fx_data[nan_indicator]
        self.current_fx_data = np.copy(temp_current_fx_data)

    # calculate pms for future and FX
    def calculate_spot(self, current_data, current_fx_data):
        pnl_base_ccy = calc_pnl_base_ccy_numba(current_data, self.current_data, self.current_holding, current_fx_data) * self.non_equity_array
        nan_indicator = np.isnan(pnl_base_ccy)
        pnl_base_ccy[nan_indicator] = 0
        self.symbol_cash += pnl_base_ccy
        self.sync_current_data(current_data)
        self.sync_current_fx_data(current_fx_data)

    # calculate pnl from dividends
    def calculate_rate(self, current_rate, current_data, current_fx_data):
        self.calculate_spot(current_data, current_fx_data)
        temp_current_rate = np.copy(current_rate)
        current_holding = np.copy(self.current_holding)
        #nan_identfier = np.isnan(temp_current_rate)
        #temp_current_rate[nan_identfier] = 0
        fx_identifier = (self.instrument_type_array == InstrumentType.FX.value)
        pnl_fx = temp_current_rate * current_holding * self.current_data * fx_identifier
        non_fx_identifier = ~fx_identifier
        pnl_non_fx = temp_current_rate * current_holding * non_fx_identifier
        pnl = pnl_fx + pnl_non_fx
        pnl_base_ccy = self.current_fx_data * pnl
        self.dividends += pnl_base_ccy

    def calculate_split(self, split_ratio):
        self.current_holding *= split_ratio
        self.sync_current_data(self.current_data/split_ratio)

    # calculate pnl from dividends
    def capture(self, current_time):
        self.historial_time.append(current_time)
        self.historial_pnl.append(np.copy(self.pnl))
        self.historial_position.append(np.copy(self.current_holding))
        self.historial_commission_fee.append(np.copy(self.commission_fee))
        self.commission_fee = np.zeros(self.n_security)
        self.historial_cash_residual.append(self.cash_residual)

    def del_log(self):
        self.check_order_log = Dict.empty(nb.int64,types.unicode_type)


    @property
    def holding_value(self):
        return (self.current_fx_data) * self.current_data * self.current_holding * self.is_equity_array

    @property
    def symbol_cash_with_fee(self):
        return self.symbol_cash - self.acc_commission_fee + self.dividends

    @property
    def cash(self):
        return self.init_cash + np.sum(self.symbol_cash_with_fee) + self.cash_residual

    @property
    def pnl(self):
        return self.symbol_cash_with_fee + self.holding_value

    @property
    def pv(self):
        return self.cash + np.sum(self.holding_value)

    @property
    def open_price(self):
        non_zero_holding_identifier = (self.current_holding != 0)
        current_holding = self.current_holding * non_zero_holding_identifier

        open_price_equity = -1 * (1/self.current_fx_data) * self.symbol_cash /current_holding
        open_price_non_equity = self.current_data
        open_price = open_price_equity * self.is_equity_array + open_price_non_equity *(self.non_equity_array)
        return open_price


if __name__ == "__main__":
    cash = 1000000
    ccy_matrix = np.array([[1, 1, 0, 1], [0, 0, 1, 0]], dtype=np.float64)
    instrument_type = np.array([InstrumentType.FX.value, InstrumentType.HK_STOCK.value, InstrumentType.CN_STOCK.value,
                                InstrumentType.US_STOCK.value]).astype(np.int64)
    portfolio_manager = PorfolioManager(cash, ccy_matrix, instrument_type)

    msg = np.zeros((5, 4))
    msg[OrderFeedback.transaction.value] = np.array([100, 100, 100, 100], dtype=np.float64)
    msg[OrderFeedback.current_data.value] = np.array([20, 40, 100, 120], dtype=np.float64)
    current_fx_data_1 = np.array([2, 4], dtype=np.float64) @ ccy_matrix
    msg[OrderFeedback.current_fx_data.value] = current_fx_data_1
    msg[OrderFeedback.commission_fee.value] = np.array([0, 0, 0, 0], dtype=np.float64)
    portfolio_manager.receive_order_feedback(msg)

    current_data = np.array([21, 41, 101, 121], dtype=np.float64)
    current_fx_data = np.array([3, 5], dtype=np.float64) @ ccy_matrix
    portfolio_manager.calculate_spot(current_data, current_fx_data)

    order = np.array([100, 100, 100, 100], dtype=np.float64)
    revised_order, order_status = portfolio_manager.check_order(order)
    target_revised_order = np.array([100, 100, 100, 100], dtype=np.float64)
    status = np.array([PmsStatus.LEGAL.value, PmsStatus.LEGAL.value, PmsStatus.LEGAL.value, PmsStatus.LEGAL.value],
                      dtype=np.float64)

    assert (target_revised_order == revised_order).all()
    assert (order_status == status).all()
