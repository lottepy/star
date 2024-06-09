import numpy as np
from numba import boolean, float64, int32, int64, jit, jitclass, typed, types

from ..utils.constants import InstrumentType,Time

MAX_SELL_AMOUNT = 100000000

class OrderManagerBase:

    def __init__(self, CASH, commission, ccy_matrix, instrument_type_array):

        self.ccy_matrix = np.copy(ccy_matrix)
        self.n_security = self.ccy_matrix.shape[1]
        self.n_ccy = self.ccy_matrix.shape[0]
        self.commission = np.copy(commission)
        self.current_holding = np.zeros(self.n_security)
        self.today_long_amount = np.zeros(self.n_security)
        self.today_short_amount = np.zeros(self.n_security)
        self.commission_fee = np.zeros(self.n_security)
        self.instrument_type_array = np.copy(instrument_type_array)

        is_cn_equity_array = (self.instrument_type_array == InstrumentType.CN_STOCK.value)
        is_hk_equity_array = (self.instrument_type_array == InstrumentType.HK_STOCK.value)
        self.allow_sell_amount = self.current_holding * (is_cn_equity_array | is_hk_equity_array) + \
                                 MAX_SELL_AMOUNT * ~(is_cn_equity_array | is_hk_equity_array)

        self.ref_aum = np.float(CASH)
        self.init_cash = np.float(CASH)
        self.symbol_cash = np.zeros(self.n_security)
        self.transaction = np.zeros(self.n_security)
        self.have_transaction = False
        self.empty_array = np.zeros(self.n_security)
        self.current_data = np.zeros(self.n_security)
        self.current_fx_data = np.zeros(self.n_ccy)
        self.is_tradable = np.array([True for symbol in range(self.n_security)])

        self.historial_pnl = []  # typed.List.empty_list(type_1d_array)
        self.historial_position = []  # typed.List.empty_list(type_1d_array)
        self.historial_commission_fee = []  # typed.List.empty_list(type_1d_array)
        self.historial_time = []  # typed.List.empty_list(int64)

    def close_position(self, current_data, current_fx_data):
        self.cross_order(np.zeros(self.n_security), current_data, current_fx_data)

    def cross_target(self,target, current_data, current_fx_data):
        order  = None
        if target is not None:
            order = target - self.current_holding
        return self.cross_order(order, current_data, current_fx_data)

    # @profile
    def cross_order(self, order, current_data, current_fx_data):
        """ handle order and filter order by allow_sell_amount for diff instrument type,eg cn equity can't sell more than base position """
        #self.calculate_spot(current_data, current_fx_data)
        if order is None:
            return None
        self.calculate_spot(current_data, current_fx_data, is_equity_included=True)
        self.transaction = np.copy(order)
        self.transaction[~ self.is_tradable] = 0
        self.transaction = np.maximum(self.transaction, -1 * self.allow_sell_amount)  # order的item可能为负

        is_cn_equity_array = (self.instrument_type_array == InstrumentType.CN_STOCK.value)
        is_hk_equity_array = (self.instrument_type_array == InstrumentType.HK_STOCK.value)
        is_us_equity_array = (self.instrument_type_array == InstrumentType.US_STOCK.value)
        is_equity_array = is_cn_equity_array | is_hk_equity_array | is_us_equity_array

        self.allow_sell_amount += np.minimum(self.empty_array, self.transaction) * is_cn_equity_array + \
                                  self.transaction * is_hk_equity_array
        self.today_long_amount += np.maximum(self.empty_array, self.transaction)  # >0表示有买入
        self.today_short_amount += np.minimum(self.empty_array, self.transaction)  # <0表示有卖出
        self.have_transaction = (self.empty_array != self.transaction).any()
        self.commission_fee = np.abs(self.transaction) * self.commission * (current_fx_data @ self.ccy_matrix) * current_data
        self.symbol_cash -= self.commission_fee

        self.symbol_cash -= (current_fx_data @ self.ccy_matrix) * current_data * order * is_equity_array

        self.current_holding = self.current_holding + self.transaction
        self.current_data = current_data
        self.current_fx_data = current_fx_data

        order_feedback_msg = {"position": np.copy(self.current_holding)}
        return order_feedback_msg

    def calculate_spot(self, current_data, current_fx_data, is_equity_included=False):
        #return
        """ calculate symbol_cash due to price diff and convert to base ccy """
        if (self.current_holding == self.empty_array).all():
            return
        pnl_base_ccy = calc_pnl_base_ccy_numba(self.instrument_type_array, current_data, self.current_data, self.current_holding,
                                               current_fx_data, self.ccy_matrix)

        self.symbol_cash += pnl_base_ccy *self.is_tradable
        self.current_fx_data = current_fx_data
        self.current_data = current_data

    def calculate_rate(self, current_rate, current_data, current_fx_data):
        #return
        multiple_i = np.copy(current_rate)
        symbol_cash = multiple_i * self.current_holding * current_data
        pnl_base_ccy = (current_fx_data @ self.ccy_matrix) * symbol_cash
        self.symbol_cash += pnl_base_ccy *self.is_tradable

    def reset_field_rollover_day(self,is_tradable):
        """ reset allow_sell_amount and other field in order manager, used to control allow sell amount in diff amount """
        self.today_long_amount = np.zeros(self.n_security)
        # self.short_long_amount = np.zeros(self.n_security)  # unused
        is_cn_equity_array = (self.instrument_type_array == InstrumentType.CN_STOCK.value)
        is_hk_equity_array = (self.instrument_type_array == InstrumentType.HK_STOCK.value)
        self.allow_sell_amount = self.current_holding * (is_cn_equity_array | is_hk_equity_array) + \
                                 MAX_SELL_AMOUNT * ~(is_cn_equity_array | is_hk_equity_array)
        self.is_tradable = is_tradable

    def capture(self, current_time):
        """ capture snapshot of order manager and save in csv """
        self.historial_time.append(current_time)
        self.historial_pnl.append(np.copy(self.pnl))
        self.historial_position.append(self.current_holding)
        self.historial_commission_fee.append(self.commission_fee)
        self.commission_fee = np.zeros(self.n_security)

    def forget(self,record_ts):
        historial_time_ts = np.array(self.historial_time)
        index = len(historial_time_ts[historial_time_ts<record_ts])
        self.historial_time = self.historial_time[index:]
        self.historial_pnl = self.historial_pnl[index:]
        self.historial_position = self.historial_position[index:]
        self.historial_commission_fee = self.historial_commission_fee[index:]

    @property
    def holding_value(self):
        is_cn_equity_array = (self.instrument_type_array == InstrumentType.CN_STOCK.value)
        is_hk_equity_array = (self.instrument_type_array == InstrumentType.HK_STOCK.value)
        is_us_equity_array = (self.instrument_type_array == InstrumentType.US_STOCK.value)
        is_equity_array = is_cn_equity_array | is_hk_equity_array | is_us_equity_array
        return (self.current_fx_data @ self.ccy_matrix) * self.current_data * self.current_holding * is_equity_array

    @property
    def pnl(self):
        return self.symbol_cash + self.holding_value

    @property
    def cash(self):
        return self.init_cash + np.sum(self.symbol_cash)

    @property
    def pv(self):
        return self.init_cash + np.sum(self.pnl)


# @jit(nopython=True, cache=True)
def calc_pnl_base_ccy_numba(instrument_type_array, current_data_new, current_data_old, current_holding,
                            current_fx_data, ccy_matrix):
    is_cn_equity_array = (instrument_type_array == InstrumentType.CN_STOCK.value)
    is_hk_equity_array = (instrument_type_array == InstrumentType.HK_STOCK.value)
    is_us_equity_array = (instrument_type_array == InstrumentType.US_STOCK.value)
    is_equity_array = is_cn_equity_array | is_hk_equity_array | is_us_equity_array
    non_equity_array = ~is_equity_array

    pnl_local_ccy = (current_data_new - current_data_old) * current_holding * non_equity_array
    pnl_base_ccy = (current_fx_data @ ccy_matrix) * pnl_local_ccy
    # pnl_base_ccy = np.dot(current_fx_data, ccy_matrix) * pnl_local_ccy
    return pnl_base_ccy
