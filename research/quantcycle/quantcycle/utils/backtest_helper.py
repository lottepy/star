import importlib
import numba as nb
import numpy as np
import pandas as pd
import re
import os
import pytz
from numba import jit
from numba import types
from datetime import datetime


class WindowDataGenerator:
    def __init__(self, window_size):
        self.window_size = window_size
        self.data_size = -1
        # self.window_data = np.zeros((self.window_size,self.data_size))
        self.index = self.window_size

    def receive_data(self, current_data):
        if self.data_size == -1:
            self.data_size = len(current_data)
            self.window_data = np.zeros((self.window_size, self.data_size))
        self.window_data[:-1] = self.window_data[1:]
        self.window_data[-1] = current_data
        self.index -= 1
        if self.index <= 0:
            return np.copy(self.window_data)
        else:
            return None


def init_strategy_pms(strategy_dict: dict, config, symbol2instrument_type_dict, symbol2ccy_dict):
    from quantcycle.app.pms_manager.portfolio_manager import PorfolioManager
    symbols = strategy_dict["symbol"]
    ccy_matrix = get_symbol2ccy_matrix(symbols, symbol2ccy_dict)
    symbol2instrument_type = np.array([symbol2instrument_type_dict[symbol] for symbol in symbols])
    pms = PorfolioManager(config["cash"], ccy_matrix, symbol2instrument_type)
    STRATEGY_MODULE = importlib.import_module(strategy_dict["strategy_module"])
    base_strategy = getattr(STRATEGY_MODULE, strategy_dict["strategy_name"])()
    base_strategy.init(pms, strategy_dict["strategy_name"], strategy_dict["params"], ccy_matrix.columns,
                       ccy_matrix.index, ccy_matrix.values)
    return base_strategy, pms, ccy_matrix


@jit(nopython=True)
def calc_pnl_base_ccy_numba(current_data_new, current_data_old, current_holding, current_fx_data):
    pnl_local_ccy = (current_data_new - current_data_old) * current_holding
    pnl_base_ccy = (current_fx_data) * pnl_local_ccy
    return pnl_base_ccy


@jit(nopython=True)
def calc_rate_base_ccy(current_holding, current_rate, current_data, current_fx_data):
    # return
    multiple_i = np.copy(current_rate)
    symbol_cash = multiple_i * current_holding * current_data
    pnl_base_ccy = (current_fx_data * symbol_cash)
    return pnl_base_ccy


def get_symbol2ccy_matrix(symbols, symbol2ccy_dict):
    """
        returns a DataFrame, as matrix with columns = symbols and index = ccys
    """
    temp_symbols = sorted(symbols)
    all_ccy = sorted(set(symbol2ccy_dict.values()))
    symbol2ccy_matrix = pd.DataFrame(np.zeros((len(all_ccy), len(temp_symbols))), index=all_ccy, columns=temp_symbols)
    for sym in temp_symbols:
        ccy = symbol2ccy_dict[sym]
        symbol2ccy_matrix.loc[ccy][sym] = 1
    return symbol2ccy_matrix


# -----------------------------------------------------------
class Fake:
    def __init__(self):
        pass


def transfer_jitclass_attribution(jitclass, attr_names=None):
    # input: jitclass
    # output: normal class contain jitclass' attribution
    #         do not contain function of jitclass
    fake = Fake()
    if attr_names is None:
        attr_names = [name for name in dir(jitclass) if not name.startswith('_')]
    for name in attr_names:
        attr = getattr(jitclass, name)
        if isinstance(attr, nb.typed.List):
            attr = [element for element in attr]
        if isinstance(attr, nb.typed.Dict):
            attr = dict([(key, value) for key, value in attr.items()])
        if hasattr(attr, '__call__'):
            continue
        setattr(fake, name, attr)
    return fake


# ------------------------------------------
def remove_files_matching(folder, pattern):
    for f in os.listdir(folder):
        if re.search(pattern, f):
            os.remove(os.path.join(folder, f))


# ------------------------------------------
timedelta64_1sec = np.timedelta64(1, 's')
timedelta64_1day = np.timedelta64(1, 'D')
unix_epoch = np.datetime64('1970-01-01T00:00:00')


@nb.njit(nb.boolean(nb.int64, nb.int64, types.NPDatetime('s')))
def is_new_day(current_timestamp: int, last_timestamp: int, anchor_time):
    # A numpy datetime64 implementation

    if last_timestamp == -1:  # beginning day
        return False

    assert current_timestamp >= last_timestamp
    current_time = unix_epoch + current_timestamp * timedelta64_1sec
    last_time = unix_epoch + last_timestamp * timedelta64_1sec

    # Calculate 22:00 UTC cutoff
    delta_days = current_timestamp // 86400
    current_day_cutoff = anchor_time + delta_days * timedelta64_1day
    delta_days = last_timestamp // 86400
    last_day_cutoff = anchor_time + delta_days * timedelta64_1day

    cur_to_cur_cutoff_delta = (current_time - current_day_cutoff) / timedelta64_1sec
    last_to_cur_cutoff_delta = (last_time - current_day_cutoff) / timedelta64_1sec
    last_to_last_cutoff_delta = (last_time - last_day_cutoff) / timedelta64_1sec

    if last_to_cur_cutoff_delta >= 0:
        return False
    if (last_to_last_cutoff_delta >= 0 > cur_to_cur_cutoff_delta and
            last_day_cutoff + timedelta64_1day == current_day_cutoff):
        return False
    if (last_to_last_cutoff_delta <= cur_to_cur_cutoff_delta < 0 and
            last_day_cutoff == current_day_cutoff):
        return False
    return True


# -------------------------------------------------------------
def to_tester_csv(full_path, data_dict, time_dict, symbols):
    # Format data to Jiahao's tester input for futures debugging
    assert data_dict['FUTURES_MAIN'].shape[0] == data_dict['FUTURES_INT'].shape[0] == \
           time_dict['FUTURES_MAIN'].shape[0] == time_dict['FUTURES_INT'].shape[0]

    n_rows = time_dict['FUTURES_MAIN'].shape[0]
    n_cols = data_dict['FUTURES_MAIN'].shape[1] * (4 + 1)       # OHLC + interest
    data = np.empty((n_rows, n_cols))
    col_indices = []
    row_indices = []
    for i in range(n_rows):
        timestamp = time_dict['FUTURES_MAIN'][i, 0]
        date = datetime.fromtimestamp(timestamp, pytz.utc)
        row_indices.append(date.strftime('%m/%d/%Y'))
        for j in range(data_dict['FUTURES_MAIN'].shape[1]):     # symbols
            symbol = symbols[j]
            for k in range(4):      # OHLC
                field_name = ['open', 'high', 'low', 'close'][k]
                if i == 0:
                    col_name = f'{field_name}_{symbol}'
                    col_indices.append(col_name)
                data[i, j * 5 + k] = data_dict['FUTURES_MAIN'][i, j, k]
            if i == 0:
                col_indices.append(f'dividend_{symbol}')
            data[i, j * 5 + 4] = data_dict['FUTURES_INT'][i, j, 0]
    df = pd.DataFrame(data=data, index=row_indices, columns=col_indices)
    df.index.name = 'date'
    df.to_csv(full_path)


@nb.njit(nb.float64[:, :](nb.float64[:, :], nb.int64[:], nb.int64))
def second_sort(array, fix_columns, sort_column):
    '''
    :param fix_columns:  columns already sort
    :param sort_column:  column need to be sort
    :param array: array need to be sort
    :return: sorted array
    '''
    start_idx = 0
    last_same_group = False
    for i in range(1, array.shape[0]):
        last_row = array[i - 1]
        current_row = array[i]
        same_group = np.all(current_row[fix_columns] == last_row[fix_columns])
        if same_group & (~last_same_group):
            start_idx = i - 1
        if (~same_group) & last_same_group:
            sub_array = array[start_idx:i]
            array[start_idx:i] = sub_array[np.argsort(sub_array[:, sort_column])]
        if same_group & (i == (array.shape[0] - 1)):
            sub_a = array[start_idx:]
            array[start_idx:] = sub_a[np.argsort(sub_a[:, sort_column])]
        last_same_group = same_group
    return array


if __name__ == '__main__':
    a = np.array([[1, 3, 3, 3, 3, 2, 2, 2, 4],
                  [2, 1, 3, 2, 4, 5, 7, 6, 2]]).transpose()
    # b = index(a.astype(np.float64), 1, np.array([0,1]).astype(np.int64))
    # print(b)
    a = a[np.argsort(a[:, 0])]
    print(a)
    b = second_sort(np.array([0]).astype(np.int64), 1, a.astype(np.float64))
    print(b)
