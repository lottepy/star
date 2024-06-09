import multiprocessing
import os
from collections import defaultdict
from ctypes import c_double, c_float, c_int32, c_int64, c_uint32, c_uint64
from datetime import timedelta
from os import listdir
from typing import List

import numba as nb
import numpy as np
import pandas as pd
from datamaster import dm_client

from .ccy_mapping import (CCY_CURRENCY, CCY_INDEX, NDF, fx_ticker_dm_map,
                          instrument_type_map)
from .constants import GET_SYMBOL_TYPE_BATCH, Time

def hdf5_group_linking(group,key):
    value_list: np.ndarray = group[f"{key}_{0}"][()]
    for i in range(1,10000):
        value_list_index = f"{key}_{i}"
        if value_list_index in group:
            value_list = np.append(value_list,group[value_list_index][()],axis=0)
        else:
            break
    return value_list

def hdf5_group_delete_dataset(group,key):
    for i in range(1,10000):
        value_list_index = f"{key}_{i}"
        if value_list_index in group:
            del group[value_list_index]
        else:
            break
    return

class TimeIndexTracker:
    """ since data used in backtest are in diff freq,need to keep track latest data point which timepoint < current timepoint """
    def __init__(self, timestamp_list):
        self.time_index = 0
        self.timestamp_list = timestamp_list

    def update_time_index(self, current_ts):
        """ search the latest timepoint than smaller than  current_ts"""
        for i in range(self.time_index, len(self.timestamp_list)):
            if self.timestamp_list[i] > current_ts:
                break
            self.time_index = i




def get_time_index_tracker_dict(ref_info: dict, rate_time_array, fx_data_time_array):
    """ ref data or factor may not be same freq as trading,
        need keep track on each ref data or factor timepoint
        
        return a dict of TimeIndexTracker for diff ref data,factor ,dividends and fx data
    """
    time_index_tracker_dict = dict([(f"{ref_type}_{ticker}", TimeIndexTracker(list(map(lambda x: x[Time.TIMESTAMP.value], data["time_array"]))))
                                    for ref_type in ref_info.keys() for ticker, data in ref_info[ref_type].items()])
    time_index_tracker_dict["dividends"] = TimeIndexTracker(list(rate_time_array[:, Time.TIMESTAMP.value]))
    time_index_tracker_dict["fx_data"] = TimeIndexTracker(list(fx_data_time_array[:, Time.TIMESTAMP.value]))

    return time_index_tracker_dict


def get_ref_window_and_ref_window_time(ref_info: dict, time_index_tracker_dict: dict,
                                       ref_window_size: int):
    """
        after finding the latest timepoint by TimeIndexTracker,need to extract the window ref data or factor before latest timepoint
    """
    ref_window = defaultdict(dict)
    ref_window_time = defaultdict(dict)
    for ref_type in ref_info.keys():
        for ticker, data in ref_info[ref_type].items():
            tracker = time_index_tracker_dict[f"{ref_type}_{ticker}"]
            ref_window[ref_type][ticker] = data["data_array"][max(tracker.time_index + 1 - ref_window_size, 0): tracker.time_index + 1]
            ref_window_time[ref_type][ticker] = data["time_array"][max(tracker.time_index + 1 - ref_window_size, 0): tracker.time_index + 1]
    return ref_window, ref_window_time


def find_csv_filenames(path_to_dir, suffix=".csv"):
    if not os.path.isdir(path_to_dir):
        return []
    filenames = listdir(path_to_dir)
    return [filename.replace(suffix, "") for filename in filenames if filename.endswith(suffix)]


def get_ccy2ticker_matrix(ccys, fx_data_source, fx_backup_data_source, freq):
    """ get bbg ticker for each ccy because dm need to use bbg ticker to get data

        returns a DataFrame, with columns=['bbg_code', 'back_up_bbg_code', 'bbg_code', 'trading_ccy'] and index = ccys 
    """
    if freq != 'DAILY':
        fx_data_source = "BGNL"
        fx_backup_data_source = "BGNL"
    ccy2ticker_list = []
    for ccy in ccys:
        temp_ccy_type = {}
        if ccy == "USD":
            temp_ccy_type = {"ccy": ccy, "bbg_code": None, "back_up_bbg_code": None, "trading_ccy": ccy}
        elif ccy in CCY_CURRENCY:
            ticker = CCY_CURRENCY[ccy]
            ticker_prefix = ticker.split(" ")[0]
            ticker_suffix = ticker.split(" ")[2]
            trading_ccy = ticker_prefix[3:] if ccy not in NDF else ccy
            temp_ccy_type = {"ccy": ccy, "bbg_code": f"{ticker_prefix} {fx_data_source} {ticker_suffix}",
                             "back_up_bbg_code": f"{ticker_prefix} {fx_backup_data_source} {ticker_suffix}" if fx_backup_data_source is not None else None,
                             "trading_ccy": trading_ccy}
        elif ccy in CCY_INDEX:
            temp_ccy_type = {"ccy": ccy, "bbg_code": CCY_INDEX[ccy], "back_up_bbg_code": CCY_INDEX[ccy], "trading_ccy": ccy}
        ccy2ticker_list.append(temp_ccy_type.copy())
    df = pd.DataFrame(ccy2ticker_list)
    df.index = df["ccy"]
    del df["ccy"]
    return df


def get_symbol_type(symbol_dict, fx_data_source, fx_backup_data_source, symbol_info,local_data_symbols):
    """ get instrument type from dm or self declare csv symbol info
    
        returns a DataFrame, with columns=['instrument_type', 'base_ccy', 'bbg_code', 'lot_size', 'back_up_bbg_code', 'source']
    """
    batch_number = GET_SYMBOL_TYPE_BATCH
    res_dict = {}
    ticker_map = fx_ticker_dm_map.copy()
    symbol_info_symbols = symbol_info.index
    for sym_type, symbols in symbol_dict.items():
        dm_hv_symbol = list(set(symbols) - set(symbol_info_symbols))
        if len(dm_hv_symbol) != 0:
            temp_symbol2dm_ticker = dict(map(lambda x: (x, ticker_map.get(x, x)), dm_hv_symbol))
            temp_symbol2dm_ticker_key = list(temp_symbol2dm_ticker.keys())
            number_of_batch = int(len(temp_symbol2dm_ticker)/50)
            for i in range(number_of_batch+1):
                temp_batch_symbol2dm_ticker_key = list( temp_symbol2dm_ticker_key [ i*batch_number : min( (i+1)*batch_number,len(temp_symbol2dm_ticker_key) )] )
                if len(temp_batch_symbol2dm_ticker_key)==0:
                    continue
                res = dm_client.info(symbols = list(map(lambda x: temp_symbol2dm_ticker[x],temp_batch_symbol2dm_ticker_key)),
                                            fields="instrument_subtype.instrument_subtype_name,trading_currency,bloomberg_code,list_exchange_id")
                #res = dm_client.get("/api/v1/instruments/",symbols = list(map(lambda x: temp_symbol2dm_ticker[x],temp_batch_symbol2dm_ticker_key)) ,
                #                    fields="subtype,trading_currency,bloomberg_code,list_exchange")

                res_column = ['instrument_type', 'base_ccy', "bbg_code","exchange_id"]
                temp_res_dict = dict(map(lambda x: (x, dict(zip(res_column, res["values"][temp_symbol2dm_ticker[x]]))), temp_batch_symbol2dm_ticker_key ))
                for symbol, value in temp_res_dict.items():
                    value['instrument_type'] = instrument_type_map.get(value['instrument_type'], value['instrument_type'])
                    value['lot_size'] = None if value['instrument_type'] != "FX" else None
                    value["back_up_bbg_code"] = None
                    value["source"] = "data_master"
                    if value['instrument_type'] == "FX":
                        bbg_code_list = value['bbg_code'].split(" ")
                        value["bbg_code"] = f"{bbg_code_list[0]} {fx_data_source} Curncy"
                        value["back_up_bbg_code"] = f"{bbg_code_list[0]} {fx_backup_data_source} Curncy" if fx_backup_data_source is not None else None
                        value["exchange_id"] = 0
                    if symbol in local_data_symbols:
                        value["source"] = "self"
                        value["bbg_code"] = None
                        value["back_up_bbg_code"] = None
                        value["exchange_id"] = 0
                    #value["exchange_id"] = int(value["exchange_id"])
                res_dict.update(temp_res_dict)

        other_symbol = list(set(symbols) - set(dm_hv_symbol))
        if len(other_symbol) != 0:
            temp_res_dict = dict(map(lambda x: (x, dict(symbol_info.loc[x])), other_symbol))
            no_data_ticker = list(set(symbol_info.index[(pd.isnull(symbol_info["bbg_code"]) & (symbol_info["source"] != "self"))]) & set(other_symbol))
            if len(no_data_ticker) != 0:
                raise Exception(f'some tickers:{list(no_data_ticker)} in symbol_info but not have data in local folder and bbg ticker for dm')
            res_dict.update(temp_res_dict)

    df = pd.DataFrame(res_dict.values(), index=res_dict.keys())
    if "exchange_id" not in df:
        df["exchange_id"] = 0
    df["exchange_id"] = df["exchange_id"].fillna(0)
    df["exchange_id"] = np.array(df["exchange_id"],dtype=np.int32)
    return df


def get_symbol2ccy_matrix(symbols, symbol2ccy_dict):
    """ 
        returns a DataFrame, as matrix with columns = symbols and index = ccys 
    """
    all_ccy = sorted(set(symbol2ccy_dict.values()))
    symbol2ccy_matrix = pd.DataFrame(np.zeros((len(all_ccy), len(symbols))), index=all_ccy, columns=symbols)
    for sym in symbols:
        ccy = symbol2ccy_dict[sym]
        symbol2ccy_matrix.loc[ccy][sym] = 1
    return symbol2ccy_matrix


@nb.jit(cache=True, nopython=True)
def get_time(time):
    intraday_time = time[0] * 3600 + time[1] * 60 + time[2]
    return intraday_time


@nb.jit(cache=True, nopython=True)
def get_day(day):
    day_no = day[0] * 31 * 12 + day[1] * 31 + day[2]
    return day_no


@nb.jit(cache=True, nopython=True)
def get_var(values):
    values = np.sort(values)
    n = len(values)
    i = int(n * 0.05) + 1
    return values[i]


def get_today_seconds(hour, minute, second):
    return hour * 3600 + minute * 60 + second


def array_to_raw_array(array: np.ndarray):
    """
        从numpy array转为multiprocessing.RawArray，自动判断numpy array的dtype
    """
    TYPE_STR_TO_CTYPE = {
        'float32': c_float,
        'float64': c_double,
        'double': c_double,
        'int64': c_int64,
        'uint64': c_uint64,
        'int32': c_int32,
        'uint32': c_uint32
    }
    return multiprocessing.RawArray(TYPE_STR_TO_CTYPE[array.dtype.name], array.ravel())


def raw_array_to_array(raw_array, shape: tuple):
    """
        从multiprocessing.RawArray转为numpy array
    """
    c_type = raw_array._type_

    if c_type is c_float:
        dtype = np.float32
    elif c_type is c_double:
        dtype = np.float64
    elif c_type is c_int64:
        dtype = np.int64
    elif c_type is c_uint64:
        dtype = np.uint64
    elif c_type is c_int32:
        dtype = np.int32
    elif c_type is c_uint32:
        dtype = np.uint32
    else:
        raise ValueError(str(raw_array._type_))
    return np.frombuffer(raw_array, dtype).reshape(shape)
