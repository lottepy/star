import os
from collections import defaultdict

import pandas as pd
import numpy as np

from quantcycle.utils.production_constant import Frequency, Symbol

from ....utils.update_nested_dict import update_dict
from ....utils.info_data_helper import parse_tradable_info
from ....utils.timestamp_manipulation import string2timestamp
from ....utils.csv_import import get_sym_from_dir, get_csv_info_df, from_sym_to_other_sym_read_csv, get_sym_from_csv_name, backup_mnemonic_from_info_df, info_df_to_sym_dict, CSV_FIELDS
from ..proxy_base import ProxyBase
from ..mapping import (EXCHANGE_CALENDAR_MAPPING, EXCHANGE_TRADE_TIME_MAPPING)


class LocalCSV(ProxyBase):
    def __init__(self):
        super().__init__()
        self.info_path = ''
        self.info_df = pd.DataFrame()

    def on_data_bundle(self, data_bundle):
        '''
            download one data bundle
        '''
        self.logger.info(f'LocalCSV On data bundle: {data_bundle}')
        dc_args = data_bundle.get("DataCenterArgs", {})
        dir_path = dc_args.get('dir','')
        info_df = self.update_info(dc_args.get('info',''))
        info_dict = info_df_to_sym_dict(info_df)
        try:
            symbol_tuples = _parse_symbols(data_bundle, info_df)
            fields_dict = _parse_fields(data_bundle)
            data = {}
            _suffix = data_bundle.get("Slot", 'data')

            ## hardcode must update info with info.csv
            res = {sym[Symbol.MNEMONIC.value]: {'symbol': sym[Symbol.MNEMONIC.value]} for sym in symbol_tuples}
            parse_tradable_info([sym[Symbol.MNEMONIC.value] for sym in symbol_tuples], res, EXCHANGE_CALENDAR_MAPPING, EXCHANGE_TRADE_TIME_MAPPING)
            csv_res = {sym[Symbol.MNEMONIC.value]:info_dict.get(sym[Symbol.SYMBOL.value],{}) for sym in symbol_tuples}
            csv_res = _upper_case_symboltype(csv_res)
            csv_res = _remove_key_nan(csv_res)
            csv_res = _format_info_values(csv_res)
            update_dict(res, csv_res)
            info = _replace_key_with_suffix(res,suffix='info')
            update_dict(data, info)
            update_dict(ProxyBase.dm_info_summary, info)

            # ================================================
            fields_for_historical = fields_dict.get('historical', [])
            fields_for_unknown = fields_dict.get('unknown', [])
            fields = fields_for_historical+fields_for_unknown
            freq = data_bundle.get("Frequency", "DAILY")
            start_date = data_bundle.get("StartTS", 1577836800)
            end_date = data_bundle.get("EndTS", 1580428800)
            # ================================================

            if fields != ['INFO']:
                if dc_args.get("key_name", "").split("/")[-1] == "CCPFXRATE" and dc_args.get("fx2sym", False):
                    csv_name_list = _symbol_df_from_fx(symbol_tuples, info)
                else:
                    csv_name_list = [info[f'{symbol[Symbol.MNEMONIC.value]}/info'].get('csv_name',symbol[Symbol.SYMBOL.value]) for symbol in symbol_tuples]
                sym_csv_tuples = list(zip(symbol_tuples, csv_name_list))
                timezone_list = [info[f'{symbol[Symbol.MNEMONIC.value]}/info'].get('timezone','UTC') for symbol in symbol_tuples]
                dict_df = _find_local_data(
                    symbols= csv_name_list,
                    fields=fields,
                    start_date=start_date,
                    end_date=end_date,
                    local_data_dir=dir_path,
                    timezone=timezone_list
                )
                dict_df = {sym[0][Symbol.MNEMONIC.value]+'/'+_suffix:dict_df[sym[1]] for sym in sym_csv_tuples}

                update_dict(data, dict_df)
            return data
        except Exception as e:
            self.logger.error(f'{e}')
            raise e

    def update_info(self, info_path: str):
        if len(info_path) == 0:
            return pd.DataFrame(columns=CSV_FIELDS)
        if len(self.info_path) != 0:
            if type(info_path) == str:
                if info_path == self.info_path:
                    return self.info_df
            else:
                if (info_path.fillna(0) == self.info_path.fillna(0)).all(axis=None):
                    return self.info_df
        if type(info_path) == str:
            assert os.path.exists(info_path), f"LocalCSV: {info_path} dir path not exist"
        self.info_path = info_path
        self.info_df = get_csv_info_df(info_path)
        return self.info_df 

def _parse_symbols(data_bundle, info_df):
    '''
        map symbol mnemonic to dm symbols
    '''

    symbol = data_bundle.get("Symbol", [])
    symbol_args = data_bundle.get("SymbolArgs", {})
    dc_args = data_bundle.get("DataCenterArgs", {})
    data_source = symbol_args.get("DataSource", "")
    dm_symbol = []
    if not symbol:
        symbol = get_sym_from_csv_name(dc_args.get('dir',''), dc_args.get('info',''))

    symbol_mnemonic = [
        i+" "+data_source if data_source else i for i in symbol]
    symbol_mnemonic = backup_mnemonic_from_info_df(symbol, symbol_mnemonic, info_df)

    output = list(zip(symbol_mnemonic, symbol))
    return output


def _parse_fields(data_bundle):
    '''
        map fields convention to dm recognized api and fields
    '''
    flds = data_bundle.get("Fields")
    if type(flds) == str:
        flds = [flds]
    rslt = defaultdict(list)
    for fld in flds:
        # parse historical related fields
        # if fld == 'INFO':
        #     rslt["info"].append("subtype")
        #     rslt["info"].append("bloomberg_code")
        #     rslt["info"].append("list_exchange")
        #     # 'trading_currency' info in get_info_data
        if fld == 'OHLC':
            rslt["historical"].append("open")
            rslt["historical"].append("high")
            rslt["historical"].append("low")
            rslt["historical"].append("close")
        # other fields will be put into unknown api
        else:
            rslt["unknown"].append(fld)
    return rslt

# TODO
# def _replace_csv_symbol_by_mnemonic(symbol_tuples, res, suffix=""):
#     '''
#         replace the csv result's key by symbol mnemonic
#     '''
#     if suffix:
#         _suffix = "/"+suffix
#     else:
#         _suffix = ""
#     dmid2mnemonic = dict(zip([symbol[Symbol.DM_SYMBOL.value] for symbol in symbol_tuples],
#                              [symbol[Symbol.MNEMONIC.value] for symbol in symbol_tuples]))
#     tmp = {dmid2mnemonic[key]+_suffix: value for key, value in res.items()}
#     return tmp

def _replace_key_with_suffix(res, suffix=""):
    '''
        add suffix for data_collection
    '''
    _suffix = "/"+suffix
    tmp = {k+_suffix:v for k,v in res.items()}
    return tmp

def _find_local_data(symbols, fields, start_date, end_date, local_data_dir, timezone):
    file_list = os.listdir(local_data_dir)
    res = {}
    for i in range(len(symbols)):
        if f'{symbols[i]}.csv' in file_list:
            df = pd.read_csv(os.path.join(
                local_data_dir, f'{symbols[i]}.csv'), index_col=0)
            df = _format_timeindex(df, timezone[i])
            df = df.loc[(df.index >= start_date) & (df.index <= end_date)]
            if fields:
                if len(fields) == len(df.columns):
                    df = df[fields]
                else:
                    # columns = [col for col in fields if col in df.columns]
                    df = pd.concat([df[col] if col in df.columns else pd.DataFrame(columns=[col]) for col in fields], axis=1)
                    df.columns = fields
            else:
                columns = df.columns
                df = df[columns]
            res[symbols[i]] = df
        else:
            res[symbols[i]] = pd.DataFrame(columns=fields)
    return res

def _format_timeindex(df, timezone: str = 'UTC'):
    '''
        transform the original result from dm to desired format
        support timezone: 'HKT'
    '''

    df.index = string2timestamp(df.index, timezone)
    df.index.name = 'timestamp'
    return df

def _symbol_df_from_fx(symbol_tuples: tuple, info: dict) -> list:
    res = []
    for tup in symbol_tuples:
        trading_ccy = info[tup[Symbol.MNEMONIC.value]].get('trading_currency', '')
        assert trading_ccy, f'LocalCSV: {tup[Symbol.SYMBOL.value]} trading_currency is not defined in info.csv.'
        #''.join([,trading_ccy])
        
    return []

def _remove_key_nan(res: dict) -> dict:
    '''    remove key with nan value    '''
    tmp = {}
    for k,v in res.items():
        tmp[k] = {key:val for key,val in v.items() if (type(val)==list or type(val)==np.ndarray or pd.notnull(val))}
    return tmp

def _format_info_values(res: dict) -> dict:
    '''    update some formats of info  e.g. list_exchange
    '''
    for k,v in res.items():
        pass
        # if 'list_exchange' in v and type(v['list_exchange'])==str:
        #     res[k]['list_exchange'] = v['list_exchange'].split(',')
    return res

def _upper_case_symboltype(csv_res):
    for values in csv_res.values():
        if type(values.get('symboltype',None)) == str:
            values['symboltype'] = values['symboltype'].upper()
    return csv_res
