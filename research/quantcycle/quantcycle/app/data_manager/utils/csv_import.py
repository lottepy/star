'''
    To handle symbols mapping according to local info.csv file or info dataframe
'''

import os
import pandas as pd
import numpy as np
from ast import literal_eval

CSV_FIELDS = ['symbol','csv_name','source','symboltype','trading_currency','lot_size','bloomberg_code','back_up_bbg_code','timezone','MAIN','CCPFXRATE','INT']

def get_sym_from_dir(local_data_dir: str) -> list:
    file_list = os.listdir(local_data_dir)
    return [os.path.splitext(sym)[0] for sym in file_list]

def get_csv_info_df(info_path: str) -> pd.DataFrame:
    if len(info_path) == 0:
        return pd.DataFrame(columns=CSV_FIELDS)
    if type(info_path) == str:
        assert os.path.exists(info_path), f"{info_path} path not exist"
        info_df = pd.read_csv(info_path)
    elif type(info_path) == pd.DataFrame:
        info_df = info_path
    else:
        assert False, f'Please make sure info_path {info_path} data type is either "str" or "pd.Dataframe"'
    info_df['symbol'] = info_df['symbol'].astype(str)
    if 'csv_name' in info_df.columns:
        info_df['csv_name'] = np.where(pd.isnull(info_df['csv_name']),info_df['csv_name'],info_df['csv_name'].astype(str))
    if 'trade_time' in info_df.columns:
        info_df['trade_time'] = info_df['trade_time'].apply(literal_eval)
    return info_df

def from_sym_to_other_sym_read_csv(csv_sym_list: list, info_df, from_sym: str, target_sym: str) -> dict:
    df = info_df.dropna(subset=[from_sym])
    df.index = df[from_sym]
    temp_dict = df.to_dict('index')
    return {csv_sym:temp_dict[csv_sym][target_sym] for csv_sym in csv_sym_list if str(temp_dict.get(csv_sym, {}).get(target_sym,'nan')) != 'nan'}

def get_dc_list(symbol_list: list, info_dict: dict, target_sym: str) -> list:
    '''
    symbol not exist in info: default datamaster
    symbol exist but nan info: default localcsv
    '''
    return [info_dict.get(sym, {target_sym:'datamaster'})[target_sym] if str(info_dict.get(sym, {target_sym:'datamaster'}).get(target_sym,'nan')) != 'nan' else 'localcsv' for sym in symbol_list]

def get_sym_from_csv_name(dir_path: str, info_path: str):
    assert dir_path, f' not exist in DataCenterArgs'
    csv_sym_list = get_sym_from_dir(dir_path)
    info_df = get_csv_info_df(info_path)
    csvsym2sym_map = from_sym_to_other_sym_read_csv(csv_sym_list, info_df, 'csv_name', 'symbol')
    symbol = [csvsym2sym_map[sym] if csvsym2sym_map.get(sym,'') else sym for sym in csv_sym_list]
    return symbol

def backup_mnemonic_from_info_df(symbol: list, symbol_mnemonic: list, info_df = pd.DataFrame()) -> list:
    if info_df.empty: return symbol_mnemonic
    mnemonic_sym_with_source = {symbol[i]: symbol_mnemonic[i] for i in range(len(symbol))}
    source = from_sym_to_other_sym_read_csv(symbol, info_df, 'symbol','source')
    source = {k:k+" "+v for k, v in source.items()}
    mnemonic_sym_with_source.update(source)
    # mnemonic overwritten by the csv file name
    symbol_mnemonic = list(mnemonic_sym_with_source.values())
    return symbol_mnemonic

def info_df_to_sym_dict(info_df):
    df = info_df.dropna(subset=['symbol'])
    df.index = df.symbol
    info_dict = df.to_dict('index') 
    return info_dict