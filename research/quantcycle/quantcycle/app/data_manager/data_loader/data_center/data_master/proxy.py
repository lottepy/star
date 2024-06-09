import os
from collections import defaultdict
import pandas as pd
import numpy as np


from datamaster import dm_client

from quantcycle.utils.production_constant import Frequency, Symbol

from ....utils.update_nested_dict import update_dict
from ....utils.csv_import import get_csv_info_df, from_sym_to_other_sym_read_csv
from ....utils.timestamp_manipulation import string2timestamp
from ....utils.info_data_helper import parse_tradable_info
from ..mapping import (INSTRUMENT_TYPE_MAPPING, MNEMONIC2SYMBOL,
                       TRADING_CURRENCY_MAPPING, STOCK_CCY_MAPPING, 
                       ETF_CCY_MAPPING, EXCHANGE_CALENDAR_MAPPING, EXCHANGE_TRADE_TIME_MAPPING)
from ..data.dm_constants import DATA_MASTER_LOAD_BATCH
from ..proxy_base import ProxyBase


class DataMaster(ProxyBase):
    def __init__(self):
        super().__init__()
        dm_client.refresh_config()
        dm_client.start()

    def on_data_bundle(self, data_bundle):
        '''
            download one data bundle
        '''
        self.logger.info(f'DataMaster Proxy: On data bundle: {data_bundle}')
        dc_args = data_bundle.get('DataCenterArgs',{})
        is_load_datamaster_pickle = dc_args.get('is_load_datamaster', False)
        if is_load_datamaster_pickle: 
            return {}
        try:
            symbols = _parse_symbols(data_bundle)
            fields_dict = _parse_fields(data_bundle)
            data = {}
            _suffix = data_bundle.get("Slot", 'data')
            flag_find_local_data = data_bundle.get(
                "SymbolArgs", {}).get("LOCAL_DATA", False)
            dm_client_get_interface = data_bundle.get(
                "SymbolArgs", {}).get("dm_client_get", '')
            LOCAL_DATA_DIR = data_bundle.get("SymbolArgs", {}).get(
                "LOCAL_DATA_DIR", os.environ.get("QC_DM_LOCAL_DATA_DIR", ""))
            # info
            fields_for_info = fields_dict.get('info', [])
            res = _get_info_data(self.logger,
                                symbol_tuples=symbols,
                                fields=fields_for_info)
            res = _update_bundle_info(res, data_bundle)
            info_res = _replace_dm_symbol_by_mnemonic(symbols, res, suffix=_suffix)
            update_dict(data, info_res)
            update_dict(ProxyBase.dm_info_summary, info_res)
            
            # ================================================
            # TODO: tempprary unknown will be treated as historical
            fields_for_historical = fields_dict.get('historical', [])
            fields_for_tradable = fields_dict.get('tradable_table', [])
            fields_for_unknown = fields_dict.get('unknown', [])
            fields = fields_for_historical+fields_for_unknown
            freq = data_bundle.get("Frequency", "DAILY")
            start_date = data_bundle.get("StartTS", 1577836800)
            end_date = data_bundle.get("EndTS", 1580428800)
            kwargs = _get_historical_arguments(data_bundle)
            
            if fields_for_tradable:
                self.logger.info(f'DataMaster Proxy: Begin to download tradable table for {len(symbols)} symbols.')        
                res = _get_tradable_table(
                                    logger=self.logger,
                                    symbols=symbols,
                                    fields=fields_for_tradable,
                                    start_date=start_date,
                                    end_date=end_date,
                                    exchange_info=ProxyBase.dm_info_summary)
                self.logger.info(f'DataMaster Proxy: Done downloading tradable table for {len(symbols)} symbols.')        
                res = _replace_dm_symbol_by_mnemonic(
                                    symbols, res, suffix=_suffix)
                update_dict(data, res)
                update_dict(ProxyBase.dm_info_summary, info_res)
            else:   
                self.logger.info(f'DataMaster Proxy: Begin to download {len(symbols)} symbols.')        
                if freq == "DAILY":
                    res = _get_daily_data(self.logger,
                                        symbols_tuples=symbols,
                                        fields=fields,
                                        start_date=start_date,
                                        end_date=end_date,
                                        fill_method='ffill',
                                        flag_find_local_data=flag_find_local_data,
                                        local_data_dir=LOCAL_DATA_DIR,
                                        dm_client_get_interface=dm_client_get_interface,
                                        exchange_info=ProxyBase.dm_info_summary,
                                        **kwargs)
                    res = _replace_dm_symbol_by_mnemonic(
                        symbols, res, suffix=_suffix)
                    update_dict(data, res)
                elif freq == "MINUTE":
                    res = _get_minute_data(
                        symbols=[symbol[Symbol.DM_SYMBOL.value]
                                for symbol in symbols],
                        fields=fields,
                        start_date=start_date,
                        end_date=end_date,
                        fill_method='ffill',
                        threads=2,
                        flag_find_local_data=flag_find_local_data,
                        local_data_dir=LOCAL_DATA_DIR,
                        **kwargs
                    )
                    res = _replace_dm_symbol_by_mnemonic(
                        symbols, res, suffix=_suffix)
                    update_dict(data, res)
                else:
                    raise ValueError(f"Can't handle Frequency:{freq}")
            # ================================================
            symbol_dict = {symbol[Symbol.MNEMONIC.value] +
                           "/symbol": symbol for symbol in symbols}
            update_dict(data, symbol_dict)
            return data
        except Exception as e:
            self.logger.error(f'{e}')
            raise e

def _parse_symbols(data_bundle):
    '''
        map symbol mnemonic to dm symbols
    '''
    symbol = data_bundle.get("Symbol")
    symbol_args = data_bundle.get("SymbolArgs", {})
    data_source = symbol_args.get("DataSource", "")
    dc_args = data_bundle.get("DataCenterArgs",{})
    local_sym_info_dir = dc_args.get("info","")

    symbol_mnemonic = [
        i+" "+data_source if data_source else i for i in symbol]
    dm_symbol = [MNEMONIC2SYMBOL.get(i, i) for i in symbol_mnemonic]

    if len(local_sym_info_dir) != 0:
        info_df = get_csv_info_df(local_sym_info_dir)
        # dm_sym = {symbol[i]: dm_symbol[i] for i in range(len(symbol))}
        mnemonic_sym_with_source = {symbol[i]: symbol_mnemonic[i] for i in range(len(symbol))}
        dm_code_dict = from_sym_to_other_sym_read_csv(symbol, info_df, 'symbol','bloomberg_code')
        source = from_sym_to_other_sym_read_csv(symbol, info_df, 'symbol','source')
        for i, sym in enumerate(symbol):
            dm_code = dm_code_dict.get(sym, '')
            if not dm_code:
                dm_code = source.get(sym, '')
            if not dm_code:
                dm_code = dm_symbol[i]
            dm_symbol.append(dm_code)
        source = {k:k+" "+v for k, v in source.items()}
        mnemonic_sym_with_source.update(source)
        # dm_symbol = list(dm_sym.values())
        # mnemonic overwritten by the csv file name
        symbol_mnemonic = list(mnemonic_sym_with_source.values())
        dm_symbol = [MNEMONIC2SYMBOL.get(i, i) for i in dm_symbol]

    freq = data_bundle.get("Frequency", "DAILY")
    if freq not in ["DAILY", "HOURLY", "MINUTE"]:
        freq = "DAILY"
    freq = Frequency[freq].value
    freqs = [freq] * len(dm_symbol)
    output = list(zip(symbol_mnemonic, symbol, dm_symbol, freqs))
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
        # parse info related fields
        if fld == 'INFO':
            rslt["info"].append("subtype")
            rslt["info"].append("bloomberg_code")
            rslt["info"].append("list_exchange")
            rslt["info"].append("trading_currency")
            # 'trading_currency' info in get_info_data
        # parse historical related fields
        elif fld == 'OHLC':
            rslt["historical"].append("open")
            rslt["historical"].append("high")
            rslt["historical"].append("low")
            rslt["historical"].append("close")
            # rslt["historical"].append("volume")
            # rslt["historical"].append("amount")
            # rslt["historical"].append("bbg_daily_total_return")
            # rslt["historical"].append("choice_fq_factor")

            # rslt["historical"].append("ms_cash_amount")
            # rslt["historical"].append("choice_cash_dividend_fin2")
            # rslt["historical"].append("choice_transformation")
        # other fields will be put into unknown api
        elif fld == 'a_shares_dividend':
            # rslt["historical"].append("ms_cash_amount")
            rslt["historical"].append("choice_cash_dividend_fin2")
            rslt["historical"].append("choice_transformation")

        elif fld == 'tradable_table':
            rslt['tradable_table'].append('tradable_table')
        else:
            rslt["unknown"].append(fld)
    return rslt


def _get_historical_arguments(data_bundle):
    '''
        get possible kwargs from data_bundle
    '''
    symbol_args = data_bundle.get("SymbolArgs", {})
    output = {}
    if 'DMAdjustType' in symbol_args.keys():
        output['adjust_type'] = symbol_args['DMAdjustType']
    if 'DMCalendar' in symbol_args.keys():
        output['calendar'] = symbol_args['DMCalendar']
    return output


def _replace_dm_symbol_by_mnemonic(symbol_tuples, res, suffix=""):
    '''
        replace the dm result's key by symbol mnemonic
    '''
    if suffix:
        _suffix = "/"+suffix
    else:
        _suffix = ""
    dmid2mnemonic = dict(zip([symbol[Symbol.DM_SYMBOL.value] for symbol in symbol_tuples],
                             [symbol[Symbol.MNEMONIC.value] for symbol in symbol_tuples]))
    tmp = {dmid2mnemonic[key]+_suffix: value for key, value in res.items()}
    return tmp

def _update_bundle_info(info_dict: dict, data_bundle: dict) -> dict:
    # for one in info_dict.values():
    #     one["data_frequency"] = data_bundle['Frequency']
    return info_dict

def _get_info_data(logger, symbol_tuples, fields):
    '''
        depends on dm_client.get_info_data()
        #TODO: pass all fields to dm will result in error,
        dirty work is done here by request each field
    '''
    batch = DATA_MASTER_LOAD_BATCH
    symbols = [symbol[Symbol.DM_SYMBOL.value] for symbol in symbol_tuples]
    symbol_map = {symbol[Symbol.DM_SYMBOL.value]: symbol[Symbol.MNEMONIC.value] for symbol in symbol_tuples}

    fields = set(fields)
    if len(fields) == 0:
        return {}

    # ============ OLD code for dm_client error while getting trading_currency
    # asset_type = _get_symbol_asset_type(symbols)

    # # additional fields required for some asset classes
    # if asset_type == 'FX' or asset_type == 'FUTURES':
    #     fields.add('trading_currency')
    # elif asset_type == 'STOCK' or asset_type == 'ETF':
    #     pass
    # ============ OLD code for dm_client error while getting trading_currency

    number_of_batch = len(symbols)//batch + int(len(symbols)%batch != 0)
    try:
        res = [elem for i in range(number_of_batch) for elem in 
                dm_client.get('api/v1/instruments', 
                            symbols=symbols[i*batch:min((i+1)*batch, len(symbols))], 
                            fields=fields)]
    except Exception as e:
        fields.discard('trading_currency')
        res = [elem for i in range(number_of_batch) for elem in 
                dm_client.get('api/v1/instruments', 
                            symbols=symbols[i*batch:min((i+1)*batch, len(symbols))], 
                            fields=fields)]

    # type mapping
    null_ex_sym = []
    null_ex_time_sym = []
    for one in res:
        one["symboltype"] = INSTRUMENT_TYPE_MAPPING.get(
            one["subtype"], None).upper()
        assert one["symboltype"] != None, f'DM: {one["subtype"]} subtype cannot be identified in INSTRUMENT_TYPE_MAPPING'
        ####### trading currency #######
        trading_curncy = one.get("trading_currency", None)
        if trading_curncy is None:
            if one["symboltype"] == 'FX' or one["symboltype"] == 'MSFXINDEX':
                mnemonic = symbol_map[one["symbol"]]
                short_symbol = mnemonic.split(' ')[0]
                trading_curncy = TRADING_CURRENCY_MAPPING.get(short_symbol, None)
                assert trading_curncy != None, f'DM: {short_symbol} short_symbol cannot be identified in TRADING_CURRENCY_MAPPING'        
            # elif asset_type == 'STOCK_ETF':
            elif one['symboltype'] == 'CN_STOCK' or one['symboltype'] == 'HK_STOCK' or one['symboltype'] == 'US_STOCK':
                trading_curncy = STOCK_CCY_MAPPING.get(one["symboltype"], None)
                assert trading_curncy != None, f'DM: {one["symboltype"]} symboltype cannot be identified in STOCK_CCY_MAPPING'
            elif one['symboltype'] == 'ETF':
                if one["list_exchange"] == None:
                    continue
                trading_curncy = ETF_CCY_MAPPING.get(one["list_exchange"], None)
                if trading_curncy == 'HKD': one["symboltype"] = 'HK_ETF'
                if trading_curncy == 'USD': one["symboltype"] = 'US_ETF'
                if trading_curncy == 'CNY': one["symboltype"] = 'CN_ETF'
                assert trading_curncy != None, f'DM: {one["symbol"]} {one["list_exchange"]} list_exchange cannot be identified in ETF_CCY_MAPPING'
            elif one['symboltype'] == 'CONTINUOUSFUTURES':
                # mnemonic = symbol_map[one["symbol"]]
                # short_symbol = mnemonic.split(' ')[0]
                # trading_curncy = ETF_CCY_MAPPING.get(one["list_exchange"], None)          
                assert trading_curncy != None, f'DM: {one["symbol"]} trading_curncy cannot be identified in DataMaster'        
            one["trading_currency"] = trading_curncy
        ####### trading currency #######

        ####### update symboltype ######        
        if one['symboltype'] == 'CONTINUOUSFUTURES':
            if trading_curncy == 'HKD': one["symboltype"] = 'HK_CONTINUOUSFUTURES'
            if trading_curncy == 'USD': one["symboltype"] = 'US_CONTINUOUSFUTURES'
            if trading_curncy == 'CNY': one["symboltype"] = 'CN_CONTINUOUSFUTURES'
        elif one['symboltype'] == 'ETF':
            if trading_curncy == 'HKD': one["symboltype"] = 'HK_ETF'
            if trading_curncy == 'USD': one["symboltype"] = 'US_ETF'
            if trading_curncy == 'CNY': one["symboltype"] = 'CN_ETF'
        ####### update symboltype ######

    rslt = {i["symbol"]: i for i in res}
    null_ex_sym, null_ex_time_sym = parse_tradable_info(symbols, rslt, EXCHANGE_CALENDAR_MAPPING, EXCHANGE_TRADE_TIME_MAPPING)
    ####### exchange info ######
    if null_ex_sym:
        logger.warning(f'WARNING: These [{null_ex_sym}] have NULL list_exchange name, (ALL_TRADABLE, "00:00,UTC") is assumed for all mentioned symbols.')
    if null_ex_time_sym:
        logger.warning(f'WARNING: These [{null_ex_time_sym}] have NULL trade_time, time_zone info, (ALL_TRADABLE, "00:00,UTC") is assumed for all mentioned symbols.')
    # empty_info_sym = set([sym[Symbol.DM_SYMBOL.value]for sym in symbol_tuples]).difference(set(rslt.keys()))
    # rslt.update({sym:{} for sym in empty_info_sym})
    return rslt

def _get_symbol_asset_type(symbols):
    info_subtype = dm_client.get('api/v1/instruments',
                                 symbols=symbols[0],
                                 fields='subtype')

    # assumption: one data_group constains symbols of same asset type. i.e. stock or FX
    if info_subtype:
        subtype = INSTRUMENT_TYPE_MAPPING.get(info_subtype[0]["subtype"], None)
    else:
        subtype = 'unknown'
    # assert subtype != None, f'{symbols[0]} subtype cannot be identified'
    # MSFXCNY and MSFXHKD subtype
    asset_type = 'unknown'
    if subtype == 'FX' or subtype == 'MSFXINDEX': asset_type = 'FX'
    if subtype == 'CN_STOCK' or subtype == 'HK_STOCK' or subtype == 'US_STOCK' or subtype == 'ETF': asset_type = 'STOCK_ETF'
    if subtype == 'CONTINUOUSFUTURES': asset_type = 'FUTURES'
    # assert asset_type != '', f'{symbols[0]} asset_type cannot be identified'

    return asset_type

def _get_tradable_table(logger, symbols, fields, start_date, end_date, exchange_info):
    '''
        Cautions: HARDCODE mapping
        "NYSE ARCA":"NYSE",
        "SEHK":"HKEX",
        "NASDAQ GM": "NASDAQ",
        "BATS": "NASDAQ"
        "CHINEXT": "SZSE"
    '''
    
    start_date_daily = pd.to_datetime(start_date, unit='s').strftime("%Y-%m-%d")
    end_date_daily = pd.to_datetime(end_date + 3600*24, unit='s').strftime("%Y-%m-%d")
        # exchange_map = {sym[Symbol.DM_SYMBOL.value]: exchange_info[f'{sym[Symbol.MNEMONIC.value]}/info']['list_exchange'] if exchange_info[f'{sym[Symbol.MNEMONIC.value]}/info'].get('list_exchange',{'list_exchange':'ALL_TRADABLE'}) else 'ALL_TRADABLE' for sym in symbols}
    exchange_map = {}
    for sym in symbols:
            exchange_map[sym[Symbol.DM_SYMBOL.value]] = exchange_info[f'{sym[Symbol.MNEMONIC.value]}/info']['list_exchange']
    all_exchange = set(exchange_map.values())
    try:
        exchange_calendar_map = {ex: sorted(dm_client.get(
                            "/exchange_calendar/", 
                            ex= EXCHANGE_CALENDAR_MAPPING.get(ex, ex),
                            start_date = start_date_daily,
                            end_date = end_date_daily)) if ex != 'ALL_TRADABLE' else ex
                            for ex in all_exchange }
    except Exception as e:
        logger.error(f'_get_tradable_table: Requested exchange list: {all_exchange}')
        raise e

    res = {sym: {'exchange_calendar':exchange_calendar_map[ex]} if ex!="ALL_TRADABLE" else {'list_exchange':"ALL_TRADABLE",'exchange_calendar':exchange_calendar_map[ex]} for sym, ex in exchange_map.items()}
    return res

def _get_daily_data(logger, symbols_tuples, fields, start_date, end_date, fill_method, flag_find_local_data, local_data_dir, dm_client_get_interface='',exchange_info={},**kwargs):
    '''
        depends on dm_client.get_historical_data()
    '''
    if len(fields) == 0:
        return {}
    batch = DATA_MASTER_LOAD_BATCH

    symbols = [symbol[Symbol.DM_SYMBOL.value] for symbol in symbols_tuples]
    start_date_daily = pd.to_datetime(start_date, unit='s').strftime("%Y-%m-%d")
    end_date_daily = pd.to_datetime(end_date, unit='s').strftime("%Y-%m-%d")
    number_of_batch = len(symbols)//batch + int(len(symbols)%batch != 0)
    
    #### Hardcode close time is FX close time 
    time_info = {}
    for sym in symbols_tuples:
        key = sym[Symbol.MNEMONIC.value]+'/info'
        if key not in exchange_info:
            time_info[sym[Symbol.DM_SYMBOL.value]] = (['00:00','00:00'],'UTC')
            continue
        time_info[sym[Symbol.DM_SYMBOL.value]] = (exchange_info[key]["trade_time"],exchange_info[key]["timezone"])

    empty_df = pd.DataFrame(columns=fields)
    if not dm_client_get_interface:
        res = {}
        for i in range(number_of_batch):
            historical_data = dm_client.historical(
                    symbols=symbols[i*batch:min((i+1)*batch, len(symbols))],
                    fields=fields,
                    start_date=start_date_daily,
                    end_date=end_date_daily,
                    fill_method=1,
                    **kwargs
                )
            for key, value in historical_data['values'].items():
                if historical_data['fields'] is None:
                    historical_data['fields'] = ["date"] + fields
                data_frame = pd.DataFrame(value, columns=historical_data['fields'])  
                res[key] = _format_daily_result(data_frame.ffill(),timezone=time_info[key][1],close_time=time_info[key][0][-1].split('-')[-1])
            
    else:
        # TODO: temporary for getting individual_contract one by one
        res = {sym: _format_daily_result(pd.DataFrame(
                dm_client.get(f'api/v1/{dm_client_get_interface}/{sym}',
                    # symbol=,
                    start_date=start_date_daily,
                    end_date=end_date_daily,
                    # **kwargs
                ), columns=['date', 'individual_contract', 'next_close']),
                timezone=time_info[sym][1],
                close_time=time_info[sym][0][-1].split('-')[-1]
                )
            for i in range(number_of_batch) for sym in symbols[i*batch:min((i+1)*batch, len(symbols))]}

    if len(res.keys()) != len(symbols):
        res.update({sym:empty_df for sym in symbols if sym not in res.keys()})
    #lst_empty_symbol =[key for key, value in res.items() if value.empty]

    if flag_find_local_data:
        logger.info(f'DataMaster Proxy: Start batching with local csv files...')
        local_res = _find_local_data(
            logger,
            symbols=symbols,
            fields=fields,
            start_date=start_date,
            end_date=end_date,
            local_data_dir=local_data_dir
        )

        res = {key: _join_daily_result(value, local_res.get(
            key, pd.DataFrame(columns=fields))) for key, value in res.items()}
        logger.info(f'DataMaster Proxy: Batching with local csv files ends.')
    return res


def _find_local_data(logger, symbols, fields, start_date, end_date, local_data_dir):
    file_list = os.listdir(local_data_dir)
    res = {}
    for sym in symbols:
        if f'{sym}.csv' in file_list:
            df = pd.read_csv(os.path.join(
                local_data_dir, f'{sym}.csv'), index_col=0)
            df = _format_timeindex(df)
            df = df.loc[(df.index >= start_date) & (df.index <= end_date)]
            columns = [col for col in fields if col in df.columns]
            df = df.loc[:, columns]
            res[sym] = df
            logger.info(f'{sym} csv batch found.')
        else:
            res[sym] = pd.DataFrame(columns=fields)
    return res

def _format_timeindex(df, timezone: str = 'UTC'):
    '''
        transform the original result from dm to desired format
        support timezone: 'HKT'
    '''

    df.index = string2timestamp(df.index, timezone)
    df.index.name = 'timestamp'
    return df

def _join_daily_result(df_A, df_B):
    '''
        usd df_B to complete df_A. df_A is from dm, and df_B is from local
    '''
    empty_B = pd.DataFrame(index=df_B.index)
    # add index existed in B not A
    tmp = pd.concat([df_A, empty_B], sort=True, axis=1)
    tmp.update(df_B, overwrite=True)
    return tmp


def _format_daily_result(df, timezone="UTC", close_time="18:00"):
    '''
        transform the original result from dm to desired format
    '''
    # df.set_index(df.columns[0], inplace=True)
    df.set_index(df.columns[np.where(df.columns.str.contains('date'))[0][0]], inplace=True)
    if df.empty:
        return df

    # df.index = pd.to_datetime(df.index, format='%Y-%m-%d').values.astype(np.int64) // 10 ** 9
    # df.index.name = 'timestamp'
    df.index = [f'{date} {close_time}' for date in df.index]
    df = _format_timeindex(df, timezone)
    return df

def _get_minute_data(symbols, fields, start_date, end_date, fill_method, flag_find_local_data, local_data_dir, **kwargs):
    '''
        TODO 
        depends on dm_client.get_minute_data_multi_thread()
    '''
    if len(fields) == 0:
        return {}
    res = dm_client.get_minute_data_multi_thread(
        symbols=symbols,
        fields=fields,
        start_date=start_date,
        end_date=end_date,
        fill_method=fill_method,
        to_dataframe=True,
        **kwargs
    )
    return res
