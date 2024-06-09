'''
    generate data bundles according to data group and available local data.
'''
import numpy as np
import pandas as pd
import copy
from ..data_loader.data_center.mapping import CCY2USDCCP
from ..utils.get_symbol_info import get_sym_from_csv_name
from ..utils.csv_import import *
from quantcycle.app.data_manager.data_loader.data_center.data.dm_constants import LabelField

SUPPORTED_ACC_CCY = ['USD', 'HKD', 'CNY']


def route_data_request(data_group_list, symbol_info):
    '''
        From data group list to data bundle list
    '''
    bundles = []
    for data_group in data_group_list:
        _bundles = _generate_data_bundle(data_group, symbol_info)
        bundles.extend(_bundles)
    bundles = _redirect_data_bundle(bundles, symbol_info, local_first=True)
    return bundles


def _generate_data_bundle(data_group, symbol_info):
    '''
        Given a data group, generate the correlated data_bundles
    '''
    bundles = []
    # parse data group
    label = data_group.get("Label", "Default")
    symbols = data_group.get("Symbol")
    action_type = data_group.get("DMActions","")
    symbol_args = data_group.get("SymbolArgs", {})
    data_source = symbol_args.get("DataSource", "")
    adjust_type = symbol_args.get("DMAdjustType", 0)
    dm_calendar = symbol_args.get("DMCalendar", '')
    settlement_type = symbol_args.get("SettlementType", '')
    ### local csv backup datamaster data ###
    is_local_data_backup_datamaster = symbol_args.get("LOCAL_DATA")
    local_data_dir = symbol_args.get("LOCAL_DATA_DIR")
    dc_map_path = symbol_args.get("DatacenterMap", "")
    ########################################
    is_output_tradable_table = data_group.get("Fields",[]) == 'tradable_table' or data_group.get("Fields",[]) == ['tradable_table']
    acc_ccy = symbol_args.get("AccountCurrency", "")
    backup_data_source = symbol_args.get("BackupDataSource", "")
    new_data_source = data_source+"+" + \
        backup_data_source if backup_data_source else data_source
    start_ts = data_group.get("StartTS", 1577836800)
    end_ts = data_group.get("EndTS", 1580428800)
    freq = data_group.get("Frequency", "DAILY")
    data_center = data_group.get("DataCenter", "DataMaster")
    data_center_args = data_group.get("DataCenterArgs", {})
    hdf5_path = data_center_args.get("DataPath", "")
    pnl_type = data_center_args.get("pnl_type", None)
    fields = data_group.get("Fields", ["OHLC"])
    if settlement_type:
        if settlement_type.lower() == "open": fields = ["open", "close"]
        elif settlement_type.lower() == "close": fields = ["close"]
    fields = [fields] if type(fields) == str else fields
    other_options = data_group.get("Other",{})
    dm_download_dividend = symbol_args.get("DMGetDividend", False)
    dm_download_split = symbol_args.get("DMGetSplit", False)
    
    

    # datacenter mapping for LocalCSV
    dm_map={}
    info_df = pd.DataFrame()
    if len(dc_map_path) != 0:
        info_df = get_csv_info_df(dc_map_path)
        info_dict = info_df_to_sym_dict(info_df)
        main_dc_info = get_dc_list(symbols, info_dict, 'MAIN')
        fxrate_dc_info = get_dc_list(symbols, info_dict, 'CCPFXRATE')
        interest_dc_info = get_dc_list(symbols, info_dict, 'INT')
        split_dc_info = get_dc_list(symbols, info_dict, 'SPLIT')
        dc_info = np.array([main_dc_info, fxrate_dc_info, interest_dc_info, split_dc_info])
        dm_map = np.isin(dc_info, 'datamaster')

    # Strat load data
    # ====================================
    ###### Local CSV import data
    if data_center == 'LocalCSV':
        key_name = data_center_args.get('key_name', '')
        data_field = key_name.split('/')[-1]
        if not key_name:
            dir_path = data_center_args['dir']
            assert not ('\\' in dir_path), f'Data group {label} dir path please use "/"'
            folder_name = dir_path.split('/')[-1]
            key_name = label+f'-{folder_name}'
        
        if 'CCPFXRATE' == data_field:
            slot = 'fxrate'
        elif 'MAIN' == data_field:
            slot = 'data'
        elif 'INT' == data_field:
            slot = 'INT'
        elif 'SPLIT' == data_field:
            slot = 'SPLIT'
        else:
            slot = data_field

        ###### for backup dm data usage ########
        sym_using_csv = symbols
        if len(dc_map_path) != 0:
            sym_arr = np.array(symbols)
            sym_using_csv = list(sym_arr[dm_map[LabelField[data_field].value] == False])
        ########################################
        strat_load = {
            "Label": key_name,
            "Type": "Download",
            "DataCenter": data_center,
            "DataCenterArgs": data_center_args,
            "Symbol": sym_using_csv,
            "SymbolArgs": {"DataSource": new_data_source},
            "Slot": slot,
            "Fields": fields,
            "StartTS": start_ts,
            "EndTS": end_ts,
            "Frequency": freq
        }
        bundles.append(strat_load)
        if not symbols:
            symbols = get_sym_from_csv_name(data_center_args['dir'],data_center_args['info'])
        data_center_args.update({"ffill": False})
        if settlement_type and 'MAIN' == data_field:
            stack_data = {
                "Label": key_name,
                "Type": "Process",
                "DataCenter": data_center,
                "DataCenterArgs": data_center_args,
                "Symbol": symbols,
                "SymbolArgs": {
                    "DataSource": new_data_source,
                    "SettlementType": settlement_type
                },
                "Slot": "data",
                "Fields": fields,
                "Actions": "SETTLEMENT",
                "StartTS": start_ts,
                "EndTS": end_ts,
                "Frequency": freq
            }
            bundles.append(stack_data)
            fields = ["data", "field"]
        stack_data = {
            "Label": key_name,
            "Type": "Process",
            "DataCenter": data_center,
            "DataCenterArgs": data_center_args,
            "Symbol": symbols,
            "SymbolArgs": {
                "DataSource": new_data_source,
                "SettlementType": settlement_type
            },
            "Slot": slot,
            "Fields": fields,
            "Actions": "STACK",
            "StartTS": start_ts,
            "EndTS": end_ts,
            "Frequency": freq
        }
        bundles.append(stack_data)
        if len(fields)==1 and fields[0] == 'INFO':
            # TODO dirty work. INFO bundle must be the last bundle now.
            # To get INFO only if config 'fields' has "INFO" only
            bundles = []
        info_ccp = {
            "Label": key_name.split('/')[0]+"/INFO",
            "Type": "Process",
            "DataCenter": data_center,
            "DataCenterArgs": data_center_args,
            "Symbol": symbols,
            "SymbolArgs": {"DataSource": new_data_source},
            "Slot": '',
            "Fields": [""],
            "Actions": "INFO",
            "StartTS": start_ts,
            "EndTS": end_ts,
            "Frequency": freq
        }
        bundles.append(info_ccp)
    elif data_center == 'ResultReader':
        # reset the symbol names

        # symbols = [label+' Strat_'+ str(sym) for sym in symbols]

        symbols = np.array(data_group.get("Symbol"))

        # TODO hardcode get symbols everytime
        fields.append('symbols')
        if 'metrics' in fields:
            is_cal_metrics = True
            # Calculate metrics requires pnl, pos, ref_aum TODO: must include symbols?
            fields_without_metrics = set(fields + ['pnl','position','ref_aum'])
            fields_without_metrics.discard('metrics')
            fields_without_metrics = list(fields_without_metrics)
        else:
            fields_without_metrics = fields
            is_cal_metrics = False

        for field in fields_without_metrics:
            strat_load = {
                "Label": label+'-Strat/load_'+field,
                "Type": "Download",
                "DataCenter": data_center,
                "DataCenterArgs": {
                    "DataPath": hdf5_path,
                    "pnl_type": pnl_type,
                },
                "Symbol": symbols,
                "SymbolArgs": {"DataSource": data_source},
                "Slot": field,
                "Fields": field,
                "StartTS": start_ts,
                "EndTS": end_ts,
                "Frequency": freq
            }
            bundles.append(strat_load)

            if field in ['position', 'cost', 'pnl']:
                pass_3d_data = {
                    "Label": label+'-Strat/'+field,
                    "Type": "Process",
                    "DataCenter": data_center,
                    "Symbol": symbols,
                    "SymbolArgs": {"DataSource": data_source},
                    "Actions": "PASS3DNP",
                    "Slot": field,
                    "Fields": field,
                    "StartTS": start_ts,
                    "EndTS": end_ts,
                    "Frequency": freq
                }
                bundles.append(pass_3d_data)

        # calculate metrics
        if is_cal_metrics:
            # metrics
            dtype = 'np_numeric'
            allocation_freq = data_group.get("SymbolArgs").get("Metrics")["allocation_freq"]
            lookback_points_list = data_group.get("SymbolArgs").get("Metrics")["lookback_points_list"]
            addition = data_group.get("SymbolArgs").get("Metrics")["addition"]
            multiplier = data_group.get("SymbolArgs").get("Metrics")["multiplier"]
            for i in lookback_points_list:
                metrics_strat = {
                    "Label": f"{label}-Strat/metrics_{i}",
                    "Type": "Process",
                    "DataCenter": data_center,
                    "DataCenterArgs": {"DataPath": hdf5_path},
                    "Symbol": symbols,
                    "SymbolArgs": {"DataSource": data_source},
                    "Actions": "METRICS",
                    "ActionsArgs": {
                        "allocation_freq": allocation_freq, # for pd.date_range
                        "lookback_points_list": [i],
                        "addition": addition,
                        "multiplier": multiplier,
                        'OutputDataType': dtype
                    },
                    "Slot": 'metrics',
                    "Fields": 'metrics',
                    "StartTS": start_ts,
                    "EndTS": end_ts,
                    "Frequency": freq
                }
                bundles.append(metrics_strat)

        # stack data
        for field in fields_without_metrics:
            dtype = 'bypass_arr2raw'
            if field in ['position', 'cost', 'pnl']:
               # skip if field does not belong to attributes
               continue
            stack_data = {
                "Label": label+'-Strat/'+field,
                "Type": "Process",
                "DataCenter": data_center,
                "Symbol": [label],    # To get strategy data
                "Slot": field,
                "Fields": field,
                "Actions": "STACK",
                "ActionsArg": {'OutputDataType': dtype},
                "StartTS": start_ts,
                "EndTS": end_ts,
                "Frequency": freq
            }
            bundles.append(stack_data)
            
        stack_data = {
            "Label": label+'-Strat/id_map',
            "Type": "Process",
            "DataCenter": data_center,
            "Symbol": [label],    # To get strategy data
            "Slot": 'id_map',
            "Fields": 'id_map',
            "Actions": "STACK",
            "ActionsArg": {'OutputDataType': 'bypass_arr2raw'},
            "StartTS": start_ts,
            "EndTS": end_ts,
            "Frequency": freq
        }
        bundles.append(stack_data)

        stratidmapping = {
            "Label": label+'-Strat/ID_symbol_map',
            "Type": "Process",
            "DataCenter": data_center,
            "Symbol": symbols,
            "Slot": 'symbols',
            "Fields": fields_without_metrics,
            "Actions": "STRATIDMAP",
            "ActionsArg": {'OutputDataType': 'bypass_arr2raw'},
            "StartTS": start_ts,
            "EndTS": end_ts,
            "Frequency": freq
        }
        bundles.append(stratidmapping)
    elif data_center == "DataMaster":
        if not is_output_tradable_table:
            symbols = np.array(data_group.get("Symbol"))
            # classfy symbols by types
            symbols_with_source = [i+" "+data_source if data_source else i for i in symbols]
            symbols_with_source = np.array(backup_mnemonic_from_info_df(symbols, symbols_with_source, info_df))
            symbol_types = np.array([symbol_info[i+"/info"].get("symboltype", "").upper()
                                    for i in symbols_with_source])
            trading_currency_types = np.array([symbol_info[i+"/info"].get("trading_currency", "")
                                    for i in symbols_with_source])
            # filter symbols type
            ## FX ##
            fx_select_symbols = symbols[symbol_types == 'FX']
            ## STOCK ##
            stock_sym = (np.core.defchararray.find(symbol_types,"ETF") != -1) | (np.core.defchararray.find(symbol_types,"STOCK") != -1)
            stock_select_symbols = symbols[stock_sym]
            hk_stock_select_symbols = symbols[(symbol_types == 'HK_STOCK') | (symbol_types == 'HK_ETF')]
            us_stock_select_symbols = symbols[(symbol_types == 'US_STOCK') | (symbol_types == 'US_ETF')]
            cn_stock_select_symbols = symbols[(symbol_types == 'CN_STOCK') | (symbol_types == 'CN_ETF')]
            uk_stock_select_symbols = symbols[symbol_types == 'UK_STOCK']
            ## FUTURES ##
            futures_sym = (np.core.defchararray.find(symbol_types,"CONTINUOUSFUTURES") != -1) | (np.core.defchararray.find(symbol_types,"INDIVIDUALFUTURES") != -1)
            futures_select_symbols = symbols[futures_sym]
            hk_futures_select_symbols = symbols[(symbol_types=='HK_CONTINUOUSFUTURES')]
            us_futures_select_symbols = symbols[(symbol_types=='US_CONTINUOUSFUTURES')]
            cn_futures_select_symbols = symbols[(symbol_types=='CN_CONTINUOUSFUTURES')]
            continuous_futures_select_symbols = symbols[(symbol_types=='US_CONTINUOUSFUTURES') | (symbol_types=='CN_CONTINUOUSFUTURES') | (symbol_types=='HK_CONTINUOUSFUTURES')]
            individual_futures_select_symbols = symbols[symbol_types == 'INDIVIDUALFUTURES']

            if len(fx_select_symbols) != 0:
                # deal with type: FX
                # ====================================
                opt_noint = other_options.get("NoINT", False)
                # data source data must be downloaded

                fxmain_sym = fx_select_symbols
                if len(dc_map_path) != 0:
                    fxmain_sym = fx_select_symbols[dm_map[LabelField.MAIN.value]]
                ############# MAIN ##############
                if len(fxmain_sym)!=0:
                    download_bundle = {
                        "Label": label+'-FX/MAIN',
                        "Type": "Download",
                        "DataCenter": data_center,
                        "Symbol": fxmain_sym,
                        "SymbolArgs": {
                            "DataSource": data_source,
                            'LOCAL_DATA': is_local_data_backup_datamaster,
                            "LOCAL_DATA_DIR": local_data_dir,
                        },
                        "Slot": "data",
                        "Fields": fields,
                        "StartTS": start_ts,
                        "EndTS": end_ts,
                        "Frequency": freq
                    }
                    bundles.append(download_bundle)
                    if backup_data_source:
                        backup_bundle = {
                            "Label": label+'-FX/MAIN',
                            "Type": "Download",
                            "DataCenter": data_center,
                            "Symbol": fxmain_sym,
                            "SymbolArgs": {
                                "DataSource": backup_data_source,
                                'LOCAL_DATA': is_local_data_backup_datamaster,
                                "LOCAL_DATA_DIR": local_data_dir,
                            },
                            "Slot": "data",
                            "Fields": fields,
                            "StartTS": start_ts,
                            "EndTS": end_ts,
                            "Frequency": freq
                        }
                        bundles.append(backup_bundle)
                    # BACKUP Process
                    if backup_data_source:
                        backup_price = {
                            "Label": label+'-FX/MAIN',
                            "Type": "Process",
                            "DataCenter": data_center,
                            "Symbol": fxmain_sym,
                            "SymbolArgs": {"DataSource": data_source,
                                        "BackupDataSource": backup_data_source},
                            "Slot": "data",
                            "Actions": "BACKUP",
                            "Fields": fields,
                            "StartTS": start_ts,
                            "EndTS": end_ts,
                            "Frequency": freq
                        }
                        bundles.append(backup_price)
                    # pack price
                    if settlement_type:
                        settlement_data = {
                            "Label": label+'-FX/MAIN',
                            "Type": "Process",
                            "DataCenter": data_center,
                            "DataCenterArgs": data_center_args,
                            "Symbol": fxmain_sym,
                            "SymbolArgs": {
                                "DataSource": new_data_source,
                                "SettlementType": settlement_type
                            },
                            "Slot": "data",
                            "Fields": fields,
                            "Actions": "SETTLEMENT",
                            "StartTS": start_ts,
                            "EndTS": end_ts,
                            "Frequency": freq
                        }
                        bundles.append(settlement_data)
                        fields = ["data", "field"]
                    stack_price = {
                        "Label": label+'-FX/MAIN',
                        "Type": "Process",
                        "DataCenter": data_center,
                        "Symbol": fxmain_sym,
                        "SymbolArgs": {
                            "DataSource": new_data_source,
                            "SettlementType": settlement_type
                        },
                        "Slot": "data",
                        "Fields": fields,
                        "Actions": "STACK",
                        "StartTS": start_ts,
                        "EndTS": end_ts,
                        "Frequency": freq
                    }
                    bundles.append(stack_price)
                ############# MAIN ##############

                fxrate_sym = fx_select_symbols
                if len(dc_map_path) != 0:
                    fxrate_sym = fx_select_symbols[dm_map[LabelField.CCPFXRATE.value]]
                ############# FX RATE ##############
                if len(fxrate_sym)!=0:
                    unique_symbols = sorted(
                        list(set([i[:3] for i in fxrate_sym]+[i[-3:] for i in fxrate_sym])))
                    if 'USD' in unique_symbols:
                        unique_symbols.remove('USD')
                    ccp_symbols = [CCY2USDCCP[i] for i in unique_symbols]
                    fx_bundle = {
                        "Label": label+'-FX/MAIN',
                        "Type": "Download",
                        "DataCenter": data_center,
                        "Symbol": ccp_symbols,
                        "SymbolArgs": {
                            "DataSource": data_source,
                            'LOCAL_DATA': is_local_data_backup_datamaster,
                            "LOCAL_DATA_DIR": local_data_dir
                        },
                        "Slot": "fxrate",
                        "Fields": ['close'],
                        "StartTS": start_ts,
                        "EndTS": end_ts,
                        "Frequency": freq
                    }
                    bundles.append(fx_bundle)
                    # backup fx bundle
                    if backup_data_source:
                        backup_fx_bundle = {
                            "Label": label+'-FX/MAIN',
                            "Type": "Download",
                            "DataCenter": data_center,
                            "Symbol": ccp_symbols,
                            "SymbolArgs": {
                                "DataSource": backup_data_source,
                                'LOCAL_DATA': is_local_data_backup_datamaster,
                                "LOCAL_DATA_DIR": local_data_dir
                            },
                            "Slot": "fxrate",
                            "Fields": ['close'],
                            "StartTS": start_ts,
                            "EndTS": end_ts,
                            "Frequency": freq
                        }
                        bundles.append(backup_fx_bundle)

                    # BACKUP Process
                    if backup_data_source:
                        backup_fxrate = {
                            "Label": label+'-FX/MAIN',
                            "Type": "Process",
                            "DataCenter": data_center,
                            "Symbol": ccp_symbols,
                            "SymbolArgs": {"DataSource": data_source,
                                        "BackupDataSource": backup_data_source},
                            "Slot": "fxrate",
                            "Actions": "BACKUP",
                            "Fields": ['close'],
                            "StartTS": start_ts,
                            "EndTS": end_ts,
                            "Frequency": freq
                        }
                        bundles.append(backup_fxrate)
                    # ccy fxrate
                    ccy_fxrate = {
                        "Label": label+'-FX/CCPFXRATE',
                        "Type": "Process",
                        "DataCenter": data_center,
                        "Symbol": fxrate_sym,
                        "SymbolArgs": {
                            "DataSource": new_data_source,
                            "info_df": info_df
                            },
                        "Slot": 'fxrate',
                        "Fields": ["close"],
                        "Actions": "CCYFX",
                        "StartTS": start_ts,
                        "EndTS": end_ts,
                        "Frequency": freq
                    }
                    bundles.append(ccy_fxrate)
                ############# FX RATE ##############

                int_sym = fx_select_symbols
                if len(dc_map_path) != 0:
                    int_sym = fx_select_symbols[dm_map[LabelField.INT.value]]
                ############# INTEREST/DIVIDEND ##############
                if len(int_sym)!=0:
                    unique_symbols = sorted(
                        list(set([i[:3] for i in int_sym]+[i[-3:] for i in int_sym])))
                    if 'USD' in unique_symbols:
                        unique_symbols.remove('USD')
                    ccp_symbols = [CCY2USDCCP[i] for i in unique_symbols]
                    if not opt_noint:
                        usd_interest = {
                            "Label": label+'-FX/USDINT',
                            "Type": "Download",
                            "DataCenter": data_center,
                            "Symbol": ["USD"],
                            "SymbolArgs": {
                                "DataSource": "INT",
                                'LOCAL_DATA': is_local_data_backup_datamaster,
                                "LOCAL_DATA_DIR": local_data_dir,
                            },
                            "Slot": "data",
                            "Fields": ["close"],
                            "StartTS": start_ts,
                            "EndTS": end_ts,
                            "Frequency": freq
                        }
                        bundles.append(usd_interest)
                    # other ccy tn
                    if not opt_noint:
                        tn = {
                            "Label": label+'-FX/TN',
                            "Type": "Download",
                            "DataCenter": data_center,
                            # TODO not support HKD CNY INT calculation
                            "Symbol": unique_symbols,
                            "SymbolArgs": {
                                "DataSource": "TN",
                                'LOCAL_DATA': is_local_data_backup_datamaster,
                                "LOCAL_DATA_DIR": local_data_dir
                            },
                            "Slot": "data",
                            "Fields": ["close"],
                            "StartTS": start_ts,
                            "EndTS": end_ts,
                            "Frequency": freq
                        }
                        bundles.append(tn)
                    # download fx rate for interest rate calculation
                    if not opt_noint:
                        fx_int = {
                            "Label": label+'-FX/data',
                            "Type": "Download",
                            "DataCenter": data_center,
                            "Symbol": ccp_symbols,
                            "SymbolArgs": {
                                "DataSource": "BGNL",
                                'LOCAL_DATA': is_local_data_backup_datamaster,
                                "LOCAL_DATA_DIR": local_data_dir
                            },
                            "Slot": "fxrate",
                            "Fields": ["close"],
                            "StartTS": start_ts,
                            "EndTS": end_ts,
                            "Frequency": freq
                        }
                        bundles.append(fx_int)
                    # calculate interest rate
                    # TODO not support HKD CNY INT calculation
                    if not opt_noint:
                        calc_int = {
                            "Label": label+'-FX/INT',
                            "Type": "Process",
                            "DataCenter": data_center,
                            "Symbol": int_sym,
                            "SymbolArgs": {"DataSource": "INT", "UnderlyingSource": "BGNL"},
                            "Slot": "INT",
                            "Fields": "INT",
                            "Actions": "INT",
                            "StartTS": start_ts,
                            "EndTS": end_ts,
                            "Frequency": freq
                        }
                        bundles.append(calc_int)
                ############# INTEREST/DIVIDEND ##############

                # symbol INFO must be last bundle
                if len(fields)==1 and fields[0] == 'INFO':
                    # TODO dirty work. INFO bundle must be the last bundle now.
                    # To get INFO only if config 'fields' has "INFO" only
                    bundles = []
                info_ccp = {
                    "Label": label+'-FX/INFO',
                    "Type": "Process",
                    "DataCenter": data_center,
                    "Symbol": fx_select_symbols,
                    "SymbolArgs": {"DataSource": data_source},
                    "Slot": '',
                    "Fields": [""],
                    "Actions": "INFO",
                    "StartTS": start_ts,
                    "EndTS": end_ts,
                    "Frequency": freq
                }
                bundles.append(info_ccp)
                # ====================================

            if len(stock_select_symbols) != 0:
                # deal with type : STOCK (all stocks)
                # ====================================

                stockmain_sym = stock_select_symbols
                if len(dc_map_path) != 0:
                    stockmain_sym = stock_select_symbols[dm_map[LabelField.MAIN.value]]
                ############# MAIN ##############
                if len(stockmain_sym)!=0:
                    download_bundle = {
                    "Label": label+'-STOCKS/MAIN',
                    "Type": "Download",
                    "DataCenter": data_center,
                    "Symbol": stockmain_sym,
                    "SymbolArgs": {
                        "DataSource": data_source,
                        "DMAdjustType": adjust_type,
                        "DMCalendar": dm_calendar,
                        'LOCAL_DATA': is_local_data_backup_datamaster,
                        "LOCAL_DATA_DIR": local_data_dir,
                    },
                    "Slot": "data",
                    "Fields": fields,
                    "StartTS": start_ts,
                    "EndTS": end_ts,
                    "Frequency": freq
                    }
                    bundles.append(download_bundle)
                    if backup_data_source:
                        backup_bundle = {
                            "Label": label+'-STOCKS/MAIN',
                            "Type": "Download",
                            "DataCenter": data_center,
                            "Symbol": stockmain_sym,
                            "SymbolArgs": {
                                "DataSource": backup_data_source,
                                "DMAdjustType": adjust_type,
                                "DMCalendar": dm_calendar,
                                'LOCAL_DATA': is_local_data_backup_datamaster,
                                "LOCAL_DATA_DIR": local_data_dir,
                            },
                            "Slot": "data",
                            "Fields": fields,
                            "StartTS": start_ts,
                            "EndTS": end_ts,
                            "Frequency": freq
                        }
                        bundles.append(backup_bundle)

                        backup_price = {
                            "Label": label+'-STOCKS/MAIN',
                            "Type": "Process",
                            "DataCenter": data_center,
                            "Symbol": stockmain_sym,
                            "SymbolArgs": {
                                "DataSource": data_source,
                                "BackupDataSource": backup_data_source,
                                'LOCAL_DATA': is_local_data_backup_datamaster,
                                "LOCAL_DATA_DIR": local_data_dir
                            },
                            "Slot": "data",
                            "Actions": "BACKUP",
                            "Fields": fields,
                            "StartTS": start_ts,
                            "EndTS": end_ts,
                            "Frequency": freq
                        }
                        bundles.append(backup_price)
                    
                    if settlement_type:
                        settlement_data = {
                            "Label": label+'-STOCKS/MAIN',
                            "Type": "Process",
                            "DataCenter": data_center,
                            "DataCenterArgs": data_center_args,
                            "Symbol": stockmain_sym,
                            "SymbolArgs": {
                                "DataSource": new_data_source,
                                "SettlementType": settlement_type
                            },
                            "Slot": "data",
                            "Fields": fields,
                            "Actions": "SETTLEMENT",
                            "StartTS": start_ts,
                            "EndTS": end_ts,
                            "Frequency": freq
                        }
                        bundles.append(settlement_data)
                        fields = ["data", "field"]

                    stack_stock_price = {
                        "Label": label+'-STOCKS/MAIN',
                        "Type": "Process",
                        "DataCenter": data_center,
                        "DataCenterArgs": {"ffill": False},
                        "Symbol": stockmain_sym,
                        "SymbolArgs": {
                            "DataSource": new_data_source,
                            "SettlementType": settlement_type
                        },
                        "Slot": "data",
                        "Fields": fields,
                        "Actions": "STACK",
                        "StartTS": start_ts,
                        "EndTS": end_ts,
                        "Frequency": freq
                    }
                    bundles.append(stack_stock_price)
                    
                ############# MAIN ##############

                stock_fxrate_sym = stock_select_symbols
                if len(dc_map_path) != 0:
                    stock_fxrate_sym = stock_select_symbols[dm_map[LabelField.CCPFXRATE.value]]
                ############# CCPFXRATE ##############
                if len(stock_fxrate_sym)!=0:
                    # TODO currently USDCNY USDHKD FX hardcode DataMaster as datasource
                    assert acc_ccy in SUPPORTED_ACC_CCY or acc_ccy == 'LOCAL', f'Account currency {acc_ccy} is not supported currently.'
                    if acc_ccy in SUPPORTED_ACC_CCY:
                        ccp_symbols_list = []
                        for ccy in trading_currency_types:
                            if CCY2USDCCP.get(ccy,''):
                                ccp_symbols_list.append(CCY2USDCCP[ccy])
                            elif ccy == 'USD':
                                ccp_symbols_list.append('USDUSD')
                            else:
                                assert False, f'No matching ccy {ccy} in CCY2USDCCP mapping.'
                        ccp_symbols_list.append(''.join(['USD',acc_ccy]))

                        dm_ccp_list = set(ccp_symbols_list)
                        dm_ccp_list.discard('USDUSD')
                        dm_ccp_list = list(dm_ccp_list)
                        # TODO hardcode no source for USDCNY and USDHKD
                        fx_data_source = 'BGNL'
                        fx_backup_data_source = ''
                        fx_new_data_source = fx_data_source+"+" + \
                                fx_backup_data_source if fx_backup_data_source else fx_data_source
                        if dm_ccp_list:
                            # to get INFO
                            info_bundle = {
                                "Label": "INFO/info",
                                "Type": "Download",
                                "DataCenter":'DataMaster',
                                "Symbol": dm_ccp_list,
                                "SymbolArgs": {
                                    "DataSource": fx_data_source,
                                    'LOCAL_DATA': is_local_data_backup_datamaster,
                                    "LOCAL_DATA_DIR": local_data_dir
                                },
                                "Slot": 'info',
                                "Fields": "INFO",
                                "Frequency": "DAILY"
                            }
                            bundles.append(info_bundle)

                            fx_bundle = {
                                "Label": label+'-FX/MAIN',
                                "Type": "Download",
                                "DataCenter": 'DataMaster',
                                "Symbol": dm_ccp_list,
                                "SymbolArgs": {
                                    "DataSource": fx_data_source,
                                    'LOCAL_DATA': is_local_data_backup_datamaster,
                                    "LOCAL_DATA_DIR": local_data_dir
                                },
                                "Slot": "fxrate",
                                "Fields": ['close'],
                                "StartTS": start_ts,
                                "EndTS": end_ts,
                                "Frequency": freq
                            }
                            bundles.append(fx_bundle)
                            # download backup fx data bundle
                            if fx_backup_data_source != '':
                                backup_fx_bundle = {
                                    "Label": label+'-FX/MAIN',
                                    "Type": "Download",
                                    "DataCenter": 'DataMaster',
                                    "Symbol": dm_ccp_list,
                                    "SymbolArgs": {
                                        "DataSource": fx_backup_data_source,
                                        'LOCAL_DATA': is_local_data_backup_datamaster,
                                        "LOCAL_DATA_DIR": local_data_dir
                                    },
                                    "Slot": "fxrate",
                                    "Fields": ['close'],
                                    "StartTS": start_ts,
                                    "EndTS": end_ts,
                                    "Frequency": freq
                                }
                                bundles.append(backup_fx_bundle)

                                backup_fxrate = {
                                    "Label": label+'-FX/MAIN',
                                    "Type": "Process",
                                    "DataCenter": 'DataMaster',
                                    "Symbol": dm_ccp_list,
                                    "SymbolArgs": {"DataSource": fx_data_source,
                                                "BackupDataSource": fx_backup_data_source},
                                    "Slot": "fxrate",
                                    "Actions": "BACKUP",
                                    "Fields": ['close'],
                                    "StartTS": start_ts,
                                    "EndTS": end_ts,
                                    "Frequency": freq
                                }
                                bundles.append(backup_fxrate)

                        ccyfx_sym = ccp_symbols_list

                        ccy_fxrate = {
                            "Label": label+'-STOCKS/CCPFXRATE',
                            "Type": "Process",
                            "DataCenter": 'DataMaster',
                            "Symbol": ccyfx_sym,
                            "SymbolArgs": {
                                "DataSource": fx_new_data_source,
                                "UserSymbolList": stock_fxrate_sym,
                                "AccountCCY": acc_ccy
                            },
                            "Slot": 'fxrate',
                            "Fields": ["close"],
                            "Actions": "CCYFX",
                            "StartTS": start_ts,
                            "EndTS": end_ts,
                            "Frequency": freq
                        }
                        bundles.append(ccy_fxrate)

                    elif acc_ccy=='LOCAL':
                    # 'LOCAL' ccy
                        # ccy fxrate
                        ccy_fxrate = {
                            "Label": label+'-STOCKS/CCPFXRATE',
                            "Type": "Process",
                            "DataCenter": 'DataMaster',
                            "Symbol": stock_fxrate_sym,
                            "SymbolArgs": {"AccountCCY": "LOCAL"},
                            "Slot": 'fxrate',
                            "Fields": ["close"],
                            "Actions": "CCYFX",
                            "StartTS": start_ts,
                            "EndTS": end_ts,
                            "Frequency": freq
                        }
                        bundles.append(ccy_fxrate)
                ############# CCPFXRATE ##############

                stock_int_sym = stock_select_symbols
                stock_split_sym = stock_select_symbols
                only_cn_stock = []
                if len(dc_map_path) != 0:
                    stock_int_sym = stock_select_symbols[dm_map[LabelField.INT.value]]
                    only_cn_stock = stock_select_symbols[(dm_map[LabelField.SPLIT.value]) & (symbol_types == 'CN_STOCK')]
                ############# INTEREST/DIVIDEND ##############
                # TODO ONLY A share dividend is available @ 20200113
                else:
                    only_cn_stock = stock_split_sym[symbol_types == 'CN_STOCK']
                #########################
                if len(stock_int_sym)!=0:
                    # dividend
                    if (dm_download_dividend or dm_download_split) and len(only_cn_stock) != 0:
                        download_stock_dividend_and_split = {
                            "Label": label+'-STOCKS/INT',
                            "Type": "Download",
                            "DataCenter": data_center,
                            "Symbol": only_cn_stock,
                            "SymbolArgs": {},
                            "Slot": "dividend_related",
                            "Fields": "a_shares_dividend",
                            "StartTS": start_ts,
                            "EndTS": end_ts,
                            "Frequency": freq
                        }
                        bundles.append(download_stock_dividend_and_split)
                    calc_stock_int = {
                        "Label": label+'-STOCKS/INT',
                        "Type": "Process",
                        "DataCenter": data_center,
                        "Symbol": stock_int_sym,
                        "SymbolArgs": {
                            "DataSource": data_source,
                            "AssetType": "STOCK",
                            "DividendAvailable": only_cn_stock,
                            "is_needed": dm_download_dividend,
                            },
                        "Slot": "dividend_related",
                        "Fields": "INT",
                        "Actions": "INT",
                        "StartTS": start_ts,
                        "EndTS": end_ts,
                        "Frequency": freq
                    }
                    bundles.append(calc_stock_int)
                    calc_stock_split = {
                        "Label": label+'-STOCKS/SPLIT',
                        "Type": "Process",
                        "DataCenter": data_center,
                        "Symbol": stock_int_sym,
                        "SymbolArgs": {
                            "DataSource": data_source,
                            "AssetType": "STOCK",
                            "DividendAvailable": only_cn_stock,
                            "is_needed": dm_download_split,
                            },
                        "Slot": "split_related",
                        "Fields": "SPLIT",
                        "Actions": "SPLIT",
                        "StartTS": start_ts,
                        "EndTS": end_ts,
                        "Frequency": freq
                    }
                    bundles.append(calc_stock_split)
                    
                ############# INTEREST/DIVIDEND ##############

                # symbol INFO must be last bundle
                if len(fields)==1 and fields[0] == 'INFO':
                    # TODO dirty work. INFO bundle must be the last bundle now.
                    # To get INFO only if config 'fields' has "INFO" only
                    bundles = []
                info_stock = {
                    "Label": label+'-STOCKS/INFO',
                    "Type": "Process",
                    "DataCenter": data_center,
                    "Symbol": stock_select_symbols,
                    "SymbolArgs": {"DataSource": data_source},
                    "Slot": '',
                    "Fields": [""],
                    "Actions": "INFO",
                    "StartTS": start_ts,
                    "EndTS": end_ts,
                    "Frequency": freq
                }
                bundles.append(info_stock)

            if len(futures_select_symbols) != 0:
                # deal with type : futures (all furtures)
                # ====================================

                # symbol INFO
                info_futures = {
                    "Label": label+'-FUTURES/INFO',
                    "Type": "Process",
                    "DataCenter": data_center,
                    "Symbol": futures_select_symbols,
                    "SymbolArgs": {"DataSource": new_data_source},
                    "Slot": '',
                    "Fields": [""],
                    "Actions": "INFO",
                    "StartTS": start_ts,
                    "EndTS": end_ts,
                    "Frequency": freq
                }
                bundles.append(info_futures)

                if len(continuous_futures_select_symbols) != 0:
                    futuresmain_sym = continuous_futures_select_symbols
                    if len(dc_map_path) != 0:
                        futuresmain_sym = continuous_futures_select_symbols[dm_map[LabelField.MAIN.value]]
                    ############# MAIN ##############
                    if len(futuresmain_sym)!=0:

                        # deal with type : continuous contract futures
                        # ====================================
                        download_bundle = {
                            "Label": label+'-FUTURES/MAIN',
                            "Type": "Download",
                            "DataCenter": data_center,
                            "Symbol": futuresmain_sym,
                            "SymbolArgs": {
                                "DataSource": data_source,
                                "DMAdjustType": adjust_type,
                                "DMCalendar": dm_calendar,
                                'LOCAL_DATA': is_local_data_backup_datamaster,
                                "LOCAL_DATA_DIR": local_data_dir,
                            },
                            "Slot": "data",
                            "Fields": fields,
                            "StartTS": start_ts,
                            "EndTS": end_ts,
                            "Frequency": freq
                        }
                        bundles.append(download_bundle)

                        download_futures_rollover_data = {
                            "Label": label+'-FUTURES/ROLLOVER_DATA',
                            "Type": "Download",
                            "DataCenter": data_center,
                            "Symbol": futuresmain_sym,
                            "SymbolArgs": {
                                "dm_client_get": 'futures/mainforce'
                            },
                            "Slot": "rollover_data",
                            "Fields": ["N/A"], # cannot be empty list
                            "StartTS": start_ts,
                            "EndTS": end_ts,
                            "Frequency": freq
                        }
                        bundles.append(download_futures_rollover_data)

                        if backup_data_source:
                            backup_bundle = {
                                "Label": label+'-FUTURES/MAIN',
                                "Type": "Download",
                                "DataCenter": data_center,
                                "Symbol": futuresmain_sym,
                                "SymbolArgs": {
                                    "DataSource": backup_data_source,
                                    "DMAdjustType": adjust_type,
                                    "DMCalendar": dm_calendar,
                                    'LOCAL_DATA': is_local_data_backup_datamaster,
                                    "LOCAL_DATA_DIR": local_data_dir,
                                },
                                "Slot": "data",
                                "Fields": fields,
                                "StartTS": start_ts,
                                "EndTS": end_ts,
                                "Frequency": freq
                            }
                            bundles.append(backup_bundle)

                            backup_price = {
                                "Label": label+'-FUTURES/MAIN',
                                "Type": "Process",
                                "DataCenter": data_center,
                                "Symbol": futuresmain_sym,
                                "SymbolArgs": {
                                    "DataSource": data_source,
                                    "BackupDataSource": backup_data_source,
                                    'LOCAL_DATA': is_local_data_backup_datamaster,
                                    "LOCAL_DATA_DIR": local_data_dir
                                },
                                "Slot": "data",
                                "Actions": "BACKUP",
                                "Fields": fields,
                                "StartTS": start_ts,
                                "EndTS": end_ts,
                                "Frequency": freq
                            }
                            bundles.append(backup_price)

                        # stack_futures_price = {
                        #     "Label": label+'-FUTURES/MAIN',
                        #     "Type": "Process",
                        #     "DataCenter": data_center,
                        #     "Symbol": futuresmain_sym,
                        #     "SymbolArgs": {"DataSource": new_data_source},
                        #     "Slot": "data",
                        #     "Fields": fields,
                        #     "Actions": "APPEND",
                        #     "ActionsArg": {
                        #         'OriginalDataSlot': 'data',
                        #         'AppendDataSlot': 'rollover_data',
                        #         'AppendAxis': 1,
                        #         'JoinMethod': 'inner'
                        #     },
                        #     "StartTS": start_ts,
                        #     "EndTS": end_ts,
                        #     "Frequency": freq
                        # }
                        # bundles.append(stack_futures_price)
                        if settlement_type:
                            settlement_data = {
                                "Label": label+'-FUTURES/MAIN',
                                "Type": "Process",
                                "DataCenter": data_center,
                                "DataCenterArgs": data_center_args,
                                "Symbol": futuresmain_sym,
                                "SymbolArgs": {
                                    "DataSource": new_data_source,
                                    "SettlementType": settlement_type
                                },
                                "Slot": "data",
                                "Fields": fields,
                                "Actions": "SETTLEMENT",
                                "StartTS": start_ts,
                                "EndTS": end_ts,
                                "Frequency": freq
                            }
                            bundles.append(settlement_data)
                            fields = ["data", "field"]

                        stack_futures_price = {
                            "Label": label+'-FUTURES/MAIN',
                            "Type": "Process",
                            "DataCenter": data_center,
                            "DataCenterArgs": {"ffill": False},
                            "Symbol": futuresmain_sym,
                            "SymbolArgs": {
                                "DataSource": new_data_source,
                                "SettlementType": settlement_type
                            },
                            "Slot": "data",
                            "Fields": fields,
                            "Actions": "STACK",
                            "StartTS": start_ts,
                            "EndTS": end_ts,
                            "Frequency": freq
                        }
                        bundles.append(stack_futures_price)

                        stack_futures_individual_contracts_info = {
                            "Label": label+'-FUTURES/CONTRACTS_INFO',
                            "Type": "Process",
                            "DataCenter": data_center,
                            "DataCenterArgs": {"ffill": False},
                            "Symbol": futuresmain_sym,
                            "SymbolArgs": {"DataSource": new_data_source},
                            "Slot": "rollover_data",
                            "Fields": ['individual_contract', 'next_close'],
                            "Actions": "STACK",
                            "StartTS": start_ts,
                            "EndTS": end_ts,
                            "Frequency": freq
                        }
                        bundles.append(stack_futures_individual_contracts_info)
                    ############# MAIN ##############

                    futures_fxrate_sym = continuous_futures_select_symbols
                    if len(dc_map_path) != 0:
                        futures_fxrate_sym = continuous_futures_select_symbols[dm_map[LabelField.CCPFXRATE.value]]
                    ############# CCPFXRATE ##############
                    if len(futures_fxrate_sym)!=0:
                        # TODO currently USDCNY USDHKD FX hardcode DataMaster as datasource
                        assert acc_ccy in SUPPORTED_ACC_CCY or acc_ccy == 'LOCAL', f'Account currency {acc_ccy} is not supported currently.'
                        if acc_ccy in SUPPORTED_ACC_CCY:
                            ccp_symbols_list = []
                            for ccy in trading_currency_types:
                                if CCY2USDCCP.get(ccy,''):
                                    ccp_symbols_list.append(CCY2USDCCP[ccy])
                                elif ccy == 'USD':
                                    ccp_symbols_list.append('USDUSD')
                                else:
                                    assert False, f'No matching ccy {ccy} in CCY2USDCCP mapping.'
                            ccp_symbols_list.append(''.join(['USD',acc_ccy]))

                            dm_ccp_list = set(ccp_symbols_list)
                            dm_ccp_list.discard('USDUSD')
                            dm_ccp_list = list(dm_ccp_list)
                            # TODO hardcode no source for USDCNY and USDHKD
                            fx_data_source = 'BGNL'
                            fx_backup_data_source = ''
                            fx_new_data_source = fx_data_source+"+" + \
                                    fx_backup_data_source if fx_backup_data_source else fx_data_source
                            if dm_ccp_list:
                                # to get INFO
                                info_bundle = {
                                    "Label": "INFO/info",
                                    "Type": "Download",
                                    "DataCenter":'DataMaster',
                                    "Symbol": dm_ccp_list,
                                    "SymbolArgs": {
                                        "DataSource": fx_data_source,
                                        'LOCAL_DATA': is_local_data_backup_datamaster,
                                        "LOCAL_DATA_DIR": local_data_dir
                                    },
                                    "Slot": 'info',
                                    "Fields": "INFO",
                                    "Frequency": "DAILY"
                                }
                                bundles.append(info_bundle)

                                fx_bundle = {
                                    "Label": label+'-FX/MAIN',
                                    "Type": "Download",
                                    "DataCenter": 'DataMaster',
                                    "Symbol": dm_ccp_list,
                                    "SymbolArgs": {
                                        "DataSource": fx_data_source,
                                        'LOCAL_DATA': is_local_data_backup_datamaster,
                                        "LOCAL_DATA_DIR": local_data_dir
                                    },
                                    "Slot": "fxrate",
                                    "Fields": ['close'],
                                    "StartTS": start_ts,
                                    "EndTS": end_ts,
                                    "Frequency": freq
                                }
                                bundles.append(fx_bundle)
                                # download backup fx data bundle
                                if fx_backup_data_source != '':
                                    backup_fx_bundle = {
                                        "Label": label+'-FX/MAIN',
                                        "Type": "Download",
                                        "DataCenter": 'DataMaster',
                                        "Symbol": dm_ccp_list,
                                        "SymbolArgs": {
                                            "DataSource": fx_backup_data_source,
                                            'LOCAL_DATA': is_local_data_backup_datamaster,
                                            "LOCAL_DATA_DIR": local_data_dir
                                        },
                                        "Slot": "fxrate",
                                        "Fields": ['close'],
                                        "StartTS": start_ts,
                                        "EndTS": end_ts,
                                        "Frequency": freq
                                    }
                                    bundles.append(backup_fx_bundle)

                                    backup_fxrate = {
                                        "Label": label+'-FX/MAIN',
                                        "Type": "Process",
                                        "DataCenter": 'DataMaster',
                                        "Symbol": dm_ccp_list,
                                        "SymbolArgs": {"DataSource": fx_data_source,
                                                    "BackupDataSource": fx_backup_data_source},
                                        "Slot": "fxrate",
                                        "Actions": "BACKUP",
                                        "Fields": ['close'],
                                        "StartTS": start_ts,
                                        "EndTS": end_ts,
                                        "Frequency": freq
                                    }
                                    bundles.append(backup_fxrate)

                            ccyfx_sym = ccp_symbols_list

                            ccy_fxrate = {
                                "Label": label+'-FUTURES/CCPFXRATE',
                                "Type": "Process",
                                "DataCenter": 'DataMaster',
                                "Symbol": ccyfx_sym,
                                "SymbolArgs": {
                                    "DataSource": fx_new_data_source,
                                    "UserSymbolList": futures_fxrate_sym,
                                    "AccountCCY": acc_ccy
                                },
                                "Slot": 'fxrate',
                                "Fields": ["close"],
                                "Actions": "CCYFX",
                                "StartTS": start_ts,
                                "EndTS": end_ts,
                                "Frequency": freq
                            }
                            bundles.append(ccy_fxrate)

                        elif acc_ccy=='LOCAL':
                        # 'LOCAL' ccy
                            # ccy fxrate
                            ccy_fxrate = {
                                "Label": label+'-FUTURES/CCPFXRATE',
                                "Type": "Process",
                                "DataCenter": 'DataMaster',
                                "Symbol": futures_fxrate_sym,
                                "SymbolArgs": {"AccountCCY": "LOCAL"},
                                "Slot": 'fxrate',
                                "Fields": ["close"],
                                "Actions": "CCYFX",
                                "StartTS": start_ts,
                                "EndTS": end_ts,
                                "Frequency": freq
                            }
                            bundles.append(ccy_fxrate)
                    ############# CCPFXRATE ##############

                    futures_int_sym = continuous_futures_select_symbols
                    if len(dc_map_path) != 0:
                        futures_int_sym = continuous_futures_select_symbols[dm_map[LabelField.INT.value]]
                    ############# INTEREST/DIVIDEND ##############
                    if len(futures_int_sym)!=0:
                        # dividend
                        calc_futures_int = {
                            "Label": label+'-FUTURES/INT',
                            "Type": "Process",
                            "DataCenter": data_center,
                            "Symbol": futures_int_sym,
                            "SymbolArgs": {"DataSource": "INT", "AssetType": "FUTURES"},
                            "Slot": "INT",
                            "Fields": "INT",
                            "Actions": "INT",
                            "StartTS": start_ts,
                            "EndTS": end_ts,
                            "Frequency": freq
                        }
                        bundles.append(calc_futures_int)
                    ############# INTEREST/DIVIDEND ##############

                if len(individual_futures_select_symbols) != 0:
                    # TODO
                    pass

                # symbol INFO must be last bundle
                if len(fields)==1 and fields[0] == 'INFO':
                    # TODO dirty work. INFO bundle must be the last bundle now.
                    # To get INFO only if config 'fields' has "INFO" only
                    bundles = []
                info_futures = {
                    "Label": label+'-FUTURES/INFO',
                    "Type": "Process",
                    "DataCenter": data_center,
                    "Symbol": futures_select_symbols,
                    "SymbolArgs": {"DataSource": data_source},
                    "Slot": '',
                    "Fields": [""],
                    "Actions": "INFO",
                    "StartTS": start_ts,
                    "EndTS": end_ts,
                    "Frequency": freq
                }
                bundles.append(info_futures)

        elif is_output_tradable_table:
            bundles = []
            download_tradable_table_bundle = {
                "Label": label+'-MIX/TRADABLE_TABLE',
                "Type": "Download",
                "DataCenter": data_center,
                "Symbol": symbols,
                "SymbolArgs": {
                    "DataSource": data_source,
                },
                "DataCenterArgs": data_center_args,
                "Slot": "info",
                "Fields": ['tradable_table'],
                "StartTS": start_ts,
                "EndTS": end_ts,
                "Frequency": freq
            }
            bundles.append(download_tradable_table_bundle)
            process_tradable_data = {
                "Label": label+'-MIX/TRADABLE_TABLE',
                "Type": "Process",
                "DataCenter": data_center,
                "DataCenterArgs": data_center_args,
                "Symbol": symbols,
                "SymbolArgs": {
                    "DataSource": data_source,
                    "SettlementType": settlement_type
                },
                "Slot": "tradable_table",
                "Fields": ['tradable_table'],
                "Actions": "TRADABLE",
                "StartTS": start_ts,
                "EndTS": end_ts,
                "Frequency": freq
            }
            bundles.append(process_tradable_data)
            # stack_tradable_data = {
            #     "Label": label+'-MIX/TRADABLE_TABLE',
            #     "Type": "Process",
            #     "DataCenter": data_center,
            #     "DataCenterArgs": data_center_args,
            #     "Symbol": symbols,
            #     "SymbolArgs": {"DataSource": data_source},
            #     "Slot": "tradable_table",
            #     "Fields": ['tradable_table'],
            #     "Actions": "STACK",
            #     "StartTS": start_ts,
            #     "EndTS": end_ts,
            #     "Frequency": freq
            # }
            # bundles.append(stack_tradable_data)
    elif data_center == "DataManager":
        if action_type.upper() == "STACK":
            for data_type in ["MAIN", "CCPFXRATE", "INT", "CONTRACTS_INFO", "SPLIT"]:
                stack_multi_assets_data = {
                    "Label": label+'-MIX/'+data_type,
                    "Type": "Process",
                    "DataType": "numpy",
                    "DataCenter": data_center,
                    "DataCenterArgs": {"ffill": False},
                    "Symbol": symbols,
                    # "Slot": "",
                    "Fields": [data_type],
                    "Actions": "STACK",
                    "StartTS": start_ts,
                    "EndTS": end_ts,
                    "Frequency": freq
                }
                bundles.append(stack_multi_assets_data)
            # if 'FUTURES' in [sym.split('-')[-1] for sym in symbols]:
            #     stack_multi_assets_data = {
            #         "Label": label+'-MIX/CONTRACTS_INFO',
            #         "Type": "Process",
            #         "DataType": "numpy",
            #         "DataCenter": data_center,
            #         "DataCenterArgs": {"ffill": False},
            #         "Symbol": symbols,
            #         # "Slot": "",
            #         "Fields": ['CONTRACTS_INFO'],
            #         "Actions": "STACK",
            #         "StartTS": start_ts,
            #         "EndTS": end_ts,
            #         "Frequency": freq
            #     }
            #     bundles.append(stack_multi_assets_data)
            # if 'STOCKS' in [sym.split('-')[-1] for sym in symbols]:
            #     stack_multi_assets_data = {
            #         "Label": label+'-MIX/SPLIT',
            #         "Type": "Process",
            #         "DataType": "numpy",
            #         "DataCenter": data_center,
            #         "DataCenterArgs": {"ffill": False},
            #         "Symbol": symbols,
            #         # "Slot": "",
            #         "Fields": ['SPLIT'],
            #         "Actions": "STACK",
            #         "StartTS": start_ts,
            #         "EndTS": end_ts,
            #         "Frequency": freq
            #     }
            #     bundles.append(stack_multi_assets_data)
            info_futures = {
                "Label": label+'-MIX/INFO',
                "Type": "Process",
                "DataType": 'string',
                "DataCenter": data_center,
                "Symbol": symbols,
                "Fields": [""],
                "Actions": "MERGE_INFO",
                "StartTS": start_ts,
                "EndTS": end_ts,
                "Frequency": freq
                }
            bundles.append(info_futures)

    return bundles


def _redirect_data_bundle(data_bundle_list, symbol_info, local_first):
    '''
        Given a list of data bundles, eliminate the overlapped part. If local 
        first is True, will firstly use local data
    '''
    return data_bundle_list
