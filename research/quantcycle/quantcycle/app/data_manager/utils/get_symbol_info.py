'''
    Get symbol info from data master
'''
from ..utils.csv_import import *
from ..utils.handle_data_bundle import download_data_bundle
from ..utils.update_nested_dict import update_dict, deep_compare_dict


def get_symbol_info(data_group_list, proxies, dc_symbol_map):
    info_data_group_list = [data_group for data_group in data_group_list if data_group["DataCenter"] != "ResultReader" and data_group["DataCenter"] != "DataManager" and not (data_group["DataCenter"] == "LocalCSV" and "TRADABLE_TABLE" in data_group.get("DataCenterArgs",{}).get("key_name",''))]
    gp_symbols, gp_datacenters, gp_dc_args, gp_sym_args = _get_all_symbol(info_data_group_list, dc_symbol_map)
    info_bundles = _generate_info_bundle(gp_symbols, gp_datacenters, gp_dc_args, gp_sym_args)
    info = {}
    for info_bundle in info_bundles:
        update_dict(info, download_data_bundle(info_bundle, proxies))
    return info


def _get_all_symbol(data_group_list, dc_symbol_map):
    '''
        get all symbols from all data group
    '''
    symbols = []
    datacenters = []
    dc_args = []
    sym_args = []
    
    for data_group in data_group_list:
        symbol = data_group["Symbol"]
        datacenter = data_group["DataCenter"]
        dc_arg = data_group.get("DataCenterArgs", {})
        sym_arg = data_group.get("SymbolArgs", {})
        symbol_args = data_group.get("SymbolArgs", {})
        data_source = symbol_args.get("DataSource", "")
        # symbol = [
        #     i+" "+data_source if data_source else i for i in symbol]
        if "DatacenterMap" in sym_arg and len(sym_arg["DatacenterMap"])!=0:
            _load_info_dc_sym_map(symbol, sym_arg["DatacenterMap"], dc_symbol_map)
        else:
            _load_info_dc_sym_map(symbol, dc_arg.get("info", ""), dc_symbol_map)
        symbols.append([sym for sym in symbol if dc_symbol_map.get(sym,'datamaster')==datacenter.lower()])
        datacenters.append(datacenter)
        dc_args.append(dc_arg)
        sym_args.append(sym_arg)
        
        ''' GET INFO FROM DATAMASTER FIRST
        
        if data_group["DataCenter"] == "LocalCSV":
            if not symbol:
                symbol = get_sym_from_csv_name(dc_arg['dir'], dc_arg['info'])
            datacenter = 'DataMaster'

        symbols.append(symbol)                        
        datacenters.append(datacenter)
        dc_args.append(dc_arg)
        sym_args.append(sym_arg)

        if data_group["DataCenter"] == "LocalCSV":
            #### one more data bundle to update info with info.csv using LocalCSV
            symbols.append(symbol)                        
            datacenters.append('LocalCSV')
            dc_args.append(dc_arg)
            sym_args.append(sym_arg)
        '''

    # symbols = list(set(symbols))
    return symbols, datacenters, dc_args, sym_args


def _generate_info_bundle(gp_symbols: list, gp_datacenters: list, gp_dc_args:list, gp_sym_args: list):
    '''
        generate a info bundle sent to dm proxy
    '''
    res = []
    for i in range(len(gp_symbols)):
        if gp_datacenters[i] == 'ResultReader':
            # skip if DataCenter is ResultReader
            continue
        info_bundle = {
            "Label": "INFO/info",
            "Type": "Download",
            "DataCenter":gp_datacenters[i],
            "DataCenterArgs": gp_dc_args[i],
            "Symbol": gp_symbols[i],
            "SymbolArgs": gp_sym_args[i],  
            "Slot": 'info',
            "Fields": "INFO",
            "Frequency": "DAILY"
        }
        if any([deep_compare_dict(bundle, info_bundle) for bundle in res]):
            continue
        res.append(info_bundle) 
    return res

def _load_info_dc_sym_map(symbol, dc_map_path, dc_symbol_map):
    if len(dc_map_path) == 0: return
    info_df = get_csv_info_df(dc_map_path)
    info_dict = info_df_to_sym_dict(info_df)
    dc_symbol_map.update(dict(zip(symbol, get_dc_list(symbol, info_dict, 'MAIN'))))
