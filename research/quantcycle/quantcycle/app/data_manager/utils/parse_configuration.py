'''
    Parser for setting in the config dict which is relative to data downloading
'''
import copy
from os import path
import pandas as pd

from .update_nested_dict import update_dict

DATA_GROUP_MUST_HAVE = [
    "Label", "Type", "DataCenter", "Symbol", "Fields", "StartDate", "EndDate", "Frequency"
]
PICKLE_MUST_HAVE = [
    "from_pkl", "to_pkl", "save_dir", "save_name"
]

def parse_config(config_dict: dict):
    '''
        input:
            config_dict: dictionary containing all information; may include 
                         things like a path for symbols
        output:
            symbol_list: a bunch of symbols which can be derived from different 
                         data senter via requests
            queue_list: a bunch of queues which will be fed with streaming data
            setting: a dictionary recording other setting like save prepared 
                     data to hard drive
    '''
    data_group_list = _examine_component(config_dict)
    transformed_data_group_list = \
        [_type_confirm_transform(i) for i in data_group_list]
    return transformed_data_group_list


def _examine_component(config_dict: dict):
    '''
        assert some items must exist in config_dict, then return 
        the combined data group in a list

        Data in config_dict.keys()
        at least 1 data group in config_dict['Data']
        in each data group, there must exist
            label, datacenter, symbol
        in each data group, there must exist
            StartDate, EndDate, Frequency,
            OR, in default values, these terms exist

        Data Group are items in config["Data"], which has "DataGroup" 
            in keys
    '''
    assert "Data" in config_dict.keys(), "No 'Data' section in Config"
    if config_dict.get("dm_pickle", {}) != {}:
        for i in PICKLE_MUST_HAVE:
            assert i in config_dict["dm_pickle"].keys(), f"{i} does not exist in 'dm_pickle' config"
        assert path.exists(config_dict["dm_pickle"]['save_dir']), f"dm_pickle: save_dir not exist"
        assert type(config_dict["dm_pickle"]['save_name'])==str, f"dm_pickle: save_name should be string type"
        assert type(config_dict["dm_pickle"]['to_pkl'])==bool, f"dm_pickle: to_pkl should be True or False"
        assert type(config_dict["dm_pickle"]['from_pkl'])==bool, f"dm_pickle: from_pkl should be True or False"        
        if config_dict["dm_pickle"]['from_pkl'] == True and config_dict["dm_pickle"]['to_pkl'] == False:
            return []
    data_dict = config_dict["Data"]
    keys = list(data_dict.keys())
    data_group_key_list = [i for i in keys if "DataGroup" in i]
    assert len(data_group_key_list) > 0, "No 'DataGroup' in Config.Data"
    default_values = data_dict.get("DefaultValues", {})
    data_group_list = []
    data_group_label_list = []
    for key in data_group_key_list:
        data_group = data_dict.get(key)
        _default_values = copy.deepcopy(default_values)
        _data_group = copy.deepcopy(data_group)
        combined_data_group = update_dict(_default_values, _data_group)
        for i in DATA_GROUP_MUST_HAVE:
            assert i in combined_data_group.keys(), f"{i} not in DataGroup"
        data_group_list.append(combined_data_group)
        data_group_label_list.append(combined_data_group['Label'])
    assert len(data_group_label_list) == \
        len(list(set(data_group_label_list))), "Same Labels for DataGroups"
    return data_group_list


def _type_confirm_transform(data_group):
    """
        read static file which will be stored in config.json in a format
        as path, rather than true values, such as symbol list.
        examine some variables' type
    """
    flds = "Label"
    if flds in data_group.keys():
        assert type(data_group[f"{flds}"]) == str, f"TypeError: {flds}"

    flds = "Type"
    if flds in data_group.keys():
        assert type(data_group[f"{flds}"]) == str, f"TypeError: {flds}"
        assert data_group[f"{flds}"] in ["Download", "Queue", "STACK"]

    flds = "DataCenter"
    if flds in data_group.keys():
        assert type(data_group[f"{flds}"]) == str, f"TypeError: {flds}"
        assert data_group[f"{flds}"] in ["DataMaster", "LocalHDF5", "ResultReader","LocalCSV","DataManager"]

    flds = "Symbol"
    if flds in data_group.keys():
        if data_group.get('DataCenter','') != 'ResultReader':
            if type(data_group[f"{flds}"]) == list:
                for i in data_group[f"{flds}"]:
                    assert type(i) == str, f"TypeError: {flds}"
            else:
                assert type(data_group[f"{flds}"]) == str, f"TypeError: {flds}"
                if ".csv" in data_group[f"{flds}"]:
                    data_group[f"{flds}"] = _read_local_csv_symbol(
                        data_group[f"{flds}"])
        elif data_group.get('DataCenter','') == 'ResultReader':
            # strat ID must be int
            if type(data_group[f"{flds}"]) == list or type(data_group[f"{flds}"]) == range:
                for i in data_group[f"{flds}"]:
                    assert type(i) == int, f"TypeError: {flds} strat ID should be 'int'"
            else:
                assert type(data_group[f"{flds}"]) == int or data_group[f"{flds}"] == 'ALL', f"TypeError: {flds} strat ID should be integer or 'ALL'"
                if ".csv" in data_group[f"{flds}"]:
                    data_group[f"{flds}"] = _read_local_csv_symbol(
                        data_group[f"{flds}"])

    flds = "Fields"
    if flds in data_group.keys():
        if type(data_group[f"{flds}"]) == list:
            for i in data_group[f"{flds}"]:
                assert type(i) == str, f"TypeError: {flds}"
            # checking if necessary args for 'metrics' are included
            if 'metrics' in data_group[f"{flds}"]:
                assert data_group['SymbolArgs']['Metrics'], f"For metrics, necessary Metrics args are missing"
                assert type(data_group['SymbolArgs']['Metrics']['allocation_freq']) == str, f"For metrics, allocation_freq is missing or incorrect"
                assert type(data_group['SymbolArgs']['Metrics']['lookback_points_list']) == list, f"For metrics, lookback_points_list is missing or incorrect"
                assert type(data_group['SymbolArgs']['Metrics']['addition']) == bool, f"For metrics, addition is missing or incorrect"
                assert data_group['SymbolArgs']['Metrics']['multiplier'], f"For metrics, multiplier is missing or incorrect"
        else:
            assert type(data_group[f"{flds}"]) == str, f"TypeError: {flds}"
            
    flds = "StartDate"
    if flds in data_group.keys():
        assert type(data_group[f"{flds}"]) == dict, f"TypeError: {flds}"
        data_group["StartTS"] = _parse_start_end_date(data_group[f"{flds}"])

    flds = "EndDate"
    if flds in data_group.keys():
        assert type(data_group[f"{flds}"]) == dict, f"TypeError: {flds}"
        data_group["EndTS"] = _parse_start_end_date(data_group[f"{flds}"])

    flds = "Frequency"
    if flds in data_group.keys():
        assert type(data_group[f"{flds}"]) == str, f"TypeError: {flds}"

    return data_group


def _read_local_csv_symbol(csv_path):
    """
        Read local csv symbols
    """
    tmp = pd.read_csv(csv_path)
    symbol_list = tmp.iloc[:, -1].values.tolist()
    return symbol_list


def _parse_start_end_date(date_dict):
    """
        parse date_dict to utc timestamp
        date_dict can be like:
            Year: 2012, (must)
            Month: 1, (must)
            Day:1, (must)
            Hour:1, (optional)
            Minute:1, (optional)
            Second:1,(optional)
    """
    assert "Year" in date_dict.keys()
    assert "Month" in date_dict.keys()
    assert "Day" in date_dict.keys()
    if "Second" in date_dict.keys():
        assert "Minute" in date_dict.keys()
    if "Minute" in date_dict.keys():
        assert "Hour" in date_dict.keys()

    default_date_dict = {"Hour": 0,
                         "Minute": 0,
                         "Second": 0}
    tmp = update_dict(default_date_dict, date_dict)
    return int(pd.to_datetime(f'{tmp["Year"]:04d}{tmp["Month"]:02d}{tmp["Day"]:02d} {tmp["Hour"]}:{tmp["Minute"]}:{tmp["Second"]}').timestamp())
