import json
import pickle
from pathlib import Path
from typing import Dict
import numpy as np 

def load_package_from_pkl(package_path: str) -> dict:
    ''' To load package from pickle

    Args:
        package_path (str): path of the pickle package
    '''
    with open(package_path, 'rb') as f:
        rslt = pickle.load(f)
    return rslt


def dump_package_to_pkl(data_package: dict, config: dict, package_path: str, config_path: str) -> None:
    ''' To dump package into pickle

    Args:
        data_package (dict): the dictionary of numpy array data
        config (dict): config dictionary
        package_path (str): path of the pickle
        config_path (str): path of the config
    '''
    with open(package_path, 'wb') as f:
        pickle.dump(data_package, f)
    with open(config_path, 'w') as f:
        json.dump(config, f, indent=2)
    return

####### FOR DATAMASTER PICKLE ######
def load_datamaster_data(pkl_config):
    save_dir = pkl_config.get("save_dir", "")
    save_name = pkl_config.get("save_name", "")
    data_path = str(Path(save_dir).resolve().joinpath(
        f'DATAMASTER_{save_name}'))
    data_path = data_path + '.pkl'
    config_path = data_path + '_dm_datagroup.json'
    return load_package_from_pkl(data_path)

def save_datamaster_data(datamaster_data, pickle_datagroup, pkl_config):
    save_dir = pkl_config.get("save_dir", "")
    save_name = pkl_config.get("save_name", "")
    path = str(Path(save_dir).resolve().joinpath(
        f'DATAMASTER_{save_name}'))
    data_path = path + '.pkl'
    config_path = path + '_dm_datagroup.json'
    np_array_to_list(pickle_datagroup)
    dump_package_to_pkl(datamaster_data, pickle_datagroup, 
        data_path, config_path)

def np_array_to_list(res_dict):
    for d in res_dict.values():
        for k,v in d.items():
            if type(v) == np.ndarray:
                d[k] = v.tolist()
####### FOR DATAMASTER PICKLE ######
