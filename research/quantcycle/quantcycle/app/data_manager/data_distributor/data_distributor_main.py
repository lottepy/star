""" DataDistributor module

This module is under the DataManager module. 
This module aims to convert the 3D numpy array into Raw Array or vice versa.

The module has two major files, `data_distributor_main.py` and `data_distributor_sub.py`.
`data_distributor_main.py` consists of methods for data packaging.
`data_distributor_sub.py` consists of methods for data unpackaging.

`data_distributor_main.py`
For the pack_data method, option of data backup, and data loading via pickle are 
also supported.

Todo:
    * For module TODOs
"""


import json
from pathlib import Path
from typing import Dict

import numpy as np

from quantcycle.app.data_manager.utils.pickle_helper import load_package_from_pkl, dump_package_to_pkl
from quantcycle.app.data_manager.utils import DATA_MANAGER_LOG_ROOT, DATA_MANAGER_LOG_PARENT
from quantcycle.utils.get_logger import get_logger

from ..utils.update_nested_dict import deep_compare_dict
from .utils.raw_array_transform import array_to_raw_array


class DataDistributorMain():
    def __init__(self):
        '''      
        Parameters:    
            data_package: dict
                {data_bundle["Label"]: tuple1,
                 data_bundle["Label"]: tuple2,
                 data_bundle["Label"]: tuple3 ....}

                tuple format:
                    index 0: Raw array of data array body
                    index 1: shape of data array body
                    index 2: Raw array of time array body
                    index 3: shape of time array body
                    index 4: list of symbol names
                    index 5: list of fields names
        '''
        self.data_collection = None
        self.data_package: Dict = None
        self.dict_np_data_need_pack: Dict = {}
        self.dict_str_data_need_pack: Dict = {}
        self.save_dir = ''
        self.save_prefix = 'DATA_PACKAGE'
        self.save_name = ''
        logger_name = DATA_MANAGER_LOG_PARENT+'.DataManager.DataDistributorMain' if DATA_MANAGER_LOG_PARENT else 'DataManager.DataDistributorMain'
        self.logger = get_logger(
            logger_name,
            str(DATA_MANAGER_LOG_ROOT.joinpath(
                f'{self.__class__.__name__}.log'))
        )

    def prepare(self, data_manager):
        self.data_manager = data_manager

    def pack_data(self, from_pkl: bool = False, to_pkl: bool = False) -> None:
        '''
        To pack whole dict of numpy array into dict of raw array

        Before pack from DataProcessor:
        np array needed to be converted into RawArray
            {
                data_bundle["Label"]: 
                    {
                        'data_arr':...,
                        'time_arr':..., 
                        'symbol_arr':...,
                        'fields_arr':...
                    },
                data_bundle["Label"]: 
                    {
                        'data_arr':...,
                        'time_arr':..., 
                        'symbol_arr':...,
                        'fields_arr':...
                    },
                ...
            }
        others data to be saved directly into the data_package
            {
                data_bundle["Label"]: value 1
                data_bundle["Label"]: value 2
            }        

        After packing:
            data_package: dict
            key == 'raw': dict
                {data_bundle["Label"]: tuple1,
                 data_bundle["Label"]: tuple2,
                 data_bundle["Label"]: tuple3 ....}
            key == 'others': dict
                {data_bundle["Label"]: value1,
                 data_bundle["Label"]: value2,
                 data_bundle["Label"]: value3 ....}

            tuple format:
                index 0: Raw array of data array body
                index 1: shape of data array body
                index 2: Raw array of time array body
                index 3: shape of time array body
                index 4: list of symbol names
                index 5: list of fields names
        '''
        dict_data = dict()
        dict_data['raw'] = dict()
        dict_data['others'] = dict()

        pkl_config = _get_pickle_config(self.data_manager.config_dict)
        self.save_dir = pkl_config.get("save_dir", "")
        self.save_name = pkl_config.get("save_name", "")
        to_pkl = pkl_config.get("to_pkl", to_pkl)
        from_pkl = pkl_config.get("from_pkl", from_pkl)

        np_data = self.dict_np_data_need_pack
        str_data = self.dict_str_data_need_pack
        if to_pkl and (np_data != {} or str_data != {}):
            self._pack_into_pickle(np_data, str_data)

        # data will be overwritten by pickle data
        if from_pkl:
            np_data, str_data = self._get_from_pickle()

        # Convert numpy numerical data array into Raw Array
        for key in np_data.keys():

            data_arr = np_data[key]['data_arr'].astype('float64')
            time_arr = np_data[key]['time_arr']
            symbols_arr = np_data[key]['symbol_arr']
            fields_arr = np_data[key]['fields_arr']

            dict_data['raw'][key] = tuple([array_to_raw_array(data_arr), data_arr.shape, array_to_raw_array(
                time_arr), time_arr.shape, symbols_arr, fields_arr])

        # Save the dict or str data directly into the package
        for key in str_data.keys():
            dict_data['others'][key] = str_data[key]

        self.data_package = dict_data

    def _pack_into_pickle(self, np_data: dict, str_data: dict) -> None:
        data_path = str(Path(self.save_dir).resolve().joinpath(
            f'{self.save_prefix}_{self.save_name}'))
        np_data_path = data_path + '_np.pkl'
        str_data_path = data_path + '_str.pkl'
        config_path = data_path + '.json'

        dump_package_to_pkl(
            np_data, self.data_manager.config_dict, np_data_path, config_path)
        dump_package_to_pkl(
            str_data, self.data_manager.config_dict, str_data_path, config_path)

    def _get_from_pickle(self) -> tuple:
        data_path = str(Path(self.save_dir).resolve().joinpath(
            f'{self.save_prefix}_{self.save_name}'))
        np_data_path = data_path + '_np.pkl'
        str_data_path = data_path + '_str.pkl'
        config_path = data_path + '.json'

        if _examine_config(config_path, self.data_manager.config_dict):
            np_data = load_package_from_pkl(np_data_path)
            str_data = load_package_from_pkl(str_data_path)
        else:
            raise ValueError(
                "Dumped Data has different config! Can't load data from pkl")
        return np_data, str_data


# def load_package_from_pkl(package_path: str) -> dict:
#     ''' To load package from pickle

#     Args:
#         package_path (str): path of the pickle package
#     '''
#     with open(package_path, 'rb') as f:
#         rslt = pickle.load(f)
#     return rslt


# def dump_package_to_pkl(data_package: dict, config: dict, package_path: str, config_path: str) -> None:
#     ''' To dump package into pickle

#     Args:
#         data_package (dict): the dictionary of numpy array data
#         config (dict): config dictionary
#         package_path (str): path of the pickle
#         config_path (str): path of the config
#     '''
#     with open(package_path, 'wb') as f:
#         pickle.dump(data_package, f)
#     with open(config_path, 'w') as f:
#         json.dump(config, f, indent=2)
#     return


def _examine_config(configA_path: str, configB: dict) -> bool:
    ''' examine if config is the same as the one at config_path
    excluding the pickle part

    Args:
        configA_path (str): the path to JSON config file
        configB (dict): dictionary of the config
    '''
    configA = json.load(open(configA_path, 'r'))
    tmp_A = {"dm_pickle": configA.pop("dm_pickle", {})}
    tmp_B = {"dm_pickle": configB.pop("dm_pickle", {})}
    is_dict_same = deep_compare_dict(configA, configB)
    configB.update(tmp_B)
    return is_dict_same

def _get_pickle_config(raw_config: dict):
    pkl_config = {}
    pkl_config.update(raw_config.get("dm_pickle", {}))
    return pkl_config