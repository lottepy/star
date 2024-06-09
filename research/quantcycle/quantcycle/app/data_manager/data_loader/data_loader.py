""" DataLoader module

This module is under DataManager module.
This sub-module has three main functionalities.
- Parse configuration dictionary
- Generate and route data bundle
- Download data bundle

Since additional data is needed during the process of parsing data configuration 
and routing data bundles, it's better for `DataLoader` includes the first two 
functionalities. 

Please refer to the README.md for details

Todo:
    * For module TODOs
"""


from pathlib import Path

import numpy as np

from quantcycle.app.data_manager.utils import DATA_MANAGER_LOG_ROOT, DATA_MANAGER_LOG_PARENT
from quantcycle.utils.get_logger import get_logger

from ..utils.data_collection import update_data
from ..utils.get_symbol_info import get_symbol_info
from ..utils.handle_data_bundle import (classify_data_bundle,
                                        download_data_bundle, establish_queue)
from ..utils.parse_configuration import parse_config
from ..utils.route_data_group import route_data_request
from ..utils.pickle_helper import load_package_from_pkl, dump_package_to_pkl, load_datamaster_data, save_datamaster_data
from ..utils.data_collection import long_key2key_attrs, DataCollection


class DataLoader():
    def __init__(self):
        self.data_collection = None
        self.data_manager = None
        self.data_processor = None
        self.pickle_datamaster_keys = []
        self.pickle_datagroup = {}
        self.is_pickle_done = False
        self.proxies = {}
        # self.proxies["DataMaster"] = DataMaster()
        logger_name = DATA_MANAGER_LOG_PARENT+'.DataManager.DataLoader' if DATA_MANAGER_LOG_PARENT else 'DataManager.DataLoader'
        self.logger = get_logger(
            logger_name,
            str(DATA_MANAGER_LOG_ROOT.joinpath(
                f'{self.__class__.__name__}.log'))
        )
        self.dc_symbol_map = {}

    def prepare(self, data_manager):
        self.data_manager = data_manager
        self.data_processor = data_manager.data_processor

    def load_data(self, raw_config: dict) -> None:
        ''' Take original config dict as input. 

        Parse config file into a list of data_group
        Update symbol info into data_collection
        Route data request according to the list of data group into list of data_bundle
        Load or handle data according to the list of data_bundle

        Args: 
            raw_config (dict): raw config from user
        '''
        data_group_list = parse_config(raw_config)
        symbol_info = get_symbol_info(data_group_list, self.proxies, self.dc_symbol_map)
        update_data(self.data_collection, symbol_info)
        data_bundle_list = route_data_request(data_group_list, symbol_info)
        self._handle_bundle_list(data_bundle_list)

    def _handle_bundle_list(self, data_bundle_list: list) -> None:
        '''
            For each bundle, send to target data center proxy.
            Collect returned data and update self.data_collection,
            or establish queue to receive streaming data
            In the meantime, register the required process methods 
            to data processor.

        Args:
            data_bundle_list (list): list of data_bundle dict
        '''
        _download, _queue, _process = classify_data_bundle(data_bundle_list)
        self.logger.info(
            f'#: {len(_download)} for downloading, {len(_queue)} for queue, {len(_process)} for processing')
        # TODO Multi threading can be applied
        pkl_config = self.data_manager.config_dict.get('dm_pickle',{})
        is_pickle_datamaster = pkl_config.get("pickle_datamaster",False)
        is_load_datamaster = pkl_config.get("load_datamaster",False)
        self.load_datamaster_pickle(pkl_config)
        for i, data_bundle in enumerate(_download):
            update_dc_args(data_bundle, is_load_datamaster, is_pickle_datamaster)
            data = download_data_bundle(data_bundle, self.proxies)
            update_data(self.data_collection, data)
            ########### FOR UNITTEST PICKLE ###########
            if is_pickle_datamaster and not self.is_pickle_done:
                if data_bundle['DataCenter'] == 'DataMaster': 
                    self.pickle_datamaster_keys.extend(data.keys())
                    self.pickle_datagroup[f"{i}"] = (data_bundle)
                if data_bundle['DataCenter'] != 'DataMaster' or i+1 == len(_download):
                    self.pickle_datamaster_data(data, pkl_config)
            ############################################
        for data_bundle in _queue:
            q = establish_queue(data_bundle, self.proxies)
            self.data_processor.connect_queue(q)
        for data_bundle in _process:
            self.data_processor.register(data_bundle)

    def handle_bundle(self, data_bundle: dict) -> None:
        ''' A method can be called to load data given the data_bundle 

        Note: one data_bundle should be provided in each function call only.
        '''
        self._handle_bundle_list([data_bundle])

    def load_datamaster_pickle(self, pkl_config):
        if not pkl_config.get("load_datamaster",False): return
        update_data(self.data_collection, load_datamaster_data(pkl_config))

    def pickle_datamaster_data(self, data, pkl_config):
        ''' assumptions: Datamaster data group must come before LocalCSV datagroups'''
        if not pkl_config.get("pickle_datamaster",False): return
        datamaster_data = map_datamaster_data(self.pickle_datamaster_keys, self.data_collection)
        save_datamaster_data(datamaster_data, self.pickle_datagroup, pkl_config)
        self.is_pickle_done = True

def update_dc_args(data_bundle, is_load_datamaster, is_pickle_datamaster):
    if data_bundle.get('DataCenter', '')!= 'DataMaster': return 
    dc_args = data_bundle.get('DataCenterArgs',{})
    dc_args.update({
        'is_load_datamaster': is_load_datamaster,
        'is_pickle_datamaster': is_pickle_datamaster,
        })
    data_bundle['DataCenterArgs'] = dc_args

def map_datamaster_data(long_keys, data_collection):
    res = {}
    res.update({k: getattr(data_collection[long_key2key_attrs(
        k)[0]], long_key2key_attrs(k)[1]) for k in long_keys})
    return res