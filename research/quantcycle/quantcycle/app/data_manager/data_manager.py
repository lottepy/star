""" DataManager module

This module aims at handling all data relevant requests, including getting symbol 
infomation from datamaster, getting symbol historical data from datamaster, getting 
symbol historical data from local backup, resampling downloaded data, and so on.

This module is one of the most complex modules in the project.

In short, this module will download and prepare all data for the platform.

Please refer to the README.md for details

Todo:
    * For module TODOs
"""
from copy import deepcopy

from quantcycle.app.data_manager.utils import DATA_MANAGER_LOG_ROOT, DATA_MANAGER_LOG_PARENT
from quantcycle.utils.get_logger import get_logger

from .data_distributor import DataDistributorMain
from .data_loader import DataLoader
from .data_processor import DataProcessor
from .utils.data_collection import DataCollection


class DataManager():
    ''' 
    This module aims at handling all data relevant requests, including getting symbol 
    infomation from datamaster, getting symbol historical data from datamaster, getting 
    symbol historical data from local backup, resampling downloaded data, and so on.

    Please refer to the README.md for details

    Attributes:
        prepared (bool): status of module 
        config_dict (dict): config from user
        data_loader (DataLoader): DataLoader obj
        data_processor (DataProcessor): DataProcessor obj
        data_distributor_main (DataDistributorMain): DataDistributorMain obj
        data_collection (DataCollection): DataCollection obj
    '''

    def __init__(self):
        ''' Initialize and prepare sub-modules '''
        logger_name = DATA_MANAGER_LOG_PARENT+'.DataManager' if DATA_MANAGER_LOG_PARENT else 'DataManager'
        self.logger = get_logger(
            logger_name,
            str(DATA_MANAGER_LOG_ROOT.joinpath(
                f'{self.__class__.__name__}.log'))
        )
        self.prepared = False
        self.config_dict = {}
        self.data_loader = DataLoader()
        self.data_processor = DataProcessor()
        self.data_distributor_main = DataDistributorMain()

        self.data_collection = DataCollection()
        self.prepare()
        self.logger.info('Prepared!')

    def load_config(self, config_dict: dict) -> None:
        ''' deepcopy config_dict'''
        self.logger.info(f'Receive Config...')
        self.config_dict = deepcopy(config_dict)

    def update_data_handler(self) -> None:
        """ give the new handler for data to module """
        self.data_loader.data_collection = self.data_collection
        self.data_processor.data_collection = self.data_collection
        self.data_distributor_main.data_collection = self.data_collection

    def load_data(self) -> None:
        self.logger.info(f'Downloading Data...')
        self.data_loader.load_data(self.config_dict)

    def process_data(self) -> None:
        self.logger.info(f'Processing Data...')
        self.data_processor.run()

    def pack_data(self) -> None:
        dm_pickle = self.config_dict.get("dm_pickle",{})
        from_pkl = dm_pickle.get("from_pkl",False)
        to_pkl = dm_pickle.get("to_pkl",False)
        self.logger.info(f'Packing Data...from pkl: {from_pkl}, to pkl: {to_pkl}')
        self.data_distributor_main.pack_data(from_pkl=from_pkl, to_pkl=to_pkl)

    def prepare(self) -> None:
        ''' To prepare all sub-modules '''
        self.update_data_handler()
        self.data_loader.prepare(self)
        self.data_processor.prepare(self)
        self.data_distributor_main.prepare(self)
        self.prepared = True

    def run(self) -> None:
        ''' Major run function

        1. DataLoader loads data from data sources
        2. DataProcessor process data 
        3. DataDistributorMain pack data into Raw Array
        '''

        if not self.prepared:
            self.prepare()
        self.load_data()
        self.process_data()
        self.pack_data()

    def start(self):
        # for streaming data
        raise NotImplementedError

    def handle_bundle(self, data_bundle: dict) -> None:
        ''' A method can be called to load data given the data_bundle 

        Note: one data_bundle should be provided in each function call only.
        '''
        self.data_loader.handle_bundle(data_bundle)
