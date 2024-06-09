import copy

from quantcycle.app.data_manager.utils import DATA_MANAGER_LOG_ROOT, DATA_MANAGER_LOG_PARENT
from quantcycle.app.data_manager.utils.data_collection import \
    long_key2key_attrs
from quantcycle.utils.get_logger import get_logger


class MethodBase():
    ''' Different data processing 'methods' are defined under this base class.
    Including INT, STACK, CCYFX, BACKUP, APPEND, MIN2HOUR.

    Attributes:
        deepcopy (bool): 
            If deepcopy is True, all data will be deep copied into method.data,
            otherwise, shallow copy will be applied for performance.
            Default to False.
        is_final_step (bool):
            If True, the result will be sent to distributor, 
            If not, the result will be sent to update data collection.
        data_dict (dict):
            Stores the necessary dataframe to be used.
        data_mapping (dict):
            A data mapping to access data from data_collection under DataManager.

    To add a new method, please refer to the data_processor.py docstring or README.md.

    Output from 'run' method:
        DataFrame or list of info according to is_final_step boolean.
    '''

    def __init__(self):
        self.deepcopy = False
        # If True, the result will be sent to distributor, If not, the result will be sent to update data collection
        self.is_final_step = False
        self.output_data_type = ''
        self.data_dict = {}
        self.data_mapping = {}  # key: local name, value: data_collection.key/attrs
        logger_name = DATA_MANAGER_LOG_PARENT+f'.DataManager.DataProcessor.{self.__class__.__name__}' if DATA_MANAGER_LOG_PARENT else f'DataManager.DataProcessor.{self.__class__.__name__}'
        self.logger = get_logger(
            logger_name,
            str(DATA_MANAGER_LOG_ROOT.joinpath(
                f'{self.__class__.__name__}.log'))
        )

    def on_bundle(self, data_bundle: dict, data_collection) -> None:
        ''' To get necessary data from DataProcessor

        Args:
            data_bundle (dict): The data bundle info from DataProcessor
            data_collection (DataCollection): The data collection info from DataProcessor
        '''
        # do something here
        # ...
        # create data mapping
        self.create_data_mapping(data_bundle)
        # collet mapped data
        self.map_data(data_collection)

    def create_data_mapping(self, data_bundle: dict) -> None:
        ''' To create data mapping for map_data to grab necessary data in DataProcessor.
        !! Please implement the method in the corresponding method class. !!

        Args:
            data_bundle (dict): If True, all data will be deep copied into 
            method.data, otherwise, shallow copy will be applied for performance.
        '''
        raise NotImplementedError

    def map_data(self, data_collection) -> None:
        ''' To grab data from the DataProcessor according to the data_mapping, 
            and store in data_dict.

        Args:
            data_collection (dict): data_collection from DataProcessor
        '''
        if self.deepcopy:
            self.data_dict.update({k: copy.deepcopy(getattr(data_collection[long_key2key_attrs(
                v)[0]], long_key2key_attrs(v)[1])) for k, v in self.data_mapping.items()})
        else:
            self.data_dict.update({k: getattr(data_collection[long_key2key_attrs(
                v)[0]], long_key2key_attrs(v)[1]) for k, v in self.data_mapping.items()})

    def run(self) -> None:
        ''' The major method run function.
        !! Please implement the method in the corresponding method class. !!
        '''
        raise NotImplementedError
