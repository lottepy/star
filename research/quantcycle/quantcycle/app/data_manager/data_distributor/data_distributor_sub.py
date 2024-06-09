import numpy as np

from quantcycle.app.data_manager.utils import DATA_MANAGER_LOG_ROOT, DATA_MANAGER_LOG_PARENT
from quantcycle.utils.get_logger import get_logger

from .utils.raw_array_transform import raw_array_to_array


class DataDistributorSub():
    def __init__(self):
        logger_name = DATA_MANAGER_LOG_PARENT+'.DataManager.DataDistributorSub' if DATA_MANAGER_LOG_PARENT else 'DataManager.DataDistributorSub'
        self.logger = get_logger(
            logger_name,
            str(DATA_MANAGER_LOG_ROOT.joinpath(
                f'{self.__class__.__name__}.log'))
        )

    def unpack_data(self, data_package: dict) -> dict:
        '''
        To unpack whole dict of raw array into dict of numpy array

        format: Please refer to pack_data method, reverse of the data structure.
        '''
        dict_output = dict()

        # numerical numpy data
        for key in data_package['raw'].keys():
            dict_data = dict()
            tuple_data = data_package['raw'][key]
            data_arr = tuple_data[0]
            data_arr_shape = tuple_data[1]
            time_arr = tuple_data[2]
            time_arr_shape = tuple_data[3]
            dict_data['data_arr'] = raw_array_to_array(
                data_arr, data_arr_shape)
            dict_data['time_arr'] = raw_array_to_array(
                time_arr, time_arr_shape)
            dict_data['symbol_arr'] = tuple_data[4]
            dict_data['fields_arr'] = tuple_data[5]
            dict_output[key] = dict_data

        # string data
        for key in data_package['others'].keys():
            dict_output[key] = data_package['others'][key]

        return dict_output
