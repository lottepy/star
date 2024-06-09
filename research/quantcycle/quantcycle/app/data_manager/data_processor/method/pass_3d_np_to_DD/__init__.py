from ..method_base import MethodBase
from ....utils.timestamp_manipulation import *
import numpy as np


class MethodPASS3DNP(MethodBase):
    ''' To pass 3D np array to DD '''
    def __init__(self):
        super().__init__()
        self.is_final_step = True
        self.output_data_type = 'np_numeric'

    def create_data_mapping(self, data_bundle):
        self.label = data_bundle["Label"]
        self.field = data_bundle["Fields"]
        self.data_center = data_bundle["DataCenter"]
        self.slot = data_bundle['Slot']
        self.name = self.label
        if self.data_center == "ResultReader":
            self.name = self.label.split('-')[0]
        self.data_mapping.update({self.name: self.name+"/"+self.field})

    def run(self):
        '''
            For ResultReader, the last tuple value is "Strat ID to Symbols/fields Mapping"

        Special case for id_sym_map:
            As for 'pnl', there is only one 'pnl' value for one strat ID, id_sym_map would be
            'pnl': [ ['pnl'], ['pnl'], ... ]
        '''
        if self.data_center == "ResultReader":
            tuple_data = self.data_dict[self.name]
            data_arr = tuple_data[0]
            time_arr = list2timearr(tuple_data[1])
            list_ids = tuple_data[2]
            id_sym_map = tuple_data[3]
            
        return {'data_arr': data_arr, 'time_arr': time_arr, 'symbol_arr': list_ids, 'fields_arr': id_sym_map}
