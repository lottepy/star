from ..method_base import MethodBase


class MethodSTRATIDMAP(MethodBase):
    def __init__(self):
        super().__init__()
        self.is_final_step = True
        self.output_data_type = 'bypass_arr2raw'
        
    def create_data_mapping(self, data_bundle):
        self.output_data_type = data_bundle.get("ActionsArg", {}).get("OutputDataType", 'bypass_arr2raw')
        self.name = data_bundle['Label'].split('-')[0]
        self.data_mapping.update({self.name+' symbols': self.name+'/symbols'})

    def run(self):
        # OLD code
        # return [self.data_dict[sym].values[0][0].tolist() for sym in self.symbols]
        
        # id_list = self.data_dict[self.name][2]
        return self.data_dict[self.name+' symbols'].iloc[:,0].tolist()