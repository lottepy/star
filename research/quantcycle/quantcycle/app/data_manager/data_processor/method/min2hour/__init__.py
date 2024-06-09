from ..method_base import MethodBase
import pandas as pd
import numpy as np


class MethodMIN2HOUR(MethodBase):
    '''
        To change minute data into hour data 
        # TODO now forward fill for nan value. (refer to V21)
        
    '''
    
    def __init__(self):
        super().__init__()
        self.is_final_step=False
        self.output_data_type = 'df'
        
    def create_data_mapping(self, data_bundle):
        self.symbols = data_bundle["Symbol"]
        self.data_source = data_bundle["SymbolArgs"]["DataSource"]
        self.slot = data_bundle.get("Slot","data")

        for symbol in self.symbols:
            self.data_mapping[symbol+" old"] = f"{symbol} {self.data_source}/{self.slot}"
        
    def run(self):        
        data = {}
        for symbol in self.symbols:
            old_df = self.data_dict[f"{symbol} old"].copy()

            # 1. change timestamp index to datetime
            old_df.index = pd.to_datetime(old_df.index, unit='s')            
            # 2. resample for open high low close
            # TODO now ffill nan data (refer to V21 get_dm_fx_hour_data)
            resample_obj = old_df.ffill().resample('60min')
            new_df = pd.DataFrame()
            for col in old_df.columns:
                if col == "open":
                    new_df[col] = resample_obj[col].first()
                elif col == "high":
                    new_df[col] = resample_obj[col].max()
                elif col == "low":
                    new_df[col] = resample_obj[col].min()
                elif col == "close":
                    new_df[col] = resample_obj[col].last()
                else:
                    # TODO add other data manipulations here
                    raise ValueError(f'Undefined column name: {col}')  
            new_df = new_df.dropna(how='all')

            # 3. change back to timestamp index
            new_df.index = new_df.index.values.astype(np.int64) // 10 ** 9

            data[f"{symbol} {self.data_source}/{self.slot} hour"]=new_df
            
        return data