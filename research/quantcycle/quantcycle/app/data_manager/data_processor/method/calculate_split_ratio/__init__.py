import pathlib
from typing import List
from datetime import datetime

import numpy as np
import pandas as pd

from quantcycle.app.data_manager.data_loader.data_center.mapping import (
    CCY2USDCCP, FWD_SCALE, IS_NDF)

from ..method_base import MethodBase

class MethodSPLIT(MethodBase):
    '''
        action == 'SPLIT' : construct df method
        To output a df with interest rate 

        1. construct df with necessary info 
        2. call method
        3. construct output df

        Ref. V21 get_dm_fx_daily_dividends
    '''

    def __init__(self):
        super().__init__()
        self.is_final_step = True
        self.output_data_type = 'df'

    def on_bundle(self, data_bundle: dict, data_collection) -> None:
        ''' To get necessary data from DataProcessor

        Args:
            data_bundle (dict): The data bundle info from DataProcessor
            data_collection (DataCollection): The data collection info from DataProcessor
        '''
        # # assumption: data_group have one asset type only
        # symbol = data_bundle['Symbol'][0]
        # sym_type = data_collection.get(symbol).info['symboltype']
        # self.is_stock_type = sym_type=='US_STOCK' or sym_type=='HK_STOCK' or sym_type=='CN_STOCK'
        # self.is_fx_type = sym_type=='FX'

        # create data mapping
        self.create_data_mapping(data_bundle, data_collection)
        # collet mapped data
        self.map_data(data_collection)

    def create_data_mapping(self, data_bundle, data_collection):
        self.sym_type = data_bundle['SymbolArgs'].get('AssetType', 'FX')
        self.symbols = data_bundle['Symbol']
        self.cn_symbols = data_bundle['SymbolArgs'].get('DividendAvailable', data_bundle['Symbol'])
        self.is_needed = data_bundle['SymbolArgs'].get('is_needed', False)
        self.start = data_bundle['StartTS']
        self.end = data_bundle['EndTS']
        
        if self.sym_type == 'STOCK' and self.is_needed:
            '''
            GET choice_cash_dividend_fin2
            '''
            split_ratio_df = pd.DataFrame()
            split_ratio_list = []
            for symbol in self.symbols:
                if symbol in self.cn_symbols:
                    split_ratio_df = data_collection[symbol].dividend_related.choice_transformation
                    split_ratio_list.append(split_ratio_df)
                else:
                    split_ratio_list.append(pd.DataFrame())
            self.split_ratio = split_ratio_list
            # check = 0

    def run(self):
        rate_df, sym_list, fields = self._STOCK_run()

        return rate_df, sym_list, fields
    
    def _STOCK_run(self):
    
        column = self.symbols
        # Since datamaster returns timestamps at 6:57am, we must adjust the start and end time (default 00:00am)
        if self.is_needed:
            start_timestamp = self.start
            start_dt = datetime.fromtimestamp(start_timestamp)
            adjust_start_dt = start_dt.replace(hour=14,minute=57)
            adjust_start_timestamp = int(datetime.timestamp(adjust_start_dt))
            end_timestamp = self.end
            end_dt = datetime.fromtimestamp(end_timestamp)
            adjust_end_dt = end_dt.replace(hour=14,minute=57)
            adjust_end_timestamp = int(datetime.timestamp(adjust_end_dt))
            timestamp_range = []
            while adjust_start_timestamp <= adjust_end_timestamp:
                timestamp_range.append(adjust_start_timestamp)
                adjust_start_timestamp+=86400
            # Create an empty dataframe with all timestamps
            non_fx_Rate_df = pd.DataFrame({},columns=column,index=timestamp_range).ffill(axis=0,).fillna(0)
            # Insert all entries where ratios occur
            for i in range(len(self.split_ratio)):
                non_zero_dates = self.split_ratio[i].index
                non_zero_values = self.split_ratio[i].values   
            # since data master returns an NaN entry as the ratio on a day where a cash dividend occurs
            # fill in these NaN values with 0
                for j in range(len(non_zero_dates)):
                    if non_zero_values[j] == non_zero_values[j]:
                        non_fx_Rate_df.loc[non_zero_dates[j],column[i]] = non_zero_values[j]
                    else:
                        non_fx_Rate_df.loc[non_zero_dates[j],column[i]] = 0.0
            # Add 1 to all values in the dataframe
            col_names = [col for col in non_fx_Rate_df if non_fx_Rate_df[col].dtype.kind == 'f']
            non_fx_Rate_df[col_names] += 1
            # Filter out rows that are all equal to 1 if the previous row is also all 1, unless it is the first row
            is_first = True
            for index,row in non_fx_Rate_df.iterrows():
                no_change_boolean = (row.fillna(1.0).array==(1.0)).all()
                if no_change_boolean == True and is_first == True:
                    is_first = False
                elif no_change_boolean == True and is_first == False:
                    non_fx_Rate_df.drop(index, inplace=True)
                elif no_change_boolean == False:
                    is_first = True
        else:
            non_fx_Rate_df = pd.DataFrame(1.0,columns=column,index=[self.start])


        return non_fx_Rate_df, self.symbols, ['split_ratio_last']
