import pandas as pd

from ..method_base import MethodBase
from ....utils.csv_import import backup_mnemonic_from_info_df, get_csv_info_df

class MethodSTACK(MethodBase):
    def __init__(self):
        super().__init__()
        self.is_final_step = True
        self.output_data_type = 'df'
        
    def create_data_mapping(self, data_bundle):
        self.label = data_bundle["Label"]
        self.symbols = data_bundle["Symbol"]
        self.is_final_step = data_bundle.get("ActionsArg", {}).get("is_final_step", True)
        self.output_data_type = data_bundle.get("ActionsArg", {}).get("OutputDataType", 'df')
        self.concat_axis = 1 if self.output_data_type == 'df' else 0
        self.dc = data_bundle.get("DataCenter","")
        dc_args = data_bundle.get("DataCenterArgs",{})
        self.is_ffill = dc_args.get("ffill", True)
        data_source = data_bundle.get("SymbolArgs", {}).get("DataSource", "")
        symbol_with_source = [
            symbol+" "+data_source if data_source else symbol for symbol in self.symbols]
        #### LocalCSV ####
        if self.dc == "LocalCSV":
            info_df = get_csv_info_df(dc_args.get('info',''))
            symbol_with_source = backup_mnemonic_from_info_df(self.symbols,symbol_with_source,info_df)
        slot = data_bundle.get("Slot", "data")
        self.data_mapping.update(
            {self.symbols[i]: str(symbol_with_source[i])+"/"+slot for i in range(len(self.symbols))})

    def run(self):
        lst_df = []
        for symbol in self.symbols:
            lst_df.append(self.data_dict[symbol])
        symbols = self.symbols
        if not lst_df:
            df = pd.DataFrame()
            symbols = self.symbols
            lst_df = [pd.DataFrame()]
            columns = []
        else:
            df = pd.concat(lst_df, axis=self.concat_axis)
            df = df.ffill() if self.is_ffill else df

        #### LocalCSV dependent ####
        if self.dc == 'LocalCSV':
            label_field = self.label.split('/')[-1]
            if label_field == 'CCPFXRATE':
                columns = ['fx_rate']
                df = df.ffill()
                df.columns = symbols
            elif label_field == 'INT':
                columns = ['interest_rate_last']
                df = df.fillna(0.0)
                df.columns = symbols
            else:
                columns = list(lst_df[0].columns)
        ############################
        else:
            columns = list(lst_df[0].columns)

        if self.output_data_type == 'df':
            result = (df, symbols, columns)
        elif self.output_data_type == 'bypass_arr2raw':
            result = df
        return result
