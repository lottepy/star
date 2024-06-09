from ..method_base import MethodBase
from ....utils.csv_import import backup_mnemonic_from_info_df, get_csv_info_df


class MethodINFO(MethodBase):
    ''' To get the info of symbols '''
    def __init__(self):
        super().__init__()
        self.is_final_step = True
        self.output_data_type = 'bypass_arr2raw'

    def create_data_mapping(self, data_bundle):
        self.symbols = data_bundle["Symbol"]
        data_source = data_bundle.get("SymbolArgs", {}).get("DataSource", "")
        symbol_with_source = [
            symbol+" "+data_source if data_source else symbol for symbol in self.symbols]
        dc = data_bundle.get("DataCenter","")
        dc_args = data_bundle.get("DataCenterArgs","")
        if dc == "LocalCSV":
            info_df = get_csv_info_df(dc_args.get('info',''))
            symbol_with_source = backup_mnemonic_from_info_df(self.symbols,symbol_with_source,info_df)
        self.data_mapping.update(
            {self.symbols[i]+' info': symbol_with_source[i]+"/info" for i in range(len(self.symbols))})

    def run(self):
        res_dict = {symbol: self.data_dict[symbol+' info'] for symbol in self.symbols}
        return res_dict