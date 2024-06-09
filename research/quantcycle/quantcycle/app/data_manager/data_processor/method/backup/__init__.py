import pandas as pd

from ..method_base import MethodBase


class MethodBACKUP(MethodBase):
    def __init__(self):
        super().__init__()
        self.is_final_step = False
        self.output_data_type = 'df'

    def create_data_mapping(self, data_bundle):
        self.symbols = data_bundle["Symbol"]
        self.data_source = data_bundle["SymbolArgs"]["DataSource"]
        self.backup_data_source = data_bundle["SymbolArgs"]["BackupDataSource"]
        self.slot = data_bundle.get("Slot", "data")
        self.new_data_source = self.data_source+"+"+self.backup_data_source
        for symbol in self.symbols:
            self.data_mapping[symbol +
                              " old"] = f"{symbol} {self.data_source}/{self.slot}"
            self.data_mapping[symbol +
                              " new"] = f"{symbol} {self.backup_data_source}/{self.slot}"

            self.data_mapping[symbol +
                              " symbol"] = f"{symbol} {self.data_source}/symbol"
            self.data_mapping[symbol +
                              " info"] = f"{symbol} {self.data_source}/info"
            self.data_mapping[symbol +
                              " timestamp"] = f"{symbol} {self.data_source}/timestamp"
            self.data_mapping[symbol +
                              " fields"] = f"{symbol} {self.data_source}/fields"

    def run(self):
        data = {}
        for symbol in self.symbols:
            old_df = self.data_dict[f"{symbol} old"].copy()
            new_df = self.data_dict[f"{symbol} new"]
            empty_new = pd.DataFrame(index=new_df.index)
            old_df = pd.concat([old_df, empty_new], axis=1)
            old_df.update(new_df, overwrite=False)
            data[f"{symbol} {self.new_data_source}/{self.slot}"] = old_df

            data[f"{symbol} {self.new_data_source}/symbol"] = self.data_dict.get(
                f"{symbol} symbol", None)
            data[f"{symbol} {self.new_data_source}/info"] = self.data_dict.get(
                f"{symbol} info", None)
            data[f"{symbol} {self.new_data_source}/timestamp"] = self.data_dict.get(
                f"{symbol} timestamp", None)
            data[f"{symbol} {self.new_data_source}/fields"] = self.data_dict.get(
                f"{symbol} fields", None)
        return data
