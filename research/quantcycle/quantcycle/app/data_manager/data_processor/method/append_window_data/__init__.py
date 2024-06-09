import pandas as pd

from ..method_base import MethodBase


class MethodAPPEND(MethodBase):
    def __init__(self):
        super().__init__()
        self.is_final_step = False
        self.output_data_type = 'df'

    def create_data_mapping(self, data_bundle):
        self.symbols = data_bundle["Symbol"]
        self.data_source = data_bundle["SymbolArgs"]["DataSource"]
        # !!!!
        self.original_data_slot = data_bundle["ActionsArg"].get("OriginalDataSlot", 'data')
        self.append_data_slot = data_bundle["ActionsArg"].get("AppendDataSlot", 'data')
        self.axis = data_bundle["ActionsArg"].get("AppendAxis", 1)
        self.join = data_bundle["ActionsArg"].get("JoinMethod", "inner")

        for symbol in self.symbols:
        #     self.data_mapping[symbol +
        #                       " old"] = f"{symbol} {self.data_source}/{self.slot}"
        #     self.data_mapping[symbol +
        #                       " append_df"] = f"{symbol} {self.append_data_source}/{self.slot}"

            self.data_mapping[symbol +
                                " old"] = f"{symbol}/{self.original_data_slot}"
            self.data_mapping[symbol +
                                " append_df"] = f"{symbol}/{self.append_data_slot}"
        # self.data_mapping[symbol+" symbol"] = f"{symbol} {self.data_source}/symbol"
        # self.data_mapping[symbol+" info"] = f"{symbol} {self.data_source}/info"
        # self.data_mapping[symbol+" timestamp"] = f"{symbol} {self.data_source}/timestamp"
        # self.data_mapping[symbol+" fields"] = f"{symbol} {self.data_source}/fields"

    def run(self):
        data = {}
        for symbol in self.symbols:
            old_df = self.data_dict[f"{symbol} old"].copy()
            append_df = self.data_dict.get(f"{symbol} append_df", None)
            if append_df.columns[0] in old_df.columns:
                continue
            new_df = pd.concat([old_df,append_df], join=self.join, axis=self.axis)

            # To overwrite the old df
            data[f"{symbol}/{self.original_data_slot}"] = new_df

        # for symbol in self.symbols:
        #     old_df = self.data_dict[f"{symbol} old"].copy()
        #     append_df = self.data_dict.get(f"{symbol} append_df", None)
        #     new_df = old_df.append(append_df)

        #     # To overwrite the old df
        #     data[f"{symbol} {self.data_source}/{self.slot}"] = new_df

        return data
