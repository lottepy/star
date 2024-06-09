import numpy as np

from quantcycle.app.signal_generator.base_strategy import defaultjitclass, BaseStrategy


class SampleStrategy(BaseStrategy):
    def init(self):
        self.symbol_batch = self.metadata["main"]["symbols"]

    def on_data(self, data_dict: dict, time_dict: dict, ref_data_dict: dict, ref_time_dict: dict):
        init_cash = self.portfolio_manager.init_cash
        target = np.ones(len(self.symbol_batch)) * init_cash / len(self.symbol_batch)
        return self.return_reserve_target_base_ccy(target).reshape(1,-1)