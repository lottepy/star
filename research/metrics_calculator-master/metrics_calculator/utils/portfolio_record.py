import numpy as np

from .trade_record import TradeRecord


class PortfolioRecord:
    def __init__(self, universe, pv0=None, simple_pv=None, position_list=None, pv_list=None, timestamp_list=None,
                 ref_aum_list=None, ccy_name_list=None, ccy_metrics_list=None, strategy_param=None, strategy_id=None,
                 signal_remark=None, trade_record=None, record_class='fx', commission = None):
        # strategy_id for the universe and parameter -int -example: 14
        self.strategy_id = strategy_id
        # symbol series (list) --example: ["000008.SZ","000009.SZ",]
        self.universe = universe
        # strategy parameter series (list_list) --example: [[0.2,10,],[0.4,5,],]
        self.strategy_param = strategy_param
        self.pv0 = pv0  # simple pv (list) --example: [4038,4038,]
        self.simple_pv = simple_pv  # simple pv (list) --example: [4038,4038,]
        # timestamp of every sample (list) --example: [1765165165.0,1765165168.0,]
        self.timestamp_list = timestamp_list
        # position of every sample (list_list) --example: [[100,400,],[100,300,],]
        self.position_list = position_list
        # pv of every sample (list_list) --example: [[4038,4038,],[4038,4038,],]
        self.pv_list = pv_list
        self.ref_aum_list = ref_aum_list
        self.ccy_name_list = ccy_name_list
        self.ccy_metrics_list = ccy_metrics_list
        self.sample_len = 0
        self.commission = commission
        if position_list is not None:
            # number of sample (int) --example: 145
            self.sample_len = len(position_list)
        self.trade_record = trade_record
        self.signal_remark = signal_remark
        self.record_class = record_class

    def print_record_info(self):
        print(f'<record> '
              f'strategy_id:[{self.strategy_id}] '
              f'universe:{self.universe} '
              f'strategy_param:{self.strategy_param} '
              f'pv:{sum(self.simple_pv)}'
              f'simple_pv:{self.simple_pv}')

    def array_to_list(self):
        self.position_list = [array.tolist() for array in self.position_list]
        self.pv_list = [array.tolist() for array in self.pv_list]
        self.ccy_metrics_list = [array.tolist()
                                 for array in self.ccy_metrics_list]
        self.pv0 = self.pv0.tolist()
        self.simple_pv = self.simple_pv.tolist()

    def _to_array(self):
        self.timestamp_list = np.array(self.timestamp_list)
        self.simple_pv = np.array(self.simple_pv)
        self.position_list = np.array(self.position_list)
        self.signal_remark = np.array(self.signal_remark)

        if self.trade_record is not None:
            self.trade_record._to_array()
            self.trade_record._flatten()
