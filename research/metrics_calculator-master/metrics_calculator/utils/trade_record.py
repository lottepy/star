import numpy as np


class TradeRecord:
    def __init__(self, timestamp_list, trade_symbol_list,
                 price_list, volume_list, transaction_cost=None, uuid=None, universe=None, strategy_param=None,
                 record_class='fx'):
        self.uuid = uuid  # uuid  -int -example: 14
        # symbol series (list)  --example: ["000008.SZ","000009.SZ",]
        self.universe = universe
        # strategy parameter series (list_list)  --example: [[0.2,10,],[0.4,5,],]
        self.strategy_param = strategy_param
        self.timestamp_list = timestamp_list
        self.trade_symbol_list = trade_symbol_list
        self.price_list = price_list
        self.volume_list = volume_list
        self.trade_num = len(timestamp_list)
        self.record_class = record_class
        if transaction_cost is None:
            self.transaction_cost = np.zeros_like(trade_symbol_list)
        else:
            self.transaction_cost = transaction_cost