# -*- coding:utf-8 -*-

class PairedTradeRecord:
    def __init__(self, universe=None, strategy_param=None, trade_symbol_list=None,
                 open_time_list=None, close_time_list=None, direction_list=None,
                 profit_list=None, transaction_cost_list=None, status_list = None):
        # 策略相关
        self.universe = universe
        self.strategy_param = strategy_param
        # 交易记录相关
        self.trade_symbol_list = trade_symbol_list if trade_symbol_list is not None else []# 买卖的对象 (m,)
        self.open_time_list = open_time_list if open_time_list is not None else []# 开仓的时间 (m,)
        self.close_time_list = close_time_list if close_time_list is not None else []# 平仓的时间 (m,)
        self.direction_list = direction_list if direction_list is not None else []# 多空仓 (m,) 用1表示多仓, -1表示空仓.
        self.profit_list = profit_list if profit_list is not None else []# 每一笔的利润 (m,)
        self.transaction_cost_list = transaction_cost_list if transaction_cost_list is not None else []# 交易费用 (m,)
        self.status_list = status_list if status_list is not None else []# 是否还未平仓 (m,) bool

class BetRecord:
    def __init__(self, universe=None, strategy_param=None, trade_symbol_list=None, direction_list=None, open_time_list=None,
                 close_time_list=None,  transaction_cost_list=None, status_list=None, profit_list = None):
        # 策略相关
        self.universe = universe
        self.strategy_param = strategy_param
        # 交易记录相关
        self.trade_symbol_list = trade_symbol_list  # 买卖的对象 (m,)
        self.open_time_list = open_time_list  # 开仓的时间 (m,)
        self.close_time_list = close_time_list  # 平仓的时间 (m,)
        self.direction_list = direction_list  # 多空仓 (m,) 用"L"表示多仓, "S"表示空仓.
        self.profit_list = profit_list  # 每一笔的利润 (m,)
        self.transaction_cost_list = transaction_cost_list  # 交易费用 (m,)
        self.status = status_list  # 是否还未平仓 (m,) bool
