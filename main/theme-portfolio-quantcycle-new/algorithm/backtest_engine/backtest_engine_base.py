from .utils.get_logger import get_logger


class BacktestEngineBase:
    # 允许输入的值的集合
    DATA_TYPES = {'DAILY', 'MINLY', 'HOURLY'}
    # MINUTE_TIME_EXTRA_COLS = {'MINUTE_INDEX1', 'MINUTE_INDEX2', 'SNAPSHOT_INDEX1', 'SNAPSHOT_INDEX2', 'TRADE_INDEX1', 'TRADE_INDEX2'}

    def __init__(self):
        self.logger = get_logger('backtest_engine.backtest_engine_base')

    def load_parameters(self, parameter_list):
        raise NotImplementedError

    def load_data(self):
        raise NotImplementedError

    def run(self):
        raise NotImplementedError
