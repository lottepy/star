import unittest
import numpy as np
import pandas as pd
from quantcycle.utils.strategy_pool_generator import strategy_pool_generator
from quantcycle.utils.run_once import run_once


class RefDataTest(unittest.TestCase):
    def test_stocks_trade_hourly_watch_daily(self):
        # main data: test_hourly_precision/data/STOCK/stock_data_better hourly >> jumping hourly points
        # ref data: main_daily 08:00 UTC / main_daily_open 00:00 UTC
        pool_setting = {"symbol": {"STOCKS": ['STOCK_1', 'STOCK_2', 'STOCK_3', 'STOCK_4', 'STOCK_5']},
                        "strategy_module": "tests.engine.backtest.test_ref_data.algorithm.EW_main_hourly_remark",
                        "strategy_name": "EW_strategy",
                        "params": {
                            # EW has no parameters
                        }}

        run_once('tests/engine/backtest/test_ref_data/config/stocks_main_hourly.json',
                 strategy_pool_generator(pool_setting, save=False))
        df = pd.read_csv(r'tests/engine/backtest/test_ref_data/remarks/remark_main_hourly_ref_daily-0.csv')
        df_sum_rows = df.sum(axis=0, numeric_only=True)
        assert np.isclose(sum(df_sum_rows), 8.0)


    def test_stocks_trade_daily_watch_daily_hourly(self):
        # main data: main_daily 08:00 UTC 
        # ref data: main_hourly / main_daily_info 09:00 UTC
        pool_setting = {"symbol": {"STOCKS": ['STOCK_1', 'STOCK_2', 'STOCK_3', 'STOCK_4', 'STOCK_5']},
                        "strategy_module": "tests.engine.backtest.test_ref_data.algorithm.EW_main_daily_remark",
                        "strategy_name": "EW_strategy",
                        "params": {
                            # EW has no parameters
                        }}

        run_once('tests/engine/backtest/test_ref_data/config/stocks_main_daily.json',
                 strategy_pool_generator(pool_setting, save=False))
        df = pd.read_csv(r'tests/engine/backtest/test_ref_data/remarks/remark_main_daily_ref_hourly-0.csv')
        df_sum_rows = df.sum(axis=0, numeric_only=True)
        assert np.isclose(sum(df_sum_rows), 4.0)
