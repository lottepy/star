import numpy as np
import pandas as pd

import json
import os
import unittest
from datetime import datetime

from engine_manager import EngineManager
from quantcycle.app.order_crosser.order_router import \
    TestOrderRouter as OrderRouter
from quantcycle.app.result_exporter.result_reader import ResultReader
from quantcycle.engine.quant_engine import QuantEngine
from quantcycle.utils import get_logger
from quantcycle.utils.run_once import run_once
from quantcycle.utils.strategy_pool_generator import strategy_pool_generator


class ReferenceData(unittest.TestCase):

    def test_stocks_trade_hourly_watch_daily(self):
        # mimic of the tests/engine/backtest/test_ref_data.py >> test_stocks_trade_hourly_watch_daily
        # main data: test_hourly_precision/data/STOCK/stock_data_better hourly >> jumping hourly points
        # ref data: main_daily 08:00 UTC / main_daily_open 00:00 UTC
        pool_setting = {"symbol": {"STOCKS": ['STOCK_1', 'STOCK_2', 'STOCK_3', 'STOCK_4', 'STOCK_5']},
                        "strategy_module": "tests.engine.production.test_signal_remark.algorithm.EW_main_hourly_remark",
                        "strategy_name": "EW_strategy",
                        "params": {
                            # EW has no parameters
                        }}
        quant_engine = QuantEngine()
        quant_engine.load_config(json.load(open(
            'tests/engine/production/test_signal_remark/config/stocks_main_hourly.json')),
            strategy_pool_generator(pool_setting, save=False))

        quant_engine.load_app(OrderRouter())
        quant_engine.load_data()

        quant_engine.start_production_engine()
        quant_engine.start_production_engine_other()

        quant_engine.end_production_engine()
        quant_engine.wait_production_engine()

        quant_engine.end_production_engine_other()
        quant_engine.wait_production_engine_other()

        df = pd.read_csv(r'tests/engine/production/test_signal_remark/remarks/remark_main_hourly_ref_daily-0.csv')
        df_sum_rows = df.sum(axis=0, numeric_only=True)
        assert np.isclose(sum(df_sum_rows), 8.0)
    
    def test_stocks_trade_daily_watch_daily_hourly(self):
        # mimic of the tests/engine/backtest/test_ref_data.py >> test_stocks_trade_hourly_watch_daily
        # main data: main_daily 08:00 UTC 
        # ref data: main_hourly / main_daily_info 09:00 UTC
        pool_setting = {"symbol": {"STOCKS": ['STOCK_1', 'STOCK_2', 'STOCK_3', 'STOCK_4', 'STOCK_5']},
                        "strategy_module": "tests.engine.production.test_signal_remark.algorithm.EW_main_daily_remark",
                        "strategy_name": "EW_strategy",
                        "params": {
                            # EW has no parameters
                        }}
        quant_engine = QuantEngine()
        quant_engine.load_config(json.load(open(
            'tests/engine/production/test_signal_remark/config/stocks_main_daily.json')),
            strategy_pool_generator(pool_setting, save=False))

        quant_engine.load_app(OrderRouter())
        quant_engine.load_data()

        quant_engine.start_production_engine()
        quant_engine.start_production_engine_other()

        quant_engine.end_production_engine()
        quant_engine.wait_production_engine()

        quant_engine.end_production_engine_other()
        quant_engine.wait_production_engine_other()

        #tests\engine\production\test_signal_remark\remarks\remark_main_daily_ref_hourly
        
        df = pd.read_csv(r'tests/engine/production/test_signal_remark/remarks/remark_main_daily_ref_hourly-0.csv')
        df_sum_rows = df.sum(axis=0, numeric_only=True)
        assert np.isclose(sum(df_sum_rows), 4.0)
