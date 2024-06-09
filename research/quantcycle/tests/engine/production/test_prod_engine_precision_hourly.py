import json
import os
import unittest
from datetime import datetime

import numpy as np
import pandas as pd

from engine_manager import EngineManager
from quantcycle.app.order_crosser.order_router import \
    TestOrderRouter as OrderRouter
from quantcycle.app.result_exporter.result_reader import ResultReader
from quantcycle.engine.quant_engine import QuantEngine
from quantcycle.utils import get_logger
from quantcycle.utils.run_once import run_once
from quantcycle.utils.strategy_pool_generator import strategy_pool_generator



class TestProductionEngine(unittest.TestCase):
    pass
    """ def test_EW_US_STOCK_lv1(self):
        # Day cut off at 16:00 UTC. No skipping timestamp at this time.
        # HK stocks are the same as US stocks
        quant_engine = QuantEngine()
        pool_setting = {"symbol": {"STOCKS": ['STOCK_1', 'STOCK_2', 'STOCK_3', 'STOCK_4', 'STOCK_5']},
                        "strategy_module": "tests.engine.backtest.test_hourly_precision.algorithm.EW_lv1",
                        "strategy_name": "EW_strategy",
                        "params": {
                                      # EW has no parameters
                                  }}
        quant_engine.load_config(json.load(open('tests/engine/backtest/test_hourly_precision/config/EW_US_STOCK.json')), strategy_pool_generator(pool_setting,save=False))
        reader = ResultReader(r'tests/engine/backtest/test_hourly_precision/result/EW_US_STOCK_lv1.hdf5')
        final_pnl = reader.get_strategy(id_list=[0], fields=["pnl"])[0][0].iloc[-1, 0]
        assert np.isclose(final_pnl, -21797.60) """


    def test_EW_US_STOCK_lv1(self):
        # Day cut off at 16:00 UTC. No skipping timestamp at this time.
        # HK stocks are the same as US stocks

        pool_setting = {"symbol": {"STOCKS": ['STOCK_1', 'STOCK_2', 'STOCK_3', 'STOCK_4', 'STOCK_5']},
                        "strategy_module": "tests.engine.production.test_hourly_precision.algorithm.EW_lv1",
                        "strategy_name": "EW_strategy",
                        "params": {
                                      # EW has no parameters
                                  }}
        
        quant_engine = QuantEngine()
        quant_engine.load_config(json.load(open(
            'tests/engine/production/test_hourly_precision/config/EW_US_STOCK.json')),
            strategy_pool_generator(pool_setting, save=False))

        quant_engine.load_app(OrderRouter())
        quant_engine.load_data()

        quant_engine.start_production_engine()
        quant_engine.start_production_engine_other()

        quant_engine.end_production_engine()
        quant_engine.wait_production_engine()

        quant_engine.end_production_engine_other()
        quant_engine.wait_production_engine_other()

        path_name = os.path.join("tests/engine/production/test_hourly_precision/result",
                                 f"EW_US_STOCK_lv1.hdf5")
        
        reader = ResultReader(path_name)

        final_pnl = reader.get_strategy(id_list=[0], fields=["pnl"])[0][0].iloc[-1, 0]

        reader.to_csv(export_folder = 'data_hourly/us_stock_prod', id_list = [0])
        assert np.isclose(final_pnl, -21797.60)

    def test_EW_CN_STOCK_lv1(self):
        # Calc rate at 16:00 UTC. No skipping timestamp at this time.
        pool_setting = {"symbol": {"STOCKS": ['STOCK_1', 'STOCK_2', 'STOCK_3', 'STOCK_4', 'STOCK_5']},
                        "strategy_module": "tests.engine.production.test_hourly_precision.algorithm.EW_lv1",
                        "strategy_name": "EW_strategy",
                        "params": {
                                      # EW has no parameters
                                  }}

        quant_engine = QuantEngine()
        quant_engine.load_config(json.load(open(
            'tests/engine/production/test_hourly_precision/config/EW_CN_STOCK.json')),
            strategy_pool_generator(pool_setting, save=False))

        quant_engine.load_app(OrderRouter())
        quant_engine.load_data()

        quant_engine.start_production_engine()
        quant_engine.start_production_engine_other()

        quant_engine.end_production_engine()
        quant_engine.wait_production_engine()

        quant_engine.end_production_engine_other()
        quant_engine.wait_production_engine_other()

        path_name = os.path.join("tests/engine/production/test_hourly_precision/result",
                                 f"EW_CN_STOCK_lv1.hdf5")
        
        reader = ResultReader(path_name)
        
        
        final_pnl = reader.get_strategy(id_list=[0], fields=["pnl"])[0][0].iloc[-1, 0]
        #reader.to_csv(export_folder = 'data_hourly/cn_stock_prod', id_list = [0])
        assert np.isclose(final_pnl, -21787)


    def test_EW_US_STOCK_better_timestamps(self):
        # Day cut off at 16:00 UTC. No skipping timestamp at this time.
        # HK stocks are the same as US stocks
        pool_setting = {"symbol": {"STOCKS": ['STOCK_1', 'STOCK_2', 'STOCK_3', 'STOCK_4', 'STOCK_5']},
                        "strategy_module": "tests.engine.production.test_hourly_precision.algorithm.EW_lv1",
                        "strategy_name": "EW_strategy",
                        "params": {
                                      # EW has no parameters
                                  }}

        quant_engine = QuantEngine()
        quant_engine.load_config(json.load(open(
            'tests/engine/production/test_hourly_precision/config/EW_US_STOCK_better.json')),
            strategy_pool_generator(pool_setting, save=False))

        quant_engine.load_app(OrderRouter())
        quant_engine.load_data()

        quant_engine.start_production_engine()
        quant_engine.start_production_engine_other()

        quant_engine.end_production_engine()
        quant_engine.wait_production_engine()

        quant_engine.end_production_engine_other()
        quant_engine.wait_production_engine_other()

        path_name = os.path.join("tests/engine/production/test_hourly_precision/result",
                                 f"EW_US_STOCK_better_lv1.hdf5")
        
        reader = ResultReader(path_name)

        final_pnl = reader.get_strategy(id_list=[0], fields=["pnl"])[0][0].iloc[-1, 0]
        #reader.to_csv(export_folder = 'data_hourly/us_stock_prod_better_timestamps', id_list = [0])
        assert np.isclose(final_pnl, -19411.57)


    def test_EW_US_STOCK_better_main_only(self):
        # Day cut off at 16:00 UTC. No skipping timestamp at this time.
        # HK stocks are the same as US stocks
        pool_setting = {"symbol": {"STOCKS": ['STOCK_1', 'STOCK_2', 'STOCK_3', 'STOCK_4', 'STOCK_5']},
                        "strategy_module": "tests.engine.production.test_hourly_precision.algorithm.EW_lv1",
                        "strategy_name": "EW_strategy",
                        "params": {
                                      # EW has no parameters
                                  }}
        quant_engine = QuantEngine()
        quant_engine.load_config(json.load(open(
            'tests/engine/production/test_hourly_precision/config/EW_US_STOCK_better_main_only.json')),
            strategy_pool_generator(pool_setting, save=False))

        quant_engine.load_app(OrderRouter())
        quant_engine.load_data()

        quant_engine.start_production_engine()
        quant_engine.start_production_engine_other()

        quant_engine.end_production_engine()
        quant_engine.wait_production_engine()

        quant_engine.end_production_engine_other()
        quant_engine.wait_production_engine_other()
        
        path_name = os.path.join("tests/engine/production/test_hourly_precision/result",
                                 f"EW_US_STOCK_better_main_only_lv1.hdf5")
        
        reader = ResultReader(path_name)

        final_pnl = reader.get_strategy(id_list=[0], fields=["pnl"])[0][0].iloc[-1, 0]
        #reader.to_csv(export_folder = 'data_hourly/us_stock_prod_better_main_only', id_list = [0])
        assert np.isclose(final_pnl, -19411.57)


    def test_EW_FX_lv1(self):
        # Day cut off at 00:00 UTC. No skipping timestamp at this time.
        """ pool_setting = {"symbol": {"FX": ['AUDCAD', 'AUDJPY', 'EURUSD', 'USDJPY', 'USDKRW']},
                        "strategy_module": "tests.engine.backtest.test_hourly_precision.algorithm.EW_lv1",
                        "strategy_name": "EW_strategy",
                        "params": {
                            # EW has no parameters
                        }}

        run_once('tests/engine/backtest/test_hourly_precision/config/EW_FX.json',
                 strategy_pool_generator(pool_setting, save=False))
        reader = ResultReader(r'tests/engine/backtest/test_hourly_precision/result/EW_FX_lv1.hdf5')
        reader.to_csv(export_folder = 'data_hourly/EW_FX_lv1_backtest_3', id_list = [0])
        final_pnl = reader.get_strategy(id_list=[0], fields=["pnl"])[0][0].iloc[-1, 0] """


        pool_setting = {"symbol": {"FX": ['AUDCAD', 'AUDJPY', 'EURUSD', 'USDJPY', 'USDKRW']},
                        "strategy_module": "tests.engine.production.test_hourly_precision.algorithm.EW_lv1",
                        "strategy_name": "EW_strategy",
                        "params": {
                            # EW has no parameters
                        }}
        quant_engine = QuantEngine()
        quant_engine.load_config(json.load(open(
            'tests/engine/production/test_hourly_precision/config/EW_FX.json')),
            strategy_pool_generator(pool_setting, save=False))

        quant_engine.load_app(OrderRouter())
        quant_engine.load_data()

        quant_engine.start_production_engine()
        quant_engine.start_production_engine_other()

        quant_engine.end_production_engine()
        quant_engine.wait_production_engine()

        quant_engine.end_production_engine_other()
        quant_engine.wait_production_engine_other()

        path_name = os.path.join("tests/engine/production/test_hourly_precision/result",
                                 f"EW_FX_lv1.hdf5")
        reader = ResultReader(path_name)
        #reader.to_csv(export_folder = 'data_hourly/EW_FX_lv1_prod_3', id_list = [0])
        final_pnl = reader.get_strategy(id_list=[0], fields=["pnl"])[0][0].iloc[-1, 0]


        assert np.isclose(final_pnl, -20920.67277566385)

    def test_RSI_FX_lv1(self):
        # Day cut off at 16:00 UTC. No skipping timestamp at this time.
        pool_setting = {"symbol": {"FX": ['AUDCAD', 'AUDJPY', 'EURUSD', 'USDJPY', 'USDKRW']},
                        "strategy_module": "tests.engine.production.test_hourly_precision.algorithm.RSI_lv1",
                        "strategy_name": "RSI_strategy",
                        "params": {
                            "length": [40],
                            "break_threshold": [10],
                            "stop_profit": [0.01],
                            "stop_loss": [0.005],
                            "max_hold_days": [10]
                        }}
        quant_engine = QuantEngine()
        quant_engine.load_config(json.load(open(
            'tests/engine/production/test_hourly_precision/config/RSI_FX.json')),
            strategy_pool_generator(pool_setting, save=False))

        quant_engine.load_app(OrderRouter())
        quant_engine.load_data()

        quant_engine.start_production_engine()
        quant_engine.start_production_engine_other()

        quant_engine.end_production_engine()
        quant_engine.wait_production_engine()

        quant_engine.end_production_engine_other()
        quant_engine.wait_production_engine_other()

        path_name = os.path.join("tests/engine/production/test_hourly_precision/result",
                                 f"RSI_FX_lv1.hdf5")
        reader = ResultReader(path_name)

        pnl = reader.get_strategy(id_list=[0], fields=["pnl"])[0][0].iloc[-1, 0]

        assert np.isclose(pnl, 3099.5186)
