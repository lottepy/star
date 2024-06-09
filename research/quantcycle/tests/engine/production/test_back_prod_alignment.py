"""
[ Data alignment tests ]
    This test group aims to verify the data fields that on_data() access are correctly
    aligned across productioning engine and live trading engine (production engine).

"""
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

class ProdBackTest(unittest.TestCase):
    @unittest.skip('Need revision after multi-assets.')
    def test_type_and_shape(self):
        """
        This test aims to verify data types and dimensions of
        - main data
        - secondary data
        - reference data
        - local CSV
        - signal remark
        :return: None
        """
        pool_setting_lv1 = {
            "symbol": {
                "STOCKS": ['600016.SH', '000005.SZ', '601398.SH', '600028.SH', '000100.SZ',
                        '600000.SH', '002415.SZ', '000917.SZ']
            },
            "strategy_module": "tests.engine.production.test_back_prod.algorithm.RSI_type_dim_lv1",
            "strategy_name": "RSI_strategy",
            "params": {
                "length": [10],
                "break_threshold": [5],
                "stop_profit": [0.01],
                "stop_loss": [0.005],
                "max_hold_days": [10]
            }
        }

        pool_setting_lv2 = {
            "symbol": {
                "STOCKS": ['600016.SH', '000005.SZ', '601398.SH', '600028.SH', '000100.SZ',
                        '600000.SH', '002415.SZ', '000917.SZ'],
                "RSI": list(np.arange(8).astype(np.float64)) # orginal is 3*8
            },
            "strategy_module": "tests.engine.production.test_back_prod.algorithm.RSI_type_dim_lv2",
            "strategy_name": "Allocation_strategy",
            "params": {
                # No parameters
            }
        }

        engine_manager = EngineManager()

        engine_manager.add_engine(json.load(open(r'tests/engine/production/test_back_prod/config/RSI_type_dim_lv1.json')),strategy_pool_generator(pool_setting_lv1, save=False))
        engine_manager.add_engine(json.load(open(r'tests/engine/production/test_back_prod/config/RSI_type_dim_lv2.json')),strategy_pool_generator(pool_setting_lv2, save=False))

        engine_manager.start_engine()
        engine_manager.run()
        load_data_event = engine_manager.load_engine_data()
        load_data_event.wait()

        timepoint = "20190111"
        engine_manager.handle_current_fx_data(timepoint)
        engine_manager.handle_ref_data(start_timepoint=timepoint,end_timepoint=timepoint)
        handle_current_data_event,_ = engine_manager.handle_current_data(timepoint)
        handle_current_data_event.wait()
        engine_manager.handle_current_rate_data(timepoint)

        timepoint = "20190114"
        engine_manager.handle_current_fx_data(timepoint)
        engine_manager.handle_ref_data(start_timepoint=timepoint,end_timepoint=timepoint)
        handle_current_data_event,_ = engine_manager.handle_current_data(timepoint)
        handle_current_data_event.wait()
        engine_manager.handle_current_rate_data(timepoint)

        get_logger.get_logger().info("finish handle data")

        engine_manager.kill_engine()
        engine_manager.wait_engine()
        engine_manager.kill()
        engine_manager.wait()

        df = pd.read_csv(r'tests/engine/production/test_back_prod/remarks/remark_type_dim_lv1-0.csv')
        df_sum_rows = df.sum(axis=0, numeric_only=True)
        assert np.isclose(sum(df_sum_rows), 1*5*5)

        df = pd.read_csv(r'tests/engine/production/test_back_prod/remarks/remark_type_dim_lv2-0.csv')
        df_sum_rows = df.sum(axis=0, numeric_only=True)
        assert np.isclose(sum(df_sum_rows), 2*2*5)

    @unittest.skip('Need revision after multi-assets.')
    def test_data(self):
        """
        This test aims to verify data numbers of
        - main data
        - secondary data
        - reference data
        - local CSV
        - signal remark
        :return: None
        """
        self.__run_data_lv1()
        #self.__run_data_lv2()
        
        # df = pd.read_csv(r'tests/engine/production/test_back_prod/remarks/remark_data_lv2-0.csv')
        # df_sum_rows = df.sum(axis=0, numeric_only=True)
        # assert np.isclose(sum(df_sum_rows), 14.0)

    def __run_data_lv1(self):
        pool_setting = {
            "symbol": {
                "STOCKS": ['600016.SH', '000005.SZ', '601398.SH', '600028.SH', '000100.SZ',
                        '600000.SH', '002415.SZ', '000917.SZ']
            },
            "strategy_module": "tests.engine.production.test_back_prod.algorithm.RSI_data_lv1",
            "strategy_name": "RSI_strategy",
            "params": {
                "length": [10],
                "break_threshold": [5],
                "stop_profit": [0.01],
                "stop_loss": [0.005],
                "max_hold_days": [10]
            }
        }
        quant_engine = QuantEngine()
        quant_engine.load_config(json.load(open(
            'tests/engine/production/test_back_prod/config/RSI_data_lv1.json')),
            strategy_pool_generator(pool_setting, save=False))

        quant_engine.load_app(OrderRouter())
        quant_engine.load_data()

        quant_engine.start_production_engine()
        quant_engine.start_production_engine_other()

        quant_engine.end_production_engine()
        quant_engine.wait_production_engine()

        quant_engine.end_production_engine_other()
        quant_engine.wait_production_engine_other()

        df = pd.read_csv(r'tests/engine/production/test_back_prod/remarks/remark_data_lv1-0.csv')
        df_sum_rows = df.sum(axis=0, numeric_only=True)
        assert np.isclose(sum(df_sum_rows), 6.0)


    def __run_data_lv2(self):
        pool_setting = {
            "symbol": {
                "STOCKS": ['600016.SH', '000005.SZ', '601398.SH', '600028.SH', '000100.SZ',
                        '600000.SH', '002415.SZ', '000917.SZ'],
                "RSI": list(np.arange(3 * 8).astype(np.float64))
            },
            "strategy_module": "tests.engine.production.test_back_prod.algorithm.RSI_data_lv2",
            "strategy_name": "Allocation_strategy",
            "params": {
                # No parameters
            }
        }
        quant_engine = QuantEngine()
        quant_engine.load_config(json.load(open(
            'tests/engine/production/test_back_prod/config/RSI_data_lv2.json')),
            strategy_pool_generator(pool_setting, save=False))

        quant_engine.load_app(OrderRouter())
        quant_engine.load_data()

        quant_engine.start_production_engine()
        quant_engine.start_production_engine_other()

        quant_engine.end_production_engine()
        quant_engine.wait_production_engine()

        quant_engine.end_production_engine_other()
        quant_engine.wait_production_engine_other()

    @unittest.skip('Need revision after multi-assets.')
    def test_FX_type_shape_data(self):
        """
        This test aims to verify data types, dimensions and numbers of
        - main data
            - fxrate
            - int
        :return: None
        """
        
        self.__run_fx_lv1()
        #self.__run_fx_lv2()
        
        df = pd.read_csv(r'tests/engine/production/test_back_prod/remarks/remark_fx_lv1-0.csv')
        df_sum_rows = df.sum(axis=0, numeric_only=True)
        assert np.isclose(sum(df_sum_rows), 8.0)
        
        # df = pd.read_csv(r'tests/engine/production/test_back_prod/remarks/remark_fx_lv2-0.csv')
        # df_sum_rows = df.sum(axis=0, numeric_only=True)
        # assert np.isclose(sum(df_sum_rows), 14.0)

    def __run_fx_lv1(self):
        pool_setting = {
            "symbol": {
                "FX": ['AUDCAD', 'AUDJPY', 'EURUSD', 'USDJPY']
            },
            "strategy_module": "tests.engine.production.test_back_prod.algorithm.RSI_fx_lv1",
            "strategy_name": "RSI_strategy",
            "params": {
                "length": [10],
                "break_threshold": [5],
                "stop_profit": [0.01],
                "stop_loss": [0.005],
                "max_hold_days": [10]
            }
        }

        quant_engine = QuantEngine()
        quant_engine.load_config(json.load(open(
            'tests/engine/production/test_back_prod/config/RSI_fx_lv1.json')),
            strategy_pool_generator(pool_setting, save=False))

        quant_engine.load_app(OrderRouter())
        quant_engine.load_data()

        quant_engine.start_production_engine()
        quant_engine.start_production_engine_other()

        quant_engine.end_production_engine()
        quant_engine.wait_production_engine()

        quant_engine.end_production_engine_other()
        quant_engine.wait_production_engine_other()


    def __run_fx_lv2(self):
        pool_setting = {
            "symbol": {
                "FX": ['AUDCAD', 'AUDJPY', 'EURUSD', 'USDJPY'],
                "RSI": list(np.arange(3 * 8).astype(np.float64))
            },
            "strategy_module": "tests.engine.production.test_back_prod.algorithm.RSI_fx_lv2",
            "strategy_name": "Allocation_strategy",
            "params": {
                # No parameters
            }
        }
        quant_engine = QuantEngine()
        quant_engine.load_config(json.load(open(
            'tests/engine/production/test_back_prod/config/RSI_fx_lv2.json')),
            strategy_pool_generator(pool_setting, save=False))

        quant_engine.load_app(OrderRouter())
        quant_engine.load_data()

        quant_engine.start_production_engine()
        quant_engine.start_production_engine_other()

        quant_engine.end_production_engine()
        quant_engine.wait_production_engine()

        quant_engine.end_production_engine_other()
        quant_engine.wait_production_engine_other()

    
