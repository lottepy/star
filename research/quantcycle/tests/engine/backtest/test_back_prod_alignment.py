"""
[ Data alignment tests ]
    This test group aims to verify the data fields that on_data() access are correctly
    aligned across backtesting engine and live trading engine (production engine).

"""
import unittest
import numpy as np
import pandas as pd
from quantcycle.utils.strategy_pool_generator import strategy_pool_generator
from quantcycle.utils.run_once import run_once

class BackProdTest(unittest.TestCase):
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
        self.__run_type_and_shape_lv1()
        self.__run_type_and_shape_lv2()
        
        df = pd.read_csv(r'tests/engine/backtest/test_back_prod/remarks/remark_type_dim_lv1-0.csv')
        df_sum_rows = df.sum(axis=0, numeric_only=True)
        assert np.isclose(sum(df_sum_rows), 5.0)
        
        df = pd.read_csv(r'tests/engine/backtest/test_back_prod/remarks/remark_type_dim_lv2-0.csv')
        df_sum_rows = df.sum(axis=0, numeric_only=True)
        assert np.isclose(sum(df_sum_rows), 11.0)

    def __run_type_and_shape_lv1(self):
        pool_setting = {
            "symbol": {
                "STOCKS": ['600016.SH', '000005.SZ', '601398.SH', '600028.SH', '000100.SZ',
                           '600000.SH', '002415.SZ', '000917.SZ']
            },
            "strategy_module": "tests.engine.backtest.test_back_prod.algorithm.RSI_type_dim_lv1",
            "strategy_name": "RSI_strategy",
            "params": {
                "length": [10, 15, 20, 40],
                "break_threshold": [5, 10, 15, 20],
                "stop_profit": [0.01, 0.02],
                "stop_loss": [0.005, 0.01, 0.02],
                "max_hold_days": [10]
            }
        }
        run_once(r'tests/engine/backtest/test_back_prod/config/RSI_type_dim_lv1.json',
                 strategy_pool_generator(pool_setting, save=False))

    def __run_type_and_shape_lv2(self):
        pool_setting = {
            "symbol": {
                "STOCKS": ['600016.SH', '000005.SZ', '601398.SH', '600028.SH', '000100.SZ',
                        '600000.SH', '002415.SZ', '000917.SZ'],
                "RSI": list(np.arange(96 * 8).astype(np.float64))
            },
            "strategy_module": "tests.engine.backtest.test_back_prod.algorithm.RSI_type_dim_lv2",
            "strategy_name": "Allocation_strategy",
            "params": {
                # No parameters
            }
        }
        run_once(r'tests/engine/backtest/test_back_prod/config/RSI_type_dim_lv2.json',
                 strategy_pool_generator(pool_setting, save=False))


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
        self.__run_data_lv2()
        
        df = pd.read_csv(r'tests/engine/backtest/test_back_prod/remarks/remark_data_lv1-0.csv')
        df_sum_rows = df.sum(axis=0, numeric_only=True)
        assert np.isclose(sum(df_sum_rows), 6.0)
        
        df = pd.read_csv(r'tests/engine/backtest/test_back_prod/remarks/remark_data_lv2-0.csv')
        df_sum_rows = df.sum(axis=0, numeric_only=True)
        assert np.isclose(sum(df_sum_rows), 14.0)

    def __run_data_lv1(self):
        pool_setting = {
            "symbol": {
                "STOCKS": ['600016.SH', '000005.SZ', '601398.SH', '600028.SH', '000100.SZ',
                           '600000.SH', '002415.SZ', '000917.SZ']
            },
            "strategy_module": "tests.engine.backtest.test_back_prod.algorithm.RSI_data_lv1",
            "strategy_name": "RSI_strategy",
            "params": {
                "length": [10, 15, 20, 40],
                "break_threshold": [5, 10, 15, 20],
                "stop_profit": [0.01, 0.02],
                "stop_loss": [0.005, 0.01, 0.02],
                "max_hold_days": [10]
            }
        }
        run_once(r'tests/engine/backtest/test_back_prod/config/RSI_data_lv1.json',
                 strategy_pool_generator(pool_setting, save=False))

    def __run_data_lv2(self):
        pool_setting = {
            "symbol": {
                "STOCKS": ['600016.SH', '000005.SZ', '601398.SH', '600028.SH', '000100.SZ',
                        '600000.SH', '002415.SZ', '000917.SZ'],
                "RSI": list(np.arange(96 * 8).astype(np.float64))
            },
            "strategy_module": "tests.engine.backtest.test_back_prod.algorithm.RSI_data_lv2",
            "strategy_name": "Allocation_strategy",
            "params": {
                # No parameters
            }
        }
        run_once(r'tests/engine/backtest/test_back_prod/config/RSI_data_lv2.json',
                 strategy_pool_generator(pool_setting, save=False))


    def test_data_datamaster(self):
        """
        This test pulls data from DataMaster, aiming to verify data values of
        - main data
        - secondary data
        - reference data
        - local CSV
        - signal remark
        :return: None
        """
        self.__run_data_lv1_datamaster()
        self.__run_data_lv2_datamaster()

        df = pd.read_csv(r'tests/engine/backtest/test_back_prod/remarks/remark_data_lv1_dm-0.csv')
        df_sum_rows = df.sum(axis=0, numeric_only=True)
        assert np.isclose(sum(df_sum_rows), 6.0)

        df = pd.read_csv(r'tests/engine/backtest/test_back_prod/remarks/remark_data_lv2_dm-0.csv')
        df_sum_rows = df.sum(axis=0, numeric_only=True)
        assert np.isclose(sum(df_sum_rows), 14.0)

    def __run_data_lv1_datamaster(self):
        pool_setting = {
            "symbol": {
                "STOCKS": ['600016.SH', '000005.SZ', '601398.SH', '600028.SH', '000100.SZ',
                           '600000.SH', '002415.SZ', '000917.SZ']
            },
            "strategy_module": "tests.engine.backtest.test_back_prod.algorithm.RSI_data_lv1",
            "strategy_name": "RSI_strategy",
            "params": {
                "length": [10, 15, 20, 40],
                "break_threshold": [5, 10, 15, 20],
                "stop_profit": [0.01, 0.02],
                "stop_loss": [0.005, 0.01, 0.02],
                "max_hold_days": [10]
            }
        }
        run_once(r'tests/engine/backtest/test_back_prod/config/RSI_data_lv1_dm.json',
                 strategy_pool_generator(pool_setting, save=False))

    def __run_data_lv2_datamaster(self):
        pool_setting = {
            "symbol": {
                "STOCKS": ['600016.SH', '000005.SZ', '601398.SH', '600028.SH', '000100.SZ',
                           '600000.SH', '002415.SZ', '000917.SZ'],
                "RSI": list(np.arange(96 * 8).astype(np.float64))
            },
            "strategy_module": "tests.engine.backtest.test_back_prod.algorithm.RSI_data_lv2",
            "strategy_name": "Allocation_strategy",
            "params": {
                # No parameters
            }
        }
        run_once(r'tests/engine/backtest/test_back_prod/config/RSI_data_lv2_dm.json',
                 strategy_pool_generator(pool_setting, save=False))


    def test_FX_type_shape_data(self):
        """
        This test aims to verify data types, dimensions and numbers of
        - main data
            - fxrate
            - int
        :return: None
        """
        
        self.__run_fx_lv1()
        self.__run_fx_lv2()
        
        df = pd.read_csv(r'tests/engine/backtest/test_back_prod/remarks/remark_fx_lv1-0.csv')
        df_sum_rows = df.sum(axis=0, numeric_only=True)
        assert np.isclose(sum(df_sum_rows), 8.0)
        
        df = pd.read_csv(r'tests/engine/backtest/test_back_prod/remarks/remark_fx_lv2-0.csv')
        df_sum_rows = df.sum(axis=0, numeric_only=True)
        assert np.isclose(sum(df_sum_rows), 15.0)

    def __run_fx_lv1(self):
        pool_setting = {
            "symbol": {
                "FX": ['AUDCAD', 'AUDJPY', 'EURUSD', 'USDJPY']
            },
            "strategy_module": "tests.engine.backtest.test_back_prod.algorithm.RSI_fx_lv1",
            "strategy_name": "RSI_strategy",
            "params": {
                "length": [10, 15, 20, 40],
                "break_threshold": [5, 10, 15, 20],
                "stop_profit": [0.01, 0.02],
                "stop_loss": [0.005, 0.01, 0.02],
                "max_hold_days": [10]
            }
        }
        run_once(r'tests/engine/backtest/test_back_prod/config/RSI_fx_lv1.json',
                 strategy_pool_generator(pool_setting, save=False))

    def __run_fx_lv2(self):
        pool_setting = {
            "symbol": {
                "FX": ['AUDCAD', 'AUDJPY', 'EURUSD', 'USDJPY'],
                "RSI": list(np.arange(96 * 4).astype(np.float64))
            },
            "strategy_module": "tests.engine.backtest.test_back_prod.algorithm.RSI_fx_lv2",
            "strategy_name": "Allocation_strategy",
            "params": {
                # No parameters
            }
        }
        run_once(r'tests/engine/backtest/test_back_prod/config/RSI_fx_lv2.json',
                 strategy_pool_generator(pool_setting, save=False))
