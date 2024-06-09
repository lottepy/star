import numpy as np
import unittest

from quantcycle.utils.strategy_pool_generator import strategy_pool_generator
from quantcycle.app.result_exporter.result_reader import ResultReader
from quantcycle.utils.run_once import run_once


class TestBacktestEngine(unittest.TestCase):
    def test_EW_FX_lv1(self):
        # Day cut off at 16:00 UTC. No skipping timestamp at this time.
        pool_setting = {"symbol": {"FX": ['AUDCAD', 'AUDJPY', 'EURUSD', 'USDJPY', 'USDKRW']},
                        "strategy_module": "tests.engine.backtest.test_hourly_precision.algorithm.EW_lv1",
                        "strategy_name": "EW_strategy",
                        "params": {
                            # EW has no parameters
                        }}

        run_once('tests/engine/backtest/test_hourly_precision/config/EW_FX.json',
                 strategy_pool_generator(pool_setting, save=False))
        reader = ResultReader(r'tests/engine/backtest/test_hourly_precision/result/EW_FX_lv1.hdf5')
        final_pnl = reader.get_strategy(id_list=[0], fields=["pnl"])[0][0].iloc[-1, 0]
        assert np.isclose(final_pnl, -20920.67)


    def test_RSI_FX_lv1(self):
        # Day cut off at 16:00 UTC. No skipping timestamp at this time.
        pool_setting = {"symbol": {"FX": ['AUDCAD', 'AUDJPY', 'EURUSD', 'USDJPY', 'USDKRW']},
                        "strategy_module": "tests.engine.backtest.test_hourly_precision.algorithm.RSI_lv1",
                        "strategy_name": "RSI_strategy",
                        "params": {
                            "length": [10, 15, 20, 40],
                            "break_threshold": [5, 10, 15, 20, 25, 30],
                            "stop_profit": [0.01, 0.02],
                            "stop_loss": [0.005, 0.01, 0.02],
                            "max_hold_days": [10, 20]
                        }}

        run_once('tests/engine/backtest/test_hourly_precision/config/RSI_FX.json',
                 strategy_pool_generator(pool_setting, save=False))
        reader = ResultReader(r'tests/engine/backtest/test_hourly_precision/result/RSI_lv1.hdf5')
        pnl_sum = reader.get_strategy_3d(id_list=range(288), field='pnl')[0][-1].sum()
        print(pnl_sum)
        assert np.isclose(pnl_sum, -10323163.10)


    # def test_KD_FX_lv1(self):
    #     # Day cut off at 16:00 UTC. No skipping timestamp at this time.
    #     pool_setting = {"symbol": {"FX": ['AUDCAD', 'AUDJPY', 'EURUSD', 'USDJPY', 'USDKRW']},
    #                     "strategy_module": "tests.engine.backtest.test_hourly_precision.algorithm.KD_lv1",
    #                     "strategy_name": "KD_strategy",
    #                     "params": {
    #                         "length1": [10, 15, 20, 40],
    #                         "length2": [3],
    #                         "length3": [3],
    #                         "break_threshold": [5, 10, 15, 20, 25],
    #                         "stop_profit": [0.01, 0.02],
    #                         "stop_loss": [0.005, 0.01, 0.02],
    #                         "max_hold_days": [10, 20]
    #                     }}
    #
    #     run_once('tests/engine/backtest/test_hourly_precision/config/KD_FX.json',
    #              strategy_pool_generator(pool_setting, save=False))
    #     reader = ResultReader(r'tests/engine/backtest/test_hourly_precision/result/KD_lv1.hdf5')
    #     final_pnl = reader.get_strategy(id_list=[0], fields=["pnl"])[0][0].iloc[-1, 0]
    #     assert True     #! Dropped due to unreliable v21 results


    def test_EW_US_STOCK_lv1(self):
        # Day cut off at 16:00 UTC. No skipping timestamp at this time.
        # HK stocks are the same as US stocks
        pool_setting = {"symbol": {"STOCKS": ['STOCK_1', 'STOCK_2', 'STOCK_3', 'STOCK_4', 'STOCK_5']},
                        "strategy_module": "tests.engine.backtest.test_hourly_precision.algorithm.EW_lv1",
                        "strategy_name": "EW_strategy",
                        "params": {
                                      # EW has no parameters
                                  }}

        run_once('tests/engine/backtest/test_hourly_precision/config/EW_US_STOCK.json',
                 strategy_pool_generator(pool_setting, save=False))
        reader = ResultReader(r'tests/engine/backtest/test_hourly_precision/result/EW_US_STOCK_lv1.hdf5')
        final_pnl = reader.get_strategy(id_list=[0], fields=["pnl"])[0][0].iloc[-1, 0]
        assert np.isclose(final_pnl, -21797.60)


    def test_EW_CN_STOCK_lv1(self):
        # Calc rate at 16:00 UTC. No skipping timestamp at this time.
        pool_setting = {"symbol": {"STOCKS": ['STOCK_1', 'STOCK_2', 'STOCK_3', 'STOCK_4', 'STOCK_5']},
                        "strategy_module": "tests.engine.backtest.test_hourly_precision.algorithm.EW_lv1",
                        "strategy_name": "EW_strategy",
                        "params": {
                                      # EW has no parameters
                                  }}

        run_once('tests/engine/backtest/test_hourly_precision/config/EW_CN_STOCK.json',
                 strategy_pool_generator(pool_setting, save=False))
        reader = ResultReader(r'tests/engine/backtest/test_hourly_precision/result/EW_CN_STOCK_lv1.hdf5')
        final_pnl = reader.get_strategy(id_list=[0], fields=["pnl"])[0][0].iloc[-1, 0]
        assert np.isclose(final_pnl, -21787)


    def test_EW_US_STOCK_better_timestamps(self):
        # Day cut off at 16:00 UTC. No skipping timestamp at this time.
        # HK stocks are the same as US stocks
        pool_setting = {"symbol": {"STOCKS": ['STOCK_1', 'STOCK_2', 'STOCK_3', 'STOCK_4', 'STOCK_5']},
                        "strategy_module": "tests.engine.backtest.test_hourly_precision.algorithm.EW_lv1",
                        "strategy_name": "EW_strategy",
                        "params": {
                                      # EW has no parameters
                                  }}

        run_once('tests/engine/backtest/test_hourly_precision/config/EW_US_STOCK_better.json',
                 strategy_pool_generator(pool_setting, save=False))
        reader = ResultReader(r'tests/engine/backtest/test_hourly_precision/result/EW_US_STOCK_better_lv1.hdf5')
        final_pnl = reader.get_strategy(id_list=[0], fields=["pnl"])[0][0].iloc[-1, 0]
        assert np.isclose(final_pnl, -19411.57)


    def test_EW_US_STOCK_better_main_only(self):
        # Day cut off at 16:00 UTC. No skipping timestamp at this time.
        # HK stocks are the same as US stocks
        pool_setting = {"symbol": {"STOCKS": ['STOCK_1', 'STOCK_2', 'STOCK_3', 'STOCK_4', 'STOCK_5']},
                        "strategy_module": "tests.engine.backtest.test_hourly_precision.algorithm.EW_lv1",
                        "strategy_name": "EW_strategy",
                        "params": {
                                      # EW has no parameters
                                  }}

        run_once('tests/engine/backtest/test_hourly_precision/config/EW_US_STOCK_better_main_only.json',
                 strategy_pool_generator(pool_setting, save=False))
        reader = ResultReader(r'tests/engine/backtest/test_hourly_precision/result/EW_US_STOCK_better_main_only_lv1.hdf5')
        final_pnl = reader.get_strategy(id_list=[0], fields=["pnl"])[0][0].iloc[-1, 0]
        assert np.isclose(final_pnl, -19411.57)
