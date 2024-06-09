'''
This test aims to check the precision of 9 indicators strategies. 
All configs, algorithms are under the path: tests/engine/backtest/test_precision_complete

    LV1: RSI, KD, CCI, MACD_P, Boll, single_MA, single_EMA, double_MA, Donchian Channel
    LV2: Allocation_method on RSI, KD LV1 results
    LV3: Combination Strategy using RSI, KD LV2 results
'''

from unittest import TestCase

from quantcycle.utils.strategy_pool_generator import strategy_pool_generator
from quantcycle.app.result_exporter.result_reader import ResultReader
from quantcycle.utils.run_once import run_once
import numpy.testing as npt
import numpy as np
import json

from quantcycle.engine.backtest_engine import BacktestEngine
from quantcycle.app.data_manager import DataDistributorSub
from datetime import datetime


def generate_rsi_lv1_strategy_pool():
    # symbol_list = list(pd.read_csv("tests/engine/backtest/test_RSI_lv1/stock_pool/aqm_turnover_union_fx_noZAR.csv")['SECUCODE'])
    pool_setting = {"symbol":{"FX":['EURUSD', 'USDJPY', 'NZDJPY', 'NOKSEK', 'GBPJPY', 'CADJPY', 'CADCHF', 'AUDNZD', 'AUDJPY', 'AUDCAD', 'USDSEK', 'USDNOK', 'EURCHF', 'USDCHF', 'USDCAD', 'NZDUSD', 'GBPUSD', 'AUDUSD', 'USDSGD', 'USDTHB', 'GBPCAD', 'EURCAD', 'EURJPY', 'EURGBP', 'GBPCHF', 'USDINR', 'USDIDR', 'USDTWD', 'USDKRW']},
                    "strategy_module":"tests.engine.backtest.test_precision_complete.algorithm.simple_oscillator_strategy",
                    "strategy_name":"RSI_strategy",
                    "save_path":"tests/engine/backtest/test_precision_complete/strategy_pool/RSI_lv1_strategy_pool.csv",
                    "params":{
                                "length" : [10,15,20,40],
                                "break_threshold" :[5,10,15,20,25,30],
                                "stop_profit" : [0.01,0.02],
                                "stop_loss" :[0.005,0.01,0.02],
                                "max_hold_days" : [10,20]
                             }}
    strategy_pool_generator(pool_setting)

def generate_KD_lv1_strategy_pool():
    # symbol_list = list(pd.read_csv("tests/engine/backtest/test_RSI_lv1/stock_pool/aqm_turnover_union_fx_noZAR.csv")['SECUCODE'])
    pool_setting = {"symbol":{"FX":['EURUSD', 'USDJPY', 'NZDJPY', 'NOKSEK', 'GBPJPY', 'CADJPY', 'CADCHF', 'AUDNZD', 'AUDJPY', 'AUDCAD', 'USDSEK', 'USDNOK', 'EURCHF', 'USDCHF', 'USDCAD', 'NZDUSD', 'GBPUSD', 'AUDUSD', 'USDSGD', 'USDTHB', 'GBPCAD', 'EURCAD', 'EURJPY', 'EURGBP', 'GBPCHF', 'USDINR', 'USDIDR', 'USDTWD', 'USDKRW']},
                    "strategy_module":"tests.engine.backtest.test_precision_complete.algorithm.simple_oscillator_strategy",
                    "strategy_name":"KD_strategy",
                    "save_path":"tests/engine/backtest/test_precision_complete/strategy_pool/KD_lv1_strategy_pool.csv",
                    "params":{
                                "length1" : [10,15,20,40],
                                "length2" : [3],
                                "length3" : [3],
                                "break_threshold" :[5,10,15,20,25],
                                "stop_profit" : [0.01,0.02],
                                "stop_loss" :[0.005,0.01,0.02],
                                "max_hold_days" : [10,20]
                             }}
    strategy_pool_generator(pool_setting)

class TestFullLevel1PrecisionTestCase(TestCase):
    def test_RSI_LV1_precision(self):
        generate_rsi_lv1_strategy_pool()
        run_once('tests/engine/backtest/test_precision_complete/config/RSI_lv1.json')
        resultreader=ResultReader("tests/engine/backtest/test_precision_complete/result/RSI_lv1.hdf5")
        output = resultreader.get_strategy_3d(id_list=range(20000),field='pnl')
        total_pnl = output[0][-1].sum()
        self.assertEqual(round(total_pnl, 5), 1600356.88574)

    def test_KD_LV1_precision(self):
        generate_KD_lv1_strategy_pool()
        run_once('tests/engine/backtest/test_precision_complete/config/KD_lv1.json')
        resultreader=ResultReader("tests/engine/backtest/test_precision_complete/result/KD_lv1.hdf5")
        output = resultreader.get_strategy_3d(id_list=range(20000),field='pnl')
        total_pnl = output[0][-1].sum()
        self.assertEqual(round(total_pnl, 5), -15760648.96986)

def generate_rsi_allocation_strategy_pool():
    pool_setting = {"symbol":{"FX":['EURUSD', 'USDJPY', 'NZDJPY', 'NOKSEK', 'GBPJPY', 'CADJPY', 'CADCHF', 'AUDNZD', 'AUDJPY', 'AUDCAD', 'USDSEK', 'USDNOK', 'EURCHF', 'USDCHF', 'USDCAD', 'NZDUSD', 'GBPUSD', 'AUDUSD', 'USDSGD', 'USDTHB', 'GBPCAD', 'EURCAD', 'EURJPY', 'EURGBP', 'GBPCHF', 'USDINR', 'USDIDR', 'USDTWD', 'USDKRW'],
                              "RSI": list(np.arange(288*29).astype(np.float64))}, #不转成float json会报错
                    "strategy_module":"tests.engine.backtest.test_precision_complete.algorithm.RSI_lv2",
                    "strategy_name":"Allocation_strategy",
                    "save_path":"tests/engine/backtest/test_precision_complete/strategy_pool/RSI_lv2_strategy_pool.csv",
                    "params":{

                             }}
    strategy_pool_generator(pool_setting)

def generate_KD_allocation_strategy_pool():
    pool_setting = {"symbol":{"FX":['EURUSD', 'USDJPY', 'NZDJPY', 'NOKSEK', 'GBPJPY', 'CADJPY', 'CADCHF', 'AUDNZD', 'AUDJPY', 'AUDCAD', 'USDSEK', 'USDNOK', 'EURCHF', 'USDCHF', 'USDCAD', 'NZDUSD', 'GBPUSD', 'AUDUSD', 'USDSGD', 'USDTHB', 'GBPCAD', 'EURCAD', 'EURJPY', 'EURGBP', 'GBPCHF', 'USDINR', 'USDIDR', 'USDTWD', 'USDKRW'],
                              "KD": list(np.arange(240*29).astype(np.float64))}, #不转成float json会报错
                    "strategy_module":"tests.engine.backtest.test_precision_complete.algorithm.KD_lv2",
                    "strategy_name":"Allocation_strategy",
                    "save_path":"tests/engine/backtest/test_precision_complete/strategy_pool/KD_lv2_strategy_pool.csv",
                    "params":{

                             }}
    strategy_pool_generator(pool_setting)


class TestFullLevel2PrecisionTestCase(TestCase):
    def test_RSI_LV2_precision(self):
        generate_rsi_allocation_strategy_pool()
        run_once('tests/engine/backtest/test_precision_complete/config/RSI_lv2.json')
        resultreader=ResultReader("tests/engine/backtest/test_precision_complete/result/RSI_lv2.hdf5")
        output = resultreader.get_strategy_3d(id_list=range(20000),field='pnl')
        total_pnl = output[0][-1].sum()/100000
        self.assertEqual(round(total_pnl, 10), 0.0448145006)

    def test_KD_LV2_precision(self):
        generate_KD_allocation_strategy_pool()

        ts0 = datetime.now()
        backtest_engine = BacktestEngine()
        backtest_engine.load_config(json.load(open('tests/engine/backtest/test_precision_complete/config/KD_lv2.json')))
        ts = datetime.now()
        backtest_engine.prepare()
       #  data = backtest_engine.data_manager.data_distributor_main.data_package
       #  data_distributor_sub = DataDistributorSub()
       #  data = data_distributor_sub.unpack_data(data)
       #
       #  npt.assert_almost_equal(data['KD-Strat/pnl']['data_arr'][-1,0], -16400.25325, decimal=5)
       #  npt.assert_almost_equal(data['KD-Strat/position']['data_arr'][-1,0], -150169.15857, decimal=5)
       #  npt.assert_almost_equal(data['KD-Strat/metrics_61']['data_arr'][-1,0], [-3.49258e-02, -2.11610e+00,  4.72410e-02,  1.46204e-02,
       # -1.44284e-01,  7.00000e+00, -2.62601e-02, -1.29688e-02,
       #  4.28571e-01,  4.28571e-01,  4.80000e-01,  1.88484e-03,
       # -3.78760e-03,  3.39833e-03, -5.82352e-03], decimal=5)
       #  npt.assert_almost_equal(data['KD-Strat/metrics_252']['data_arr'][-1,0], [ 3.89174e-02,  7.91660e-01,  4.72410e-02,  7.23640e-03,
       #  3.89174e-02,  3.20000e+01,  1.71553e-02,  4.86453e-02,
       #  5.31250e-01,  7.18750e-01,  5.81967e-01,  4.21700e-03,
       # -3.46560e-03,  3.04065e-03, -3.46998e-03], decimal=5)
       #  npt.assert_almost_equal(data['FX-FX/INT']['data_arr'][100:103,0], [[8.26026e-05],[8.16368e-05],[8.38173e-05]], decimal=5)
       #  npt.assert_almost_equal(data['FX-FX/MAIN']['data_arr'][100:103,0], [[1.0086, 1.0124, 1.0058, 1.0112],
       # [1.0112, 1.0137, 1.0076, 1.0093],
       # [1.0093, 1.013 , 1.0011, 1.002 ]], decimal=5)
       #  npt.assert_almost_equal(data['FX-FX/CCPFXRATE']['data_arr'][100:103,0], [[0.9993 ],
       # [0.99602],
       # [0.9992 ]], decimal=5)
       #  npt.assert_almost_equal(data['KD-Strat/ref_aum'].values[100:103,0], [100000., 100000., 100000.], decimal=5)
        te = datetime.now()
        print("回测准备与导入数据用时", te-ts)
        ts = datetime.now()
        backtest_engine.start_backtest()
        te = datetime.now()
        print("回测+编译用时", te-ts)
        ts = datetime.now()
        backtest_engine.export_results()
        te = datetime.now()
        print("保存回测结果和策略状态用时", te-ts)
        te0 = datetime.now()
        print("回测总用时", te0-ts0)

        resultreader=ResultReader("tests/engine/backtest/test_precision_complete/result/KD_lv2.hdf5")
        output = resultreader.get_strategy_3d(id_list=range(20000),field='pnl')
        total_pnl = output[0][-1].sum()/100000
        self.assertEqual(round(total_pnl, 10), 0.1298523641)

def generate_combination_strategy_pool():
    pool_setting = {"symbol":{"FX":['EURUSD', 'USDJPY', 'NZDJPY', 'NOKSEK', 'GBPJPY', 'CADJPY', 'CADCHF', 'AUDNZD', 'AUDJPY', 'AUDCAD', 'USDSEK', 'USDNOK', 'EURCHF', 'USDCHF', 'USDCAD', 'NZDUSD', 'GBPUSD', 'AUDUSD', 'USDSGD', 'USDTHB', 'GBPCAD', 'EURCAD', 'EURJPY', 'EURGBP', 'GBPCHF', 'USDINR', 'USDIDR', 'USDTWD', 'USDKRW'],
                              "RSI": list(np.arange(1).astype(np.float64)), #不转成float json会报错
                              "KD": list(np.arange(1).astype(np.float64))}, #不转成float json会报错
                    "strategy_module":"tests.engine.backtest.test_precision_complete.algorithm.lv3",
                    "strategy_name":"Combination_strategy",
                    "save_path":"tests/engine/backtest/test_precision_complete/strategy_pool/lv3_strategy_pool.csv",
                    "params":{

                             }}
    strategy_pool_generator(pool_setting)

class TestFullLevel3PrecisionTestCase(TestCase):
    def test_LV3_precision(self):
        generate_combination_strategy_pool()
        run_once('tests/engine/backtest/test_precision_complete/config/lv3.json')
        resultreader=ResultReader("tests/engine/backtest/test_precision_complete/result/combination.hdf5")
        output = resultreader.get_strategy_3d(id_list=range(20000),field='pnl')
        total_pnl = output[0][-1].sum()/100000
        self.assertEqual(np.round(total_pnl, 10), 0.0873334323)
