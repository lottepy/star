import os
import unittest

import numpy as np
import pandas as pd
from quantcycle.engine.backtest_engine import BacktestEngine
from quantcycle.app.result_exporter.result_reader import ResultReader


import itertools
import json

class TestBacktestEngine(unittest.TestCase):

    #   TEST strategy路径： D:\work\quant platform\quantcycle\tests\test_backtest_engine_support\test_strategy.py
    def test_status_recorder_no_load(self):
        '''没有load pickle文件, TEST strategy存储想要存储的状态，
           然后通过status_recorder读取看和存的是否一样'''
        if os.path.exists('tests/engine/backtest/test_function_status_recorder/result/rsi.pkl'):
            os.remove('tests/engine/backtest/test_function_status_recorder/result/rsi.pkl')
        backtest_engine = BacktestEngine()
        backtest_engine.load_config(json.load(open('tests/engine/backtest/test_function_status_recorder/config/test_status_record.json')))

        backtest_engine.prepare()
        backtest_engine.start_backtest()
        backtest_engine.export_results()
        pickle_dict=backtest_engine.strategy_recorder.load("rsi",
                    "tests/engine/backtest/test_function_status_recorder/result/")
        # assert (pickle_dict[0]['rsi'] == np.array([10,5,0.01,0.005,10],dtype='float64')).all()
        # assert (pickle_dict[1]['rsi'] == np.array([20, 5, 0.01, 0.005, 10], dtype='float64')).all()
        assert pickle_dict[0]['rsi'] == 10
        assert pickle_dict[1]['rsi'] == 20



    def test_status_recorder_with_load(self):
        '''有load pickle文件, 把需要读取的状态读取到 TEST strategy，
           看TEST strategy内取到状态与pickle文件是否一致'''


        backtest_engine = BacktestEngine()
        backtest_engine.load_config(json.load(open('tests/engine/backtest/test_function_status_recorder/config/test_status_record.json')))

        backtest_engine.prepare()
        backtest_engine.start_backtest()
        pickle_dict = backtest_engine.strategy_recorder.load("rsi", "tests/engine/backtest/test_function_status_recorder/result/")
        assert (backtest_engine.strategy_id_strategy_dict[0].status.keys() == pickle_dict[0].keys())
        #assert np.array([ (backtest_engine.strategy_id_strategy_dict[0].status[k] == pickle_dict[0][k]).all() for k in list(backtest_engine.strategy_id_strategy_dict[0].status.keys())]).all()
        assert np.array([ (backtest_engine.strategy_id_strategy_dict[0].status[k] == pickle_dict[0][k]) for k in list(backtest_engine.strategy_id_strategy_dict[0].status.keys())]).all()
        assert (backtest_engine.strategy_id_strategy_dict[1].status.keys() == pickle_dict[1].keys())
        #assert np.array([ (backtest_engine.strategy_id_strategy_dict[1].status[k] == pickle_dict[1][k]).all() for k in list(backtest_engine.strategy_id_strategy_dict[0].status.keys())]).all()
        assert np.array([ (backtest_engine.strategy_id_strategy_dict[1].status[k] == pickle_dict[1][k]) for k in list(backtest_engine.strategy_id_strategy_dict[1].status.keys())]).all()


    def test_muti_universe(self):
        """
        回测跑一组相同universe，再跑另一组相同universe，与回测直接跑两组不同universe结果相同
        """

        #第一组
        length = [10]
        break_threshold = [5]
        stop_profit = [0.01]
        stop_loss = [0.005]
        max_hold_days = [10]
        all_param_combinations = list(itertools.product(length, break_threshold, stop_profit, stop_loss, max_hold_days))
        param_dict_df = pd.DataFrame(all_param_combinations,
                                     columns=["length", "break_threshold", "stop_profit", "stop_loss", "max_hold_days"])
        param_dict_list = [json.dumps(dict(row)) for index, row in param_dict_df.iterrows()]
        symbol_list = ['AUDCAD', 'AUDJPY',
                       'EURCHF', 'EURGBP']
        symbol = [json.dumps({"FX": symbol_list})]
        strategy_module = ["tests.engine.backtest.test_function_multi_universe.algorithm.simple_oscillator_strategy"]
        strategy_name = ["RSI_strategy"]

        all_combinations = list(itertools.product(strategy_module, strategy_name, param_dict_list, symbol))

        pd.DataFrame(all_combinations, columns=["strategy_module", "strategy_name", "params", "symbol"]).to_csv(
            "tests/engine/backtest/test_function_multi_universe/strategy_pool/rsi_strategy_pool_test.csv", index=False)

        backtest_engine1 = BacktestEngine()
        backtest_engine1.load_config(json.load(open('tests/engine/backtest/test_function_multi_universe/config/RSI.json')))
        backtest_engine1.prepare()
        backtest_engine1.start_backtest()
        backtest_engine1.export_results()
        result_reader1 = ResultReader("tests/engine/backtest/test_function_multi_universe/result/rsi_lv1")
        result_reader1.to_csv(export_folder='tests/engine/backtest/test_function_multi_universe/result/test1', id_list=[0])
        pnl1 = pd.read_csv(os.path.join("tests/engine/backtest/test_function_multi_universe", "result/test1/0/pnl.csv"))

        #第二组
        symbol_list = ['GBPUSD', 'NOKSEK',
                       'USDINR', 'USDJPY' ]
        symbol = [json.dumps({"FX": symbol_list})]
        all_combinations = list(itertools.product(strategy_module, strategy_name, param_dict_list, symbol))

        pd.DataFrame(all_combinations, columns=["strategy_module", "strategy_name", "params", "symbol"]).to_csv(
            "tests/engine/backtest/test_function_multi_universe/strategy_pool/rsi_strategy_pool_test.csv", index=False)

        backtest_engine2 = BacktestEngine()
        backtest_engine2.load_config(json.load(open('tests/engine/backtest/test_function_multi_universe/config/RSI_02.json')))
        backtest_engine2.prepare()
        backtest_engine2.start_backtest()
        backtest_engine2.export_results()
        result_reader2 = ResultReader("tests/engine/backtest/test_function_multi_universe/result/rsi_lv1_test_02")
        result_reader2.to_csv(export_folder='tests/engine/backtest/test_function_multi_universe/result/test2', id_list=[0])
        pnl2= pd.read_csv(os.path.join("tests/engine/backtest/test_function_multi_universe", "result/test2/0/pnl.csv"))


        #合并两组
        symbol_list1 = ['AUDCAD', 'AUDJPY',
                       'EURCHF', 'EURGBP']

        symbol_list2 = ['GBPUSD', 'NOKSEK',
                       'USDINR', 'USDJPY']

        symbol = [json.dumps({"FX": symbol_list1}),json.dumps({"FX": symbol_list2})]
        all_combinations = list(itertools.product(strategy_module, strategy_name, param_dict_list, symbol))

        pd.DataFrame(all_combinations, columns=["strategy_module", "strategy_name", "params", "symbol"]).to_csv(
            "tests/engine/backtest/test_function_multi_universe/strategy_pool/rsi_strategy_pool_test.csv", index=False)

        backtest_engine3 = BacktestEngine()
        backtest_engine3.load_config(json.load(open('tests/engine/backtest/test_function_multi_universe/config/RSI_03.json')))
        backtest_engine3.prepare()
        backtest_engine3.start_backtest()
        backtest_engine3.export_results()

        result_reader3 = ResultReader("tests/engine/backtest/test_function_multi_universe/result/rsi_lv1_test_03")
        result_reader3.to_csv(export_folder='tests/engine/backtest/test_function_multi_universe/result/test3', id_list=[0,1])
        pnl3= pd.read_csv(os.path.join("tests/engine/backtest/test_function_multi_universe", "result/test3/0/pnl.csv"))
        pnl4 = pd.read_csv(os.path.join("tests/engine/backtest/test_function_multi_universe", "result/test3/1/pnl.csv"))


        assert np.isclose(np.sum(pnl1.iloc[-1:, 2:].values), np.sum(pnl3.iloc[-1:, 2:].values))
        assert np.isclose(np.sum(pnl2.iloc[-1:, 2:].values), np.sum(pnl4.iloc[-1:, 2:].values))

