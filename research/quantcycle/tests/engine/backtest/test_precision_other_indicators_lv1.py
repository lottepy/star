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


def generate_CCI_lv1_strategy_pool():
    # symbol_list = list(pd.read_csv("tests/engine/backtest/test_CCI_lv1/stock_pool/aqm_turnover_union_fx_noZAR.csv")['SECUCODE'])
    pool_setting = {"symbol":{"FX":['EURUSD', 'USDJPY', 'NZDJPY', 'NOKSEK', 'GBPJPY', 'CADJPY', 'CADCHF', 'AUDNZD', 'AUDJPY', 'AUDCAD', 'USDSEK', 'USDNOK', 'EURCHF', 'USDCHF', 'USDCAD', 'NZDUSD', 'GBPUSD', 'AUDUSD', 'USDSGD', 'USDTHB', 'GBPCAD', 'EURCAD', 'EURJPY', 'EURGBP', 'GBPCHF', 'USDINR', 'USDIDR', 'USDTWD', 'USDKRW']},
                    "strategy_module":"tests.engine.backtest.test_precision_complete.algorithm.simple_oscillator_strategy",
                    "strategy_name":"CCI_strategy",
                    "save_path":"tests/engine/backtest/test_precision_complete/strategy_pool/CCI_lv1_strategy_pool.csv",
                    "params":{
                                "length" : [10,15,20,40],
                                "break_threshold" :[100,120,140,160,180],
                                "stop_profit" : [0.01,0.02],
                                "stop_loss" :[0.005,0.01,0.02],
                                "max_hold_days" : [10,20]
                             }}
    strategy_pool_generator(pool_setting)

def generate_MACD_P_lv1_strategy_pool():
    # symbol_list = list(pd.read_csv("tests/engine/backtest/test_MACD_P_lv1/stock_pool/aqm_turnover_union_fx_noZAR.csv")['SECUCODE'])
    pool_setting = {"symbol":{"FX":['EURUSD', 'USDJPY', 'NZDJPY', 'NOKSEK', 'GBPJPY', 'CADJPY', 'CADCHF', 'AUDNZD', 'AUDJPY', 'AUDCAD', 'USDSEK', 'USDNOK', 'EURCHF', 'USDCHF', 'USDCAD', 'NZDUSD', 'GBPUSD', 'AUDUSD', 'USDSGD', 'USDTHB', 'GBPCAD', 'EURCAD', 'EURJPY', 'EURGBP', 'GBPCHF', 'USDINR', 'USDIDR', 'USDTWD', 'USDKRW']},
                    "strategy_module":"tests.engine.backtest.test_precision_complete.algorithm.simple_oscillator_strategy",
                    "strategy_name":"MACD_P_strategy",
                    "save_path":"tests/engine/backtest/test_precision_complete/strategy_pool/MACD_P_lv1_strategy_pool.csv",
                    "params":{
                                "length1" : [5,10],
                                "length2" : [20,30],
                                "length3" : [9],
                                "break_threshold" :[-0.001,0.0,0.001,0.002,0.003],
                                "stop_profit" : [0.01,0.02],
                                "stop_loss" :[0.005,0.01,0.02],
                                "max_hold_days" : [10,20]
                             }}
    strategy_pool_generator(pool_setting)

def generate_Bollinger_MACD_Divergence_lv1_strategy_pool():
    # symbol_list = list(pd.read_csv("tests/engine/backtest/test_RSI_lv1/stock_pool/aqm_turnover_union_fx_noZAR.csv")['SECUCODE'])
    pool_setting = {"symbol":{"FX":['EURUSD', 'USDJPY', 'NZDJPY', 'NOKSEK', 'GBPJPY', 'CADJPY', 'CADCHF', 'AUDNZD', 'AUDJPY', 'AUDCAD', 'USDSEK', 'USDNOK', 'EURCHF', 'USDCHF', 'USDCAD', 'NZDUSD', 'GBPUSD', 'AUDUSD', 'USDSGD', 'USDTHB', 'GBPCAD', 'EURCAD', 'EURJPY', 'EURGBP', 'GBPCHF', 'USDINR', 'USDIDR', 'USDTWD', 'USDKRW']},
                    "strategy_module":"tests.engine.backtest.test_precision_complete.algorithm.combine_strategy",
                    "strategy_name":"Bollinger_two_macd_divergence_update_warning_confirm_strategy",
                    "save_path":"tests/engine/backtest/test_precision_complete/strategy_pool/Bollinger_MACD_Divergence_lv1_strategy_pool.csv",
                    "params":{
                                "length" : [10,15,20,40],
                                "width1" : [0.0,0.5,1.0],
                                "width2" : [1.5,2.0],
                                "macd_length1" : [10.0],
                                "macd_length2" : [25.0],
                                "macd_length3" : [10.0],
                                "dochain_length" : [10.0,20.0,40.0],
                                "stop_profit" : [0.01,0.02,0.05],
                                "stop_loss" :[0.005],
                                "max_hold_days" : [10,20]
                             }}
    strategy_pool_generator(pool_setting)    

def generate_single_MA_lv1_strategy_pool():
    # symbol_list = list(pd.read_csv("tests/engine/backtest/test_single_MA_lv1/stock_pool/aqm_turnover_union_fx_noZAR.csv")['SECUCODE'])
    pool_setting = {"symbol":{"FX":['EURUSD', 'USDJPY', 'NZDJPY', 'NOKSEK', 'GBPJPY', 'CADJPY', 'CADCHF', 'AUDNZD', 'AUDJPY', 'AUDCAD', 'USDSEK', 'USDNOK', 'EURCHF', 'USDCHF', 'USDCAD', 'NZDUSD', 'GBPUSD', 'AUDUSD', 'USDSGD', 'USDTHB', 'GBPCAD', 'EURCAD', 'EURJPY', 'EURGBP', 'GBPCHF', 'USDINR', 'USDIDR', 'USDTWD', 'USDKRW']},
                    "strategy_module":"tests.engine.backtest.test_precision_complete.algorithm.trend_strategies",
                    "strategy_name":"single_MA_strategy",
                    "save_path":"tests/engine/backtest/test_precision_complete/strategy_pool/single_MA_lv1_strategy_pool.csv",
                    "params":{
                                "stop_profit" : [0.01, 0.02, 0.05],
                                "stop_loss" :[0.003, 0.005, 0.01],
                                "max_hold_days" : [10.0, 20.0, 40.0],
                                "len_MA" : [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 80.0, 100.0]
                             }}
    strategy_pool_generator(pool_setting)    
def generate_single_EMA_lv1_strategy_pool():
    # symbol_list = list(pd.read_csv("tests/engine/backtest/test_single_EMA_lv1/stock_pool/aqm_turnover_union_fx_noZAR.csv")['SECUCODE'])
    pool_setting = {"symbol":{"FX":['EURUSD', 'USDJPY', 'NZDJPY', 'NOKSEK', 'GBPJPY', 'CADJPY', 'CADCHF', 'AUDNZD', 'AUDJPY', 'AUDCAD', 'USDSEK', 'USDNOK', 'EURCHF', 'USDCHF', 'USDCAD', 'NZDUSD', 'GBPUSD', 'AUDUSD', 'USDSGD', 'USDTHB', 'GBPCAD', 'EURCAD', 'EURJPY', 'EURGBP', 'GBPCHF', 'USDINR', 'USDIDR', 'USDTWD', 'USDKRW']},
                    "strategy_module":"tests.engine.backtest.test_precision_complete.algorithm.trend_strategies",
                    "strategy_name":"single_EMA_strategy",
                    "save_path":"tests/engine/backtest/test_precision_complete/strategy_pool/single_EMA_lv1_strategy_pool.csv",
                    "params":{
                                "stop_profit" : [0.01, 0.02, 0.05],
                                "stop_loss" :[0.003, 0.005, 0.01],
                                "max_hold_days" : [10.0, 20.0, 40.0],
                                "len_MA" : [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 80.0, 100.0]
                             }}
    strategy_pool_generator(pool_setting) 

def generate_double_MA_lv1_strategy_pool():
    # symbol_list = list(pd.read_csv("tests/engine/backtest/test_double_MA_lv1/stock_pool/aqm_turnover_union_fx_noZAR.csv")['SECUCODE'])
    pool_setting = {"symbol":{"FX":['EURUSD', 'USDJPY', 'NZDJPY', 'NOKSEK', 'GBPJPY', 'CADJPY', 'CADCHF', 'AUDNZD', 'AUDJPY', 'AUDCAD', 'USDSEK', 'USDNOK', 'EURCHF', 'USDCHF', 'USDCAD', 'NZDUSD', 'GBPUSD', 'AUDUSD', 'USDSGD', 'USDTHB', 'GBPCAD', 'EURCAD', 'EURJPY', 'EURGBP', 'GBPCHF', 'USDINR', 'USDIDR', 'USDTWD', 'USDKRW']},
                    "strategy_module":"tests.engine.backtest.test_precision_complete.algorithm.trend_strategies",
                    "strategy_name":"double_MA_strategy",
                    "save_path":"tests/engine/backtest/test_precision_complete/strategy_pool/double_MA_lv1_strategy_pool.csv",
                    "params":{
                                "stop_profit" : [0.01, 0.02, 0.05],
                                "stop_loss" :[0.003, 0.005, 0.01],
                                "max_hold_days" : [10.0, 20.0, 40.0],
                                "short_length" : [10.0, 20.0, 30.0], 
                                "long_length" : [40.0, 50.0, 60.0, 80.0]
                             }}
    strategy_pool_generator(pool_setting) 

def generate_Donchian_Channel_lv1_strategy_pool():
    # symbol_list = list(pd.read_csv("tests/engine/backtest/test_Donchian_lv1/stock_pool/aqm_turnover_union_fx_noZAR.csv")['SECUCODE'])
    pool_setting = {"symbol":{"FX":['EURUSD', 'USDJPY', 'NZDJPY', 'NOKSEK', 'GBPJPY', 'CADJPY', 'CADCHF', 'AUDNZD', 'AUDJPY', 'AUDCAD', 'USDSEK', 'USDNOK', 'EURCHF', 'USDCHF', 'USDCAD', 'NZDUSD', 'GBPUSD', 'AUDUSD', 'USDSGD', 'USDTHB', 'GBPCAD', 'EURCAD', 'EURJPY', 'EURGBP', 'GBPCHF', 'USDINR', 'USDIDR', 'USDTWD', 'USDKRW']},
                    "strategy_module":"tests.engine.backtest.test_precision_complete.algorithm.trend_strategies",
                    "strategy_name":"Donchian_Channel_strategy",
                    "save_path":"tests/engine/backtest/test_precision_complete/strategy_pool/Donchian_Channel_lv1_strategy_pool.csv",
                    "params":{
                                "stop_profit" : [0.01, 0.02, 0.05],
                                "stop_loss" :[0.003, 0.005, 0.01],
                                "max_hold_days" : [10.0, 20.0, 40.0],
                                "len_MA" : [5.0, 10.0, 15.0, 20.0, 40.0, 60.0, 80.0, 100.0]
                             }}
    strategy_pool_generator(pool_setting) 

class TestFullLevel1PrecisionTestCase(TestCase):
    def test_CCI_LV1_precision(self):
        generate_CCI_lv1_strategy_pool()
        run_once('tests/engine/backtest/test_precision_complete/config/CCI_lv1.json')
        resultreader=ResultReader("tests/engine/backtest/test_precision_complete/result/CCI_lv1.hdf5")
        output = resultreader.get_strategy_3d(id_list=range(20000),field='pnl')
        total_pnl = output[0][-1].sum()
        self.assertEqual(round(total_pnl, 5), 9969122.47394)

    def test_MACD_P_LV1_precision(self):
        generate_MACD_P_lv1_strategy_pool()
        run_once('tests/engine/backtest/test_precision_complete/config/MACD_P_lv1.json')
        resultreader=ResultReader("tests/engine/backtest/test_precision_complete/result/MACD_P_lv1.hdf5")
        output = resultreader.get_strategy_3d(id_list=range(20000),field='pnl')
        total_pnl = output[0][-1].sum()
        self.assertEqual(round(total_pnl, 5), 21284416.86453)

    def test_Bollinger_MACD_Divergence_LV1_precision(self):
        generate_Bollinger_MACD_Divergence_lv1_strategy_pool()
        run_once('tests/engine/backtest/test_precision_complete/config/Bollinger_MACD_Divergence_lv1.json')
        resultreader=ResultReader("tests/engine/backtest/test_precision_complete/result/Bollinger_MACD_Divergence_lv1.hdf5")
        output = resultreader.get_strategy_3d(id_list=range(20000),field='pnl')
        total_pnl = output[0][-1].sum()
        self.assertEqual(round(total_pnl, 5), 3029609.53829)

    def test_single_MA_LV1_precision(self):
        generate_single_MA_lv1_strategy_pool()
        run_once('tests/engine/backtest/test_precision_complete/config/single_MA_lv1.json')
        resultreader=ResultReader("tests/engine/backtest/test_precision_complete/result/single_MA_lv1.hdf5")
        output = resultreader.get_strategy_3d(id_list=range(20000),field='pnl')
        total_pnl = output[0][-1].sum()
        self.assertEqual(round(total_pnl, 5), 2615845.20256)

    def test_single_EMA_LV1_precision(self):
        generate_single_EMA_lv1_strategy_pool()
        run_once('tests/engine/backtest/test_precision_complete/config/single_EMA_lv1.json')
        resultreader=ResultReader("tests/engine/backtest/test_precision_complete/result/single_EMA_lv1.hdf5")
        output = resultreader.get_strategy_3d(id_list=range(20000),field='pnl')
        total_pnl = output[0][-1].sum()
        self.assertEqual(round(total_pnl, 5), 5631582.63815)

    def test_double_MA_LV1_precision(self):
        generate_double_MA_lv1_strategy_pool()
        run_once('tests/engine/backtest/test_precision_complete/config/double_MA_lv1.json')
        resultreader=ResultReader("tests/engine/backtest/test_precision_complete/result/double_MA_lv1.hdf5")
        output = resultreader.get_strategy_3d(id_list=range(20000),field='pnl')
        total_pnl = output[0][-1].sum()
        self.assertEqual(round(total_pnl, 5), 1704733.48046)

    def _test_Donchian_Channel_LV1_precision(self):
        generate_Donchian_Channel_lv1_strategy_pool()
        run_once('tests/engine/backtest/test_precision_complete/config/Donchian_Channel_lv1.json')
        resultreader=ResultReader("tests/engine/backtest/test_precision_complete/result/Donchian_Channel_lv1.hdf5")
        output = resultreader.get_strategy_3d(id_list=range(20000),field='pnl')
        total_pnl = output[0][-1].sum()
        self.assertEqual(round(total_pnl, 5), 7456098.13023)
