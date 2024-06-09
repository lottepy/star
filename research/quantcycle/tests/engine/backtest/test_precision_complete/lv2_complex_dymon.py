from quantcycle.engine.backtest_engine import BacktestEngine
from quantcycle.utils.strategy_pool_generator import strategy_pool_generator
from datetime import datetime
import numpy as np


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
                                # "length" : [10.]
                             }}
    strategy_pool_generator(pool_setting)

def generate_CCI_allocation_strategy_pool():
    pool_setting = {"symbol":{"FX":['EURUSD', 'USDJPY', 'NZDJPY', 'NOKSEK', 'GBPJPY', 'CADJPY', 'CADCHF', 'AUDNZD', 'AUDJPY', 'AUDCAD', 'USDSEK', 'USDNOK', 'EURCHF', 'USDCHF', 'USDCAD', 'NZDUSD', 'GBPUSD', 'AUDUSD', 'USDSGD', 'USDTHB', 'GBPCAD', 'EURCAD', 'EURJPY', 'EURGBP', 'GBPCHF', 'USDINR', 'USDIDR', 'USDTWD', 'USDKRW'],
                              "CCI": list(np.arange(240*29).astype(np.float64))}, #不转成float json会报错
                    "strategy_module":"tests.engine.backtest.test_precision_complete.algorithm.allocation_method",
                    "strategy_name":"Allocation_strategy",
                    "save_path":"tests/engine/backtest/test_precision_complete/strategy_pool/CCI_lv2_strategy_pool.csv",
                    "params":{
                                # "length" : [10.]
                             }}
    strategy_pool_generator(pool_setting)

def generate_MACD_P_allocation_strategy_pool():
    pool_setting = {"symbol":{"FX":['EURUSD', 'USDJPY', 'NZDJPY', 'NOKSEK', 'GBPJPY', 'CADJPY', 'CADCHF', 'AUDNZD', 'AUDJPY', 'AUDCAD', 'USDSEK', 'USDNOK', 'EURCHF', 'USDCHF', 'USDCAD', 'NZDUSD', 'GBPUSD', 'AUDUSD', 'USDSGD', 'USDTHB', 'GBPCAD', 'EURCAD', 'EURJPY', 'EURGBP', 'GBPCHF', 'USDINR', 'USDIDR', 'USDTWD', 'USDKRW'],
                              "MACD_P": list(np.arange(240*29).astype(np.float64))}, #不转成float json会报错
                    "strategy_module":"tests.engine.backtest.test_precision_complete.algorithm.allocation_method",
                    "strategy_name":"Allocation_strategy",
                    "save_path":"tests/engine/backtest/test_precision_complete/strategy_pool/MACD_P_lv2_strategy_pool.csv",
                    "params":{
                                # "length" : [10.]
                             }}
    strategy_pool_generator(pool_setting)

def generate_Bollinger_MACD_Divergence_allocation_strategy_pool():
    pool_setting = {"symbol":{"FX":['EURUSD', 'USDJPY', 'NZDJPY', 'NOKSEK', 'GBPJPY', 'CADJPY', 'CADCHF', 'AUDNZD', 'AUDJPY', 'AUDCAD', 'USDSEK', 'USDNOK', 'EURCHF', 'USDCHF', 'USDCAD', 'NZDUSD', 'GBPUSD', 'AUDUSD', 'USDSGD', 'USDTHB', 'GBPCAD', 'EURCAD', 'EURJPY', 'EURGBP', 'GBPCHF', 'USDINR', 'USDIDR', 'USDTWD', 'USDKRW'],
                              "Bollinger_MACD_Divergence": list(np.arange(432*29).astype(np.float64))}, #不转成float json会报错
                    "strategy_module":"tests.engine.backtest.test_precision_complete.algorithm.allocation_method",
                    "strategy_name":"Allocation_strategy",
                    "save_path":"tests/engine/backtest/test_precision_complete/strategy_pool/Bollinger_MACD_Divergence_lv2_strategy_pool.csv",
                    "params":{
                                # "length" : [10.]
                             }}
    strategy_pool_generator(pool_setting)

def generate_single_MA_allocation_strategy_pool():
    pool_setting = {"symbol":{"FX":['EURUSD', 'USDJPY', 'NZDJPY', 'NOKSEK', 'GBPJPY', 'CADJPY', 'CADCHF', 'AUDNZD', 'AUDJPY', 'AUDCAD', 'USDSEK', 'USDNOK', 'EURCHF', 'USDCHF', 'USDCAD', 'NZDUSD', 'GBPUSD', 'AUDUSD', 'USDSGD', 'USDTHB', 'GBPCAD', 'EURCAD', 'EURJPY', 'EURGBP', 'GBPCHF', 'USDINR', 'USDIDR', 'USDTWD', 'USDKRW'],
                              "single_MA": list(np.arange(216*29).astype(np.float64))}, #不转成float json会报错
                    "strategy_module":"tests.engine.backtest.test_precision_complete.algorithm.allocation_method",
                    "strategy_name":"Allocation_strategy",
                    "save_path":"tests/engine/backtest/test_precision_complete/strategy_pool/single_MA_lv2_strategy_pool.csv",
                    "params":{
                                # "length" : [10.]
                             }}
    strategy_pool_generator(pool_setting)

def generate_single_EMA_allocation_strategy_pool():
    pool_setting = {"symbol":{"FX":['EURUSD', 'USDJPY', 'NZDJPY', 'NOKSEK', 'GBPJPY', 'CADJPY', 'CADCHF', 'AUDNZD', 'AUDJPY', 'AUDCAD', 'USDSEK', 'USDNOK', 'EURCHF', 'USDCHF', 'USDCAD', 'NZDUSD', 'GBPUSD', 'AUDUSD', 'USDSGD', 'USDTHB', 'GBPCAD', 'EURCAD', 'EURJPY', 'EURGBP', 'GBPCHF', 'USDINR', 'USDIDR', 'USDTWD', 'USDKRW'],
                              "single_EMA": list(np.arange(216*29).astype(np.float64))}, #不转成float json会报错
                    "strategy_module":"tests.engine.backtest.test_precision_complete.algorithm.allocation_method",
                    "strategy_name":"Allocation_strategy",
                    "save_path":"tests/engine/backtest/test_precision_complete/strategy_pool/single_EMA_lv2_strategy_pool.csv",
                    "params":{
                                # "length" : [10.]
                             }}
    strategy_pool_generator(pool_setting)

def generate_double_MA_allocation_strategy_pool():
    pool_setting = {"symbol":{"FX":['EURUSD', 'USDJPY', 'NZDJPY', 'NOKSEK', 'GBPJPY', 'CADJPY', 'CADCHF', 'AUDNZD', 'AUDJPY', 'AUDCAD', 'USDSEK', 'USDNOK', 'EURCHF', 'USDCHF', 'USDCAD', 'NZDUSD', 'GBPUSD', 'AUDUSD', 'USDSGD', 'USDTHB', 'GBPCAD', 'EURCAD', 'EURJPY', 'EURGBP', 'GBPCHF', 'USDINR', 'USDIDR', 'USDTWD', 'USDKRW'],
                              "double_MA": list(np.arange(324*29).astype(np.float64))}, #不转成float json会报错
                    "strategy_module":"tests.engine.backtest.test_precision_complete.algorithm.allocation_method",
                    "strategy_name":"Allocation_strategy",
                    "save_path":"tests/engine/backtest/test_precision_complete/strategy_pool/double_MA_lv2_strategy_pool.csv",
                    "params":{
                                # "length" : [10.]
                             }}
    strategy_pool_generator(pool_setting)

def generate_Donchian_Channel_allocation_strategy_pool():
    pool_setting = {"symbol":{"FX":['EURUSD', 'USDJPY', 'NZDJPY', 'NOKSEK', 'GBPJPY', 'CADJPY', 'CADCHF', 'AUDNZD', 'AUDJPY', 'AUDCAD', 'USDSEK', 'USDNOK', 'EURCHF', 'USDCHF', 'USDCAD', 'NZDUSD', 'GBPUSD', 'AUDUSD', 'USDSGD', 'USDTHB', 'GBPCAD', 'EURCAD', 'EURJPY', 'EURGBP', 'GBPCHF', 'USDINR', 'USDIDR', 'USDTWD', 'USDKRW'],
                              "Donchian_Channel": list(np.arange(216*29).astype(np.float64))}, #不转成float json会报错
                    "strategy_module":"tests.engine.backtest.test_precision_complete.algorithm.allocation_method",
                    "strategy_name":"Allocation_strategy",
                    "save_path":"tests/engine/backtest/test_precision_complete/strategy_pool/Donchian_Channel_lv2_strategy_pool.csv",
                    "params":{
                                # "length" : [10.]
                             }}
    strategy_pool_generator(pool_setting)


def run_once(json_path):
    ts0 = datetime.now()
    backtest_engine = BacktestEngine()
    backtest_engine.load_config(json_path)
    ts = datetime.now()
    backtest_engine.prepare()
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

if __name__ == "__main__":
    generate_rsi_allocation_strategy_pool()
    generate_KD_allocation_strategy_pool()
    # generate_CCI_allocation_strategy_pool()
    # generate_MACD_P_allocation_strategy_pool()
    # generate_Bollinger_MACD_Divergence_allocation_strategy_pool()
    # generate_single_MA_allocation_strategy_pool()
    # generate_single_EMA_allocation_strategy_pool()
    # generate_double_MA_allocation_strategy_pool()
    # generate_Donchian_Channel_allocation_strategy_pool()

    # run_once('tests/engine/backtest/test_precision_complete/config/RSI_lv2.json')
    run_once('tests/engine/backtest/test_precision_complete/config/KD_lv2.json')
    # run_once('tests/engine/backtest/test_precision_complete/config/CCI_lv2.json')
    # run_once('tests/engine/backtest/test_precision_complete/config/MACD_P_lv2.json')
    # run_once('tests/engine/backtest/test_precision_complete/config/Bollinger_MACD_Divergence_lv2.json')
    
    # run_once('tests/engine/backtest/test_precision_complete/config/single_MA_lv2.json')
    # run_once('tests/engine/backtest/test_precision_complete/config/single_EMA_lv2.json')
    # run_once('tests/engine/backtest/test_precision_complete/config/double_MA_lv2.json')
    # run_once('tests/engine/backtest/test_precision_complete/config/Donchian_Channel_lv2.json')

    # resultreader = ResultReader("tests/engine/backtest/test_precision_complete/result/KD_lv2.hdf5")
    # output = resultreader.get_strategy(id_list=range(3), start_end_time=None, fields=['pnl'])
    # print(output/2900000)
    # pnl = []
    # for key, item in output.items():
    #     pnl.append(output[key][0].values.sum())
    
    # pnl_frame = pd.DataFrame(pnl)
    # pnl_frame.to_csv("tests/engine/backtest/test_precision_complete/result/result_Lv2.csv")
    
