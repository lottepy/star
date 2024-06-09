from quantcycle.engine.backtest_engine import BacktestEngine
from quantcycle.utils.strategy_pool_generator import strategy_pool_generator
from datetime import datetime
import numpy as np


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
    generate_combination_strategy_pool()

    run_once('tests/engine/backtest/test_precision_complete/config/lv3.json')
 