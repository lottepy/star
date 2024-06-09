import os
import unittest
import json
import numpy as np

from quantcycle.engine.backtest_engine import BacktestEngine
from quantcycle.utils.strategy_pool_generator import strategy_pool_generator

# fx_random_weight 说明
# 1、weight文件现在是backtest-engine hardcore read tests/test_case/backtest_engine_support/fx_daily_weight_v21.csv
# 2、commission文件现在是backtest-engine hardcore read tests/test_case/backtest_engine_support/aqm_turnover_union_fx.csv
# 3、其余数据文件通过data_manager 从 data_master 读取,如果data_master数据发生变化，结果也会发生改变
# 4、json与strategy_pool目前存在于tests/test_case/backtest_engine_support中

class TestBacktestEngine(unittest.TestCase):

    def test_fx_random_weight_backtest(self):
        pool_setting = {"symbol": {"FX": ['AUDCAD', 'AUDJPY', 'AUDNZD', 'AUDUSD', 'CADCHF', 'CADJPY', 'EURCAD',
                                          'EURCHF', 'EURGBP', 'EURJPY', 'EURUSD', 'GBPCAD', 'GBPCHF', 'GBPJPY',
                                          'GBPUSD', 'NOKSEK', 'NZDJPY', 'NZDUSD', 'USDCAD', 'USDCHF', 'USDIDR',
                                          'USDINR', 'USDJPY', 'USDKRW', 'USDNOK', 'USDSEK', 'USDSGD', 'USDTHB',
                                          'USDTWD']},
                        "strategy_module": "tests.engine.backtest.test_precision_fx_random_weight.algorithm.sample_strategy",
                        "strategy_name": "Fx_Random_Weight_Strategy",
                        "save_path": "tests/engine/backtest/test_precision_fx_random_weight/strategy_pool/test_fx_random_weight_strategy_pool.csv",
                        "params": {
                            "length": [10.],
                        }}
        strategy_pool_generator(pool_setting)
        backtest_engine = BacktestEngine()
        backtest_engine.load_config(json.load(open('tests/engine/backtest/test_precision_fx_random_weight/config/test_fx_random_weight.json')))
        backtest_engine.prepare()
        backtest_engine.start_backtest()
        pnl = np.sum(backtest_engine.strategy_id_pms_dict[0].historial_pnl[-1])
        pnl = round(pnl, 3)
        self.assertEqual(pnl, -342603.436)
