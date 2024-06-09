import unittest
import numpy as np
import pandas as pd
from quantcycle.utils.strategy_pool_generator import strategy_pool_generator
from quantcycle.app.result_exporter.result_reader import ResultReader
from quantcycle.utils.run_once import run_once

class TestTradableTable(unittest.TestCase):
    def test_local_tradable_table(self):
        # a test copy from multi_assets test_RW_FX_CN_stock
        pool_settings = {'symbol': {'FX': ['EURUSD', 'NOKSEK', 'NZDJPY', 'USDJPY'],
                                    'STOCKS': ['000005.SZ', '600016.SH', '600028.SH', '601398.SH', '601628.SH']},
                         'strategy_module': 'tests.engine.backtest.test_local_tradable.algorithm.RW_lv1',
                         'strategy_name': 'RandomWeighting',
                         'params': {
                             # No parameters
                         }}
        run_once(r'tests/engine/backtest/test_local_tradable/config/RW_FX_CN_stock.json',
                 strategy_pool_generator(pool_settings, save=False))
        reader = ResultReader("tests/engine/backtest/test_local_tradable/result/RW_FX_CN_stock")
        output = reader.get_strategy(id_list=list(range(9)), fields=["pnl"])
        # FX_pnl_57 = output[0][0].iloc[-1,0]+ output[1][0].iloc[-1,0] + output[2][0].iloc[-1,0] + output[3][0].iloc[-1,0]
        FX_pnl_00 = output[0][0].iloc[-2, 0] + output[1][0].iloc[-2, 0] + output[2][0].iloc[-2, 0] + \
                    output[3][0].iloc[-2, 0]  # 不计息
        Stock_pnl = output[4][0].iloc[-1, 0] + output[5][0].iloc[-1, 0] + output[6][0].iloc[-1, 0] + \
                    output[7][0].iloc[-1, 0] + output[8][0].iloc[-1, 0]
        assert np.isclose(FX_pnl_00 + Stock_pnl, -132866.678896)



