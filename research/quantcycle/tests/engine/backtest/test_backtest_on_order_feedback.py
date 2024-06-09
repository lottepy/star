import unittest
import numpy as np
import pandas as pd
from quantcycle.utils.strategy_pool_generator import strategy_pool_generator
from quantcycle.app.result_exporter.result_reader import ResultReader
from quantcycle.utils.run_once import run_once

class TestOnOrderFeedback(unittest.TestCase):
    def test_SO_mixed_CN_HK(self):
        pool_settings = {'symbol': {"STOCKS": ["600016.SH", "2388.HK", "1299.HK", "601318.SH"]},
                         'strategy_module': 'tests.engine.backtest.test_on_order_feedback.algorithm.SO',
                         'strategy_name': 'SeparateOrder',
                         'params': {
                             # No parameters
                         }}
        run_once(r'tests/engine/backtest/test_on_order_feedback/config/SO_mixed_CN_HK.json',
                 strategy_pool_generator(pool_settings, save=False))
        reader = ResultReader(r"tests/engine/backtest/test_on_order_feedback/result/SO_mixed_CN_HK")
        output = reader.get_strategy(id_list=range(4), fields=["pnl"])
        res = sum(output[i][0].iloc[-1,0] for i in range(4))
        # assert np.isclose(res, 1782.38991159)
        assert np.isclose(res, 353481.466)



