from quantcycle.utils.strategy_pool_generator import strategy_pool_generator
from quantcycle.utils.run_once import run_once
import numpy as np


def test_signal_remark():
    rsi_lv1_df = generate_rsi_lv1_pool()
    rsi_lv2_df = generate_rsi_lv2_pool()

    run_once('tests/engine/backtest/signal_remark/config/RSI_stock_lv1.json', rsi_lv1_df)
    run_once('tests/engine/backtest/signal_remark/config/RSI_stock_lv2.json', rsi_lv2_df)


def generate_rsi_lv1_pool():
    pool_setting = {"symbol": {"STOCKS": ['600016.SH', '000005.SZ', '601398.SH', '600028.SH', '601628.SH',
                                          '002024.SZ', '000100.SZ']},
                    "strategy_module": "tests.engine.backtest.signal_remark.algorithm.RSI_lv1_remark",
                    "strategy_name": "RSI_strategy",
                    "params": {
                                    "length": [10],
                                    "break_threshold": [10, 20, 30],
                                    "stop_profit": [0.01],
                                    "stop_loss": [0.005],
                                    "max_hold_days": [10]
                              }}
    return strategy_pool_generator(pool_setting, save=False)


def generate_rsi_lv2_pool():
    pool_setting = {"symbol": {"STOCKS": ['600016.SH', '000005.SZ', '601398.SH', '600028.SH',
                                          '601628.SH', '002024.SZ', '000100.SZ'],
                               "RSI": list(np.arange(3 * 7).astype(np.float64))},  # 不转成float json会报错
                    "strategy_module": "tests.engine.backtest.signal_remark.algorithm.RSI_lv2_remark",
                    "strategy_name": "Allocation_strategy",
                    "params": {

                              }}
    return strategy_pool_generator(pool_setting, save=False)
