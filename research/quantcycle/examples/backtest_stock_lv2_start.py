from quantcycle.utils.strategy_pool_generator import strategy_pool_generator
from quantcycle.utils.run_once import run_once
from datetime import datetime
import numpy as np


def generate_rsi_allocation_strategy_pool():
    pool_setting = {"symbol": {"STOCKS": ['600016.SH', '000005.SZ', '601398.SH', '600028.SH',
                                          '601628.SH', '002024.SZ', '000100.SZ'],
                               "RSI": list(np.arange(3 * 7).astype(np.float64))},  #不转成float json会报错
                    "strategy_module": "examples.strategy.stocks.algorithm.RSI_lv2",
                    "strategy_name": "Allocation_strategy",
                    "params": {

                              }}
    return strategy_pool_generator(pool_setting, save=False)


def generate_kd_allocation_strategy_pool():
    pool_setting = {"symbol": {"STOCKS": ['600000.SH', '000005.SZ', '600519.SH', '601318.SH',
                                          '601988.SH', '002415.SZ', '000917.SZ'],
                               "KD": list(np.arange(4 * 7).astype(np.float64))},   #不转成float json会报错
                    "strategy_module": "examples.strategy.stocks.algorithm.KD_lv2",
                    "strategy_name": "Allocation_strategy",
                    "params": {

                              }}
    return strategy_pool_generator(pool_setting, save=False)


if __name__ == "__main__":
    rsi_df = generate_rsi_allocation_strategy_pool()
    kd_df = generate_kd_allocation_strategy_pool()

    run_once('examples/strategy/stocks/config/RSI_stock_lv2.json', rsi_df)
    run_once('examples/strategy/stocks/config/KD_stock_lv2.json', kd_df)
