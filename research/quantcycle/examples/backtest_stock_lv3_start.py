from quantcycle.utils.strategy_pool_generator import strategy_pool_generator
from quantcycle.utils.run_once import run_once
from datetime import datetime
import numpy as np


def generate_combination_strategy_pool():
    pool_setting = {"symbol": {"STOCKS": ['600016.SH', '000005.SZ', '601398.SH', '600028.SH', '601628.SH',
                                          '002024.SZ', '000100.SZ', '600000.SH', '000005.SZ', '600519.SH',
                                          '601318.SH', '601988.SH', '002415.SZ', '000917.SZ'],
                               "RSI": list(np.arange(1).astype(np.float64)),
                               "KD": list(np.arange(1).astype(np.float64))},
                    "strategy_module": "examples.strategy.stocks.algorithm.combination_lv3",
                    "strategy_name": "Combination_strategy",
                    "params": {

                              }}
    return strategy_pool_generator(pool_setting, save=False)


if __name__ == "__main__":
    combine_df = generate_combination_strategy_pool()

    run_once('examples/strategy/stocks/config/combination_stock_lv3.json', combine_df)
