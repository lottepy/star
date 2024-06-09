from quantcycle.utils.strategy_pool_generator import strategy_pool_generator
from quantcycle.utils.run_once import run_once
from quantcycle.app.result_exporter.result_reader import ResultReader
import numpy as np


if __name__ == "__main__":
    pool_setting = {"symbol": {"STOCKS": ['STOCK_1', 'STOCK_2', 'STOCK_3', 'STOCK_4', 'STOCK_5']},
                    "strategy_module": "examples.strategy.stocks.algorithm.EW_lv1",
                    "strategy_name": "EW_strategy",
                    "params": {
                        # EW has no parameters
                    }}

    run_once('examples/strategy/stocks/config/EW_stock_hourly_with_ref.json',
             strategy_pool_generator(pool_setting, save=False))
