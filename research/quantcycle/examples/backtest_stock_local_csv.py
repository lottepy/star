from quantcycle.utils.strategy_pool_generator import strategy_pool_generator
from quantcycle.utils.run_once import run_once
from quantcycle.app.result_exporter.result_reader import ResultReader
import numpy as np


if __name__ == "__main__":
    pool_setting = {"symbol": {
        "STOCKS": ['ABC', 'KKK', 'XYZ']},
        "strategy_module": "examples.strategy.stocks.algorithm.EW_lv1",
        "strategy_name": "EW_strategy",
        "params": {
            # No parameters
        }}
    run_once('examples/strategy/stocks/config/EW_stock_local_csv_lv1.json',
             strategy_pool_generator(pool_setting, save=False))
