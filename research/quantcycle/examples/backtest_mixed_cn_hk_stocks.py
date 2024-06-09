from quantcycle.utils.strategy_pool_generator import strategy_pool_generator
from quantcycle.utils.run_once import run_once


def run_mixed_stock_precision_usd():
    pool_setting = {"symbol": {"STOCKS": ["600016.SH", "2388.HK", "1299.HK", "601318.SH"]},
                    "strategy_module": "examples.strategy.stocks.algorithm.EW_lv1_adj",
                    "strategy_name": "EW_strategy",
                    "params": {
                        # No parameters
                    }}

    run_once('examples/strategy/stocks/config/EW_mixed_CN_HK_usd.json',
             strategy_pool_generator(pool_setting, save=False))


if __name__ == '__main__':
    run_mixed_stock_precision_usd()
