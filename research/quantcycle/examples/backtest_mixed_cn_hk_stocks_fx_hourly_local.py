from quantcycle.utils.strategy_pool_generator import strategy_pool_generator
from quantcycle.utils.run_once import run_once


def run_RW_FX_csv_mixed_stocks():
    pool_setting = {"symbol": {"FX": ['AUDCAD', 'AUDJPY', 'EURUSD', 'USDJPY', 'USDKRW'],
                               'STOCKS': ['000005.SZ', '600016.SH', '600028.SH', '601398.SH', '601628.SH',
                                          "0005.HK", "1299.HK", "2388.HK",
                                          '2813.HK', '3010.HK', '3077.HK', '3081.HK']},
                    "strategy_module": "examples.strategy.mixed.algorithm.RW_lv1",
                    "strategy_name": "RandomWeighting",
                    "params": {
                        # EW has no parameters
                    }}

    run_once('examples/strategy/mixed/config/RW_FX_csv_mixed_stocks.json',
             strategy_pool_generator(pool_setting, save=False))


if __name__ == '__main__':
    run_RW_FX_csv_mixed_stocks()
