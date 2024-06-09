from quantcycle.utils.strategy_pool_generator import strategy_pool_generator
from quantcycle.utils.run_once import run_once
from datetime import datetime


def generate_rsi_strategy_pool():
    pool_setting = {"symbol": {"STOCKS": ['600016.SH', '000005.SZ', '601398.SH', '600028.SH', '601628.SH',
                                          '002024.SZ', '000100.SZ']},
                    "strategy_module": "examples.strategy.stocks.algorithm.RSI_lv1",
                    "strategy_name": "RSI_strategy",
                    "params": {
                                    "length": [10],
                                    "break_threshold": [10, 20, 30],
                                    "stop_profit": [0.01],
                                    "stop_loss": [0.005],
                                    "max_hold_days": [10]
                              }}
    return strategy_pool_generator(pool_setting, save=False)


def generate_kd_strategy_pool():
    pool_setting = {"symbol": {"STOCKS": ['600000.SH', '000005.SZ', '600519.SH', '601318.SH', '601988.SH',
                                          '002415.SZ', '000917.SZ']},
                    "strategy_module": "examples.strategy.stocks.algorithm.KD_lv1",
                    "strategy_name": "KD_strategy",
                    "params": {
                                    "length1": [10, 20],
                                    "length2": [3],
                                    "length3": [3],
                                    "break_threshold": [10, 20],
                                    "stop_profit": [0.01],
                                    "stop_loss": [0.005],
                                    "max_hold_days": [10]
                              }}
    return strategy_pool_generator(pool_setting, save=False)


if __name__ == "__main__":
    rsi_df = generate_rsi_strategy_pool()
    kd_df = generate_kd_strategy_pool()

    run_once('examples/strategy/stocks/config/RSI_stock_lv1.json', rsi_df)
    run_once('examples/strategy/stocks/config/KD_stock_lv1.json', kd_df)
