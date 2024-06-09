import json
import itertools
import os
import pandas as pd


def strategy_pool_generator(pool_setting, save=True):
    all_param_combinations = list(itertools.product(*pool_setting['params'].values()))
    param_dict_df = pd.DataFrame(all_param_combinations,
                                 columns=[*pool_setting['params'].keys()])
    param_dict_list = [json.dumps(dict(row)) for index, row in param_dict_df.iterrows()]
    symbol = [json.dumps(pool_setting['symbol'])]

    all_combinations = list(itertools.product([pool_setting['strategy_module']], [pool_setting['strategy_name']], param_dict_list, symbol))
    df = pd.DataFrame(all_combinations, columns=["strategy_module", "strategy_name", "params", "symbol"])
    if save:
        #  存储路径不存在，新建路径
        save_path = pool_setting['save_path']
        save_dir = os.path.dirname(save_path)
        if not os.path.exists(save_dir):
            os.makedirs(save_dir)
        df.to_csv(save_path, index=False)
    return df


if __name__ == '__main__':
    pool_setting = {"symbol":{"FX":['AUDCAD', 'AUDJPY', 'AUDNZD', 'AUDUSD', 'CADCHF', 'CADJPY', 'EURCAD',
                                    'EURCHF', 'EURGBP', 'EURJPY', 'EURUSD', 'GBPCAD', 'GBPCHF', 'GBPJPY',
                                    'GBPUSD', 'NOKSEK', 'NZDJPY', 'NZDUSD', 'USDCAD', 'USDCHF', 'USDIDR',
                                    'USDINR', 'USDJPY', 'USDKRW', 'USDNOK', 'USDSEK', 'USDSGD', 'USDTHB',
                                    'USDTWD'],
                              "STOCK": ['000001.SZ', '000002.HK']},
                    "strategy_module":"examples.RSI_strategy.algorithm.simple_oscillator_strategy",
                    "strategy_name":"RSI_strategy",
                    "save_path":"examples/RSI_strategy/strategy_pool/rsi_strategy_pool_test.csv",
                    "params":{
                                "length" : [10, 15, 20, 40],
                                "break_threshold" :[5, 10, 15, 20, 25, 30],
                                "stop_profit" : [0.01, 0.02],
                                "stop_loss" :[0.005, 0.01, 0.02],
                                "max_hold_days" : [10, 20]
                             }}
    strategy_pool_generator(pool_setting)
