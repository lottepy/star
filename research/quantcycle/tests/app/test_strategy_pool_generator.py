from quantcycle.utils.strategy_pool_generator import strategy_pool_generator




def test_strategy_pool_generator():
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




if __name__ == '__main__':
    test_strategy_pool_generator()