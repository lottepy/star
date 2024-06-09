import numpy as np
from quantcycle.utils.strategy_pool_generator import strategy_pool_generator
from quantcycle.utils.run_once import run_once

if __name__ == '__main__':
    pool_settings = {'symbol': {'FUTURES': ['C_MAIN1', 'C_MAIN2',
                                            'S_MAIN1', 'S_MAIN2']},
                     'strategy_module': 'examples.strategy.futures.algorithm.random_weighting_lv1',
                     'strategy_name': 'RandomWeighting',
                     'params': {
                         # No parameters
                     }}
    run_once(r'examples/strategy/futures/config/random_weighting_local_csv.json',
             strategy_pool_generator(pool_settings, save=False))
