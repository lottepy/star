import numpy as np
from quantcycle.utils.strategy_pool_generator import strategy_pool_generator
from quantcycle.utils.run_once import run_once

if __name__ == '__main__':
    pool_settings = {'symbol': {'FUTURES': ['1603000558', '1603000563',
                                            '1603000557', '1603000562']},
                     'strategy_module': 'examples.strategy.futures.algorithm.random_weighting_lv1',
                     'strategy_name': 'RandomWeighting',
                     'params': {
                         # No parameters
                     }}
    run_once(r'examples/strategy/futures/config/random_weighting_lv1.json',
             strategy_pool_generator(pool_settings, save=False))
