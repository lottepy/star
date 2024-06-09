from quantcycle.utils.run_once import run_once
import pandas as pd
import json

if __name__ == "__main__":
    json_path = r'examples/strategy/fx/config/combination.json'
    strategy_pool_df = pd.read_csv(r'examples/strategy/fx/strategy_pool/combination_strategy_pool.csv')
    run_once(json_path, strategy_pool_df)
