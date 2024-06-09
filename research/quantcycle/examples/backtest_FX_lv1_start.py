from quantcycle.utils.run_once import run_once
import pandas as pd

if __name__ == "__main__":
    json_path = r'examples/strategy/fx/config/RSI_lv1.json'
    strategy_pool_df = pd.read_csv(
        r'examples/strategy/fx/strategy_pool/RSI_lv1_strategy_pool.csv')
    run_once(json_path, strategy_pool_df)

    json_path = r'examples/strategy/fx/config/KD_lv1.json'
    strategy_pool_df = pd.read_csv(
        r'examples/strategy/fx/strategy_pool/KD_lv1_strategy_pool.csv')
    run_once(json_path, strategy_pool_df)
