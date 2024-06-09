import pandas as pd
import os
import datetime
import algorithm.addpath as addpath
from math import isnan

def ashare_bundle_creation():
    input_path = os.path.join(addpath.data_path, "Ashare", "trading_data")
    bundle_path = os.path.join(addpath.data_path, "Backtest", "bundles")
    interest_path = os.path.join(addpath.data_path, "Backtest", "interest")
    fxrate_path = os.path.join(addpath.data_path, "Backtest", "fxrate")

    files = os.listdir(input_path)

    for file in files:
        print(file)
        tmp = pd.read_csv(os.path.join(input_path, file), parse_dates=[0], index_col=0)
        tmp = tmp.ffill()
        bundle_data = pd.DataFrame(index=tmp.index)
        bundle_data['open'] = tmp['open']
        bundle_data['high'] = tmp['high']
        bundle_data['low'] = tmp['low']
        bundle_data['close'] = tmp['PX_LAST']
        bundle_data['volume'] = tmp['PX_VOLUME']
        bundle_data.index.name = 'date'
        bundle_data.to_csv(os.path.join(bundle_path, file))

        interest_data = pd.DataFrame(index=tmp.index)
        interest_data['interest_rate'] = 0
        interest_data.to_csv(os.path.join(interest_path, file))

        fxrate_data = pd.DataFrame(index=tmp.index)
        fxrate_data['fx_rate'] = 1
        fxrate_data.to_csv(os.path.join(fxrate_path, file))


if __name__ == "__main__":
    ashare_bundle_creation()