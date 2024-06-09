'''

This file is for merge time_series_factors into a monthly sample

'''

from algorithm.addpath import data_path
import pandas as pd
from os import makedirs
from os.path import exists, join
import datetime
from sklearn.preprocessing import StandardScaler


def ts_data_set_preparation():
    factor_path = join(data_path, 'factors')
    input_path = join(factor_path, 'time_series_factor', 'raw')
    save_path = join(factor_path, 'time_series_factor', 'processed')
    if not exists(save_path):
        makedirs(save_path)

    ts_raw = pd.read_csv(join(input_path, "monthly_ts_factor.csv"), parse_dates=[0], index_col=0)
    factor_mean = pd.read_csv(join(input_path, "factor_mean.csv"), parse_dates=[0], index_col=0)
    factor_std = pd.read_csv(join(input_path, "factor_std.csv"), parse_dates=[0], index_col=0)

    ts_factors = pd.concat([ts_raw, factor_mean, factor_std], axis=1)
    ts_factors.to_csv(join(save_path, "ts_factors_for_ts_model.csv"))


if __name__ == "__main__":
    ts_data_set_preparation()