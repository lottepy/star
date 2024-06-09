'''
This file is to construct forward returns with specified horizon from Config file.
The forward returns are used for model construction.

'''

from algorithm.addpath import config_path, data_path
import pandas as pd
from os import makedirs
from os.path import exists, join
import configparser
from datetime import datetime
from dateutil.relativedelta import relativedelta


def construct_returns_whole():
    nav_path = join(data_path, 'bundles', 'daily')
    ticker_path = join(data_path, 'bundles', 'Summary.csv')
    save_path = join(data_path, 'returns')
    if not exists(save_path):
        makedirs(save_path)

    config_file_path = join(config_path, "config.conf")
    config = configparser.ConfigParser()
    config.read(config_file_path)
    hrz = int(config['model_info']['prediction_horizon'])

    ms_secids = pd.read_csv(ticker_path)
    ms_secids = ms_secids['Name'].tolist()

    nav_list = []
    for ms_secid in ms_secids:
        try:
            nav_tmp_path = join(nav_path, str(ms_secid) + ".csv")
            nav_tmp = pd.read_csv(nav_tmp_path, parse_dates=[0], index_col=0)
            nav_tmp = nav_tmp[['close']].rename(columns={'close': ms_secid})
            nav_tmp = nav_tmp.resample('1M').last().ffill()
            nav_list.append(nav_tmp)
        except:
            print("Fail for " + str(ms_secid))
    nav = pd.concat(nav_list, axis=1)
    output_path = join(save_path, str(hrz) + 'M_return.csv')
    fwd_returns = nav.shift(-hrz) / nav - 1
    fwd_returns.to_csv(output_path)


def construct_mean_category_returns():
    nav_path = join(data_path, 'bundles', 'daily')
    ticker_path = join(data_path, 'bundles', 'Summary.csv')
    return_path = join(data_path, 'returns')

    config_file_path = join(config_path, "config.conf")
    config = configparser.ConfigParser()
    config.read(config_file_path)
    hrz = int(config['model_info']['time_series_prediction_hrz'])

    ms_secids = pd.read_csv(ticker_path)
    ms_secids = ms_secids['Name'].tolist()
    nav_list = []
    for ms_secid in ms_secids:
        try:
            nav_tmp_path = join(nav_path, str(ms_secid) + ".csv")
            nav_tmp = pd.read_csv(nav_tmp_path, parse_dates=[0], index_col=0)
            nav_tmp = nav_tmp[['close']].rename(columns={'close': ms_secid})
            nav_tmp = nav_tmp.resample('1M').last().ffill()
            nav_list.append(nav_tmp)
        except:
            print("Fail for " + str(ms_secid))
    nav = pd.concat(nav_list, axis=1)
    ret_tmp = nav.shift(-hrz) / nav - 1

    start_date = datetime.strptime(config['model_info']['time_series_prediction_sample_start_date'], '%Y-%m-%d')
    end_date = datetime.today() - relativedelta(months=hrz)
    dates = pd.date_range(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'), freq='M')

    category_path = join(data_path, 'categorization', 'fund_category')

    category_fwd_return_list = []
    category_fwd_return_std_list = []
    for date in dates:
        category_df = pd.read_csv(join(category_path, date.strftime("%Y-%m-%d") + '.csv'), index_col=0)
        ret = ret_tmp.loc[date.strftime('%Y-%m-%d'), :]
        category_df['fwd_return'] = ret
        category_fwd_return_tmp = pd.DataFrame(category_df.groupby('category').fwd_return.mean()).transpose()
        category_fwd_return_tmp.index = [date]
        category_fwd_return_list.append(category_fwd_return_tmp)
        category_fwd_return_std_tmp = pd.DataFrame(category_df.groupby('category').fwd_return.std()).transpose()
        category_fwd_return_std_tmp.index = [date]
        category_fwd_return_std_list.append(category_fwd_return_std_tmp)

    category_fwd_return = pd.concat(category_fwd_return_list)
    output_path1 = join(return_path, 'category_fwd_return_mean.csv')
    category_fwd_return.to_csv(output_path1)
    category_fwd_return_std = pd.concat(category_fwd_return_std_list)
    output_path2 = join(return_path, 'category_fwd_return_std.csv')
    category_fwd_return_std.to_csv(output_path2)


if __name__ == "__main__":
    construct_returns_whole()
    construct_mean_category_returns()
