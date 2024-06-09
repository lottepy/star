'''
This file is to construct the masterfile of standardized factors, which is for model construction.
The factors are winsorized at 2.5% and 97.5% in cross section and then normalized in cross section.
'''

from algorithm.addpath import config_path, data_path
import pandas as pd
from os import makedirs
from os.path import exists, join
import configparser
from datetime import datetime
from constant import cs_factor_category_list
import numpy as np

def factor_standardization(end_date):
    factor_path = join(data_path, 'factors')
    save_path1 = join(factor_path, 'cs_factor_std')
    if not exists(save_path1):
        makedirs(save_path1)
    save_path2 = join(factor_path, 'time_series_factor', 'raw')
    if not exists(save_path2):
        makedirs(save_path2)

    config_file_path = join(config_path, "config.conf")
    config = configparser.ConfigParser()
    config.read(config_file_path)
    start_date = config['model_info']['factor_data_start_date']
    start_date = datetime.strptime(start_date, "%Y-%m-%d")

    dates = pd.date_range(start_date, end_date, freq='M').tolist()
    factor_mean_list = []
    factor_std_list = []

    for date in dates:
        result_list = []
        for factor_category in cs_factor_category_list:
            try:
                path = join(factor_path, factor_category, date.strftime('%Y-%m-%d') + '.csv')
                tmp = pd.read_csv(path, index_col=0)
                if factor_category == 'beta_factor':
                    tmp = tmp.replace(0, np.nan)
                    tmp = tmp.dropna(axis=0, how='all')
                    tmp = tmp.fillna(0)
                result_list.append(tmp)
            except:
                print("No data on " + date.strftime('%Y-%m-%d') +  " for factors from category " + factor_category)
        result = pd.concat(result_list, axis=1)
        if 'company_id' in result.columns:
            result = result.drop(columns='company_id')
        result = result.dropna(axis=0, how='all')
        winsorized = result.clip(lower=result.quantile(0.025), upper=result.quantile(0.975), axis=1)
        factor_mean_tmp = winsorized.mean()
        factor_std_tmp = winsorized.std()
        standardized = winsorized - factor_mean_tmp
        for idx in standardized.columns:
            standardized[idx] = standardized[idx] / factor_std_tmp[idx]
        standardized.columns = [i + '_zscore' for i in standardized.columns]
        # for i in standardized.columns:
        #     standardized = standardized.rename(columns={i: i + '_zscore'})
        standardized = standardized.dropna(axis=0, how='all')
        output_path1 = join(save_path1, date.strftime('%Y-%m-%d') + '.csv')
        standardized.to_csv(output_path1)
        factor_mean_tmp = pd.DataFrame(factor_mean_tmp).transpose()
        factor_mean_tmp.index = [date]
        factor_mean_list.append(factor_mean_tmp)
        factor_std_tmp = pd.DataFrame(factor_std_tmp).transpose()
        factor_std_tmp.index = [date]
        factor_std_list.append(factor_std_tmp)


    factor_mean = pd.concat(factor_mean_list)
    factor_mean.columns = [i + '_mean' for i in factor_mean.columns]
    output_path2 = join(save_path2, 'factor_mean.csv')
    factor_mean.to_csv(output_path2)
    factor_std = pd.concat(factor_std_list)
    factor_std.columns = [i + '_std' for i in factor_std.columns]
    output_path3 = join(save_path2, 'factor_std.csv')
    factor_std.to_csv(output_path3)


if __name__ == "__main__":
    factor_standardization(end_date=datetime(2020,4,30))