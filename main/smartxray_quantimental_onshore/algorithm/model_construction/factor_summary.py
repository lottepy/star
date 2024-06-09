'''
This file is to construct the masterfile of standardized factors, which is for model construction.
The factors are winsorized at 2.5% and 97.5% in cross section and then normalized in cross section.
'''

from algorithm import addpath
import pandas as pd
from os import makedirs
import os
import configparser
import multiprocessing
from datetime import datetime
from constant import funda_factor_list, nav_factor_list, other_factor_list
import numpy as np


def cal_helper(dates_tmp, save_path):
    fund_list = pd.read_csv(addpath.ref_path, parse_dates=['obsolete_date', 'inception_date'], index_col=0)
    for date in dates_tmp:
        result_list = []
        fund_list_tmp = pd.concat([fund_list[fund_list['obsolete_date'] > date], fund_list[pd.isna(fund_list['obsolete_date'])]])
        fund_list_tmp = fund_list_tmp[fund_list_tmp['inception_date'] < date].index.tolist()
        for factor_category in funda_factor_list:
            try:
                path = os.path.join(addpath.historical_path, 'fundamental_factor', factor_category, date.strftime('%Y-%m-%d') + '.csv')
                tmp = pd.read_csv(path, index_col=0)
                if factor_category == 'tail_risk_factor':
                    tmp = tmp.abs()
                if factor_category == 'turnover_ratio':
                    tmp[tmp < 0.0] = np.nan
                # if factor_category == 'average_tenure':
                #     tmp = tmp / 365.0
                result_list.append(tmp)
            except:
                print("No data on " + date.strftime('%Y-%m-%d') + " for factors from category " + factor_category)

        for factor_category in nav_factor_list:
            try:
                path = os.path.join(addpath.historical_path, 'nav_factor', factor_category, date.strftime('%Y-%m-%d') + '.csv')
                tmp = pd.read_csv(path, index_col=0)
                tmp.columns = [col[:col.index('mometum') + len('mome')] + 'n' + col[col.index('mometum') + len('mome'):]
                               if 'mometum' in col else col for col in tmp.columns]
                result_list.append(tmp)
            except:
                print("No data on " + date.strftime('%Y-%m-%d') + " for factors from category " + factor_category)

        for factor_category in other_factor_list:
            try:
                path = os.path.join(addpath.historical_path, factor_category, date.strftime('%Y-%m-%d') + '.csv')
                tmp = pd.read_csv(path, index_col=0)
                if 'average_tenure' in tmp.columns:
                    tmp = tmp.rename(columns={'average_tenure': 'pm_historical_average_tenure'})
                    # tmp.loc[:, 'pm_historical_average_tenure'] = tmp['pm_historical_average_tenure'] / 365.0
                result_list.append(tmp)
            except:
                print("No data on " + date.strftime('%Y-%m-%d') + " for factors from category " + factor_category)

        result = pd.concat(result_list, axis=1)

        index = [idx for idx in result.index if idx in fund_list_tmp]
        result = result.loc[index, :]

        result = result.dropna(axis=0, how='all')
        result = result.dropna(axis=1, how='all')
        result.drop(columns=['company'], inplace=True)
        result.to_csv(os.path.join(save_path, date.strftime('%Y-%m-%d') + '.csv'), encoding='utf-8-sig')


def factor_multiprocess(start_date, end_date):
    save_path = os.path.join(addpath.model_path, 'cs_factor')
    if not os.path.exists(save_path):
        makedirs(save_path)

    # start_date = datetime.strptime(start_date, "%Y-%m-%d")
    # end_date = datetime.strptime(end_date, "%Y-%m-%d")

    dates = pd.date_range(start_date, end_date, freq='M').tolist()

    num_pms = len(dates)
    chunk_size = 10
    chunk_number = int(num_pms / chunk_size) + 1

    dates_dict = {}
    for chunk in range(chunk_number):
        if chunk == chunk_number - 1:
            ub = num_pms
        else:
            ub = (chunk + 1) * chunk_size
        lb = chunk * chunk_size
        dates_dict[chunk] = dates[lb:ub]

    pool = multiprocessing.Pool()
    result_list = []
    for chunk in range(chunk_number):
        result_list.append(pool.apply_async(cal_helper, args=(dates_dict[chunk], save_path,)))
        print('Sub-processes begins!')
    pool.close()  # 关闭进程池，表示不能再往进程池中添加进程，需要在join之前调用
    pool.join()  # 等待进程池中的所有进程执行完毕
    print("Sub-process(es) done.")

if __name__ == "__main__":
    factor_multiprocess(start_date='2014-12-31', end_date='2020-12-31')
