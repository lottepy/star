from algorithm import addpath
import multiprocessing
from datetime import datetime
import pandas as pd
import os
import numpy as np
import statsmodels.api as sm


def cs_factor_helper(date):
    factor_path = os.path.join(addpath.data_path, "Ashare", "monthly_prior", "trading_factors")
    cs_factor_path = os.path.join(addpath.data_path, "Ashare", "monthly_prior", "cs_trading_factors")

    factor_list = pd.read_csv(os.path.join(addpath.config_path, "ashare_trading_factor_list.csv"))['factor_list'].tolist()

    factor_files = os.listdir(factor_path)
    result_list = []
    for factor_file in factor_files:
        try:
            tmp = pd.read_csv(os.path.join(factor_path, factor_file), index_col=0, parse_dates=[0])
            flist = [f for f in factor_list if f in tmp.columns]
            flist.append('ret_forward')
            tmp = tmp.loc[date, flist]
            tmp = pd.DataFrame(tmp)
            tmp.columns = [factor_file[:-4]]
            result_list.append(tmp)
        except:
            print("No data on " + date.strftime('%Y-%m-%d') + " for factors from symbol " + factor_file[:-4])

    result = pd.concat(result_list, axis=1)
    result = result.transpose()
    result = result.dropna(axis=0, how='all')
    result.to_csv(os.path.join(cs_factor_path, date.strftime('%Y-%m-%d') + '.csv'))


def cs_factor_main(start, end):
    formation_dates = pd.date_range(start, end, freq='1M')

    pool = multiprocessing.Pool(15)
    for date in formation_dates:
        pool.apply_async(cs_factor_helper, args=(date,))
        print('Sub-processes begins!')
    pool.close()  # 关闭进程池，表示不能再往进程池中添加进程，需要在join之前调用
    pool.join()  # 等待进程池中的所有进程执行完毕
    print("Sub-process(es) done.")


if __name__ == "__main__":
    start = '2008-01-01'
    end = '2020-11-30'
    cs_factor_main(start, end)