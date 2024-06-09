from algorithm import addpath
import multiprocessing
from datetime import datetime
import pandas as pd
import os
import numpy as np
import statsmodels.api as sm


def discretionary_accruals(insample):
    temp_discretionary_accruals = insample.loc[:, ['TOTALACCRUAL', 'CashSales_Chg', 'PPETA']]
    temp_discretionary_accruals = temp_discretionary_accruals.dropna()
    temp_discretionary_accruals = temp_discretionary_accruals[~temp_discretionary_accruals.isin([np.nan, np.inf, -np.inf]).any(1)]
    if temp_discretionary_accruals.shape[0] >= 4:
        X = sm.add_constant(temp_discretionary_accruals.loc[:, ['CashSales_Chg', 'PPETA']])
        Y = temp_discretionary_accruals['TOTALACCRUAL']
        model = sm.OLS(endog=Y.astype(float), exog=X.astype(float), missing='drop').fit()
        discrete_accruals = model.resid

        return discrete_accruals


def cs_factor_helper(date):
    factor_path = os.path.join(addpath.data_path, "Ashare", "factors")
    cs_factor_path = os.path.join(addpath.data_path, "Ashare", "cs_factors")

    factor_list = pd.read_csv(os.path.join(addpath.config_path, "ashare_factor_list.csv"))['factor_list'].tolist()
    industry_map = pd.read_csv(os.path.join(addpath.config_path, "ashare_symbol_list.csv"), index_col='Stkcd')[['AQM_Category']]
    industries = list(set(industry_map['AQM_Category'].tolist()))

    factor_files = os.listdir(factor_path)
    result_list = []
    for factor_file in factor_files:
        try:
            tmp = pd.read_csv(os.path.join(factor_path, factor_file), index_col='end_date', parse_dates=['end_date'])
            flist = [f for f in factor_list if f in tmp.columns]
            flist.append('fwd_return_qtr')
            flist.append('fwd_return_semiyear')
            flist.append('fwd_return_annyear')
            flist.append('CashSales_Chg')
            flist.append('PPETA')
            tmp = tmp.resample('1M').last().ffill()
            tmp = tmp.loc[date, flist]
            tmp = pd.DataFrame(tmp)
            tmp.columns = [factor_file[:-4]]
            result_list.append(tmp)
        except:
            print("No data on " + date.strftime('%Y-%m-%d') + " for factors from symbol " + factor_file[:-4])

    result = pd.concat(result_list, axis=1)
    result = result.transpose()
    result = result.dropna(axis=0, how='all')

    result = pd.concat([result, industry_map], axis=1)
    da_list = []
    for industry in industries:
        tmp = result[result['AQM_Category'] == industry]
        try:
            da = discretionary_accruals(tmp)
        except:
            da = tmp['TOTALACCRUAL']
            da = da.map(lambda x: np.nan)
        da_list.append(da)
    result['discrete_accruals'] = pd.concat(da_list)

    result.to_csv(os.path.join(cs_factor_path, date.strftime('%Y-%m-%d') + '.csv'), encoding='UTF-8-sig')


def cs_factor_main(start, end):
    formation_dates = pd.date_range(start, end, freq='1M')

    pool = multiprocessing.Pool(15)
    for date in formation_dates[20:]:
        pool.apply_async(cs_factor_helper, args=(date,))
        print('Sub-processes begins!')
    pool.close()  # 关闭进程池，表示不能再往进程池中添加进程，需要在join之前调用
    pool.join()  # 等待进程池中的所有进程执行完毕
    print("Sub-process(es) done.")


if __name__ == "__main__":
    start = '2008-05-31'
    end = '2020-12-31'
    cs_factor_main(start, end)
