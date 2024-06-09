import numpy as np
import pandas as pd
import os, math
import datetime
import multiprocessing
from algorithm import addpath


def year_factors(start_date, end_date):
    start_time = datetime.datetime.now()
    in_process_path = addpath.temp_data_path
    datapath = addpath.data_path
    bundle_data_path = addpath.bundle_path
    ticker_path = addpath.ref_path
    secid_list = list(pd.read_csv(ticker_path)['ms_secid'].tolist())
    output_path = os.path.join(in_process_path, "total_years")
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    '''read all bundles data'''
    df_list = []
    for secid in secid_list:
        if os.path.exists(os.path.join(bundle_data_path, 'daily', str(secid) + '.csv')):
            start = pd.read_csv(os.path.join(bundle_data_path, 'daily', str(secid) + '.csv'))['date'][0]
            end = pd.read_csv(os.path.join(bundle_data_path, 'daily', str(secid) + '.csv'))['date'].tolist()[-1]
            tmp_df = pd.DataFrame([secid, start, end])
            df_list.append(tmp_df)
    df = pd.concat(df_list, axis=1)
    df = df.T
    df.columns = ['secid', 'start_date', 'end_date']
    df.set_index('secid', inplace=True)
    '''df: columns = ['start_date', 'end_date'], index = ['secid']'''
    print('Finished reading bundles data!')

    dates = list(pd.date_range(start_date, end_date, freq='M').tolist())
    for date in dates:
        print('Processing {}'.format(str(date.date())))
        start_time = datetime.datetime.now()
        tmp_df = df
        '''create a column filled with current date'''
        tmp_df['current_date'] = str(date.date())
        tmp_df['current_date'] = pd.to_datetime(tmp_df['current_date'])
        tmp_df['start_date'] = pd.to_datetime(tmp_df['start_date'])
        tmp_df['end_date'] = pd.to_datetime(tmp_df['end_date'])

        '''create a column filled with 0's'''
        tmp_df['total_years'] = 0
        current_yr = (tmp_df['current_date'].sub(tmp_df['start_date']).dt.days.div(365.25).round(4))
        existing_data_yr = (tmp_df['end_date'].sub(tmp_df['start_date']).dt.days.div(365.25).round(4))
        tmp_df.loc[((tmp_df['current_date'] >= tmp_df['start_date']) & (tmp_df['current_date'] <= tmp_df['end_date'])), ['total_years']] = current_yr # scenario1: start_date <= current_date <= end_date
        tmp_df.loc[tmp_df['end_date'] < tmp_df['current_date'], ['total_years']] = existing_data_yr # scenario2: start_date < end_date < current_date

        '''create a column filled with the active status of the fund'''
        tmp_df['active_flag'] = 0
        tmp_df.loc[tmp_df['current_date'] > tmp_df['start_date'], ['active_flag']] = 1 # scenario1 union scenario2 union inequality

        tmp_df = tmp_df.drop(columns=['start_date', 'end_date', 'current_date'])
        tmp_df.ffill()
        tmp_df.to_csv(os.path.join(output_path, str(date.date()) + '.csv'))

    end_time = datetime.datetime.now()
    print('Foundation years calculation finished in %s' % str(end_time-start_time))


def company_mapping():
    fund_company_mapping_path = os.path.join(addpath.historical_path, 'fund_company', "company_mapping_raw.csv")
    fund_company_mapping = pd.read_csv(fund_company_mapping_path)
    fund_company_mapping = fund_company_mapping[fund_company_mapping['provider_company_status'] == 1]
    fund_company_mapping = fund_company_mapping.groupby('provider_company_id')
    output = pd.DataFrame(columns=['provider', 'funds'])
    count = 0
    for key, item in fund_company_mapping:
        output.loc[count] = [key, list(set(list(item['Unnamed: 0'])))]
        count += 1
    return output


def factor_category_calculation(funds_in_company, cate_df, category, factor_tmp, factor_name):
    factor_company = []
    for fund in funds_in_company:
        if fund in factor_tmp.index and fund in cate_df.index:
            if cate_df.loc[fund]['category'] == category:
                accum_factor_fund = factor_tmp.loc[fund, factor_name]
                factor_company.append(accum_factor_fund)
        else:
            continue
    return factor_company
def factor_calculation_method(factors, method='avg'):
    if len(factors) == 0:
        return np.nan
    else:
        if method == 'avg':
            if np.all(np.isnan(factors)):
                return np.nan
            else:
                return np.nanmean(factors)
        elif method == 'max':
            return np.nanmax(factors)
        elif method == 'sum':
            if np.all(np.isnan(factors)):
                return np.nan
            else:
                return np.nansum(factors)


def cate_calculator(cate, row, category_tmp, aum_tmp, return_factor_tmp, volatility_factor_tmp, mdd_factor_tmp,
                    flag_tmp, factor_output):
    aum = factor_category_calculation(row['funds'], category_tmp, cate, aum_tmp, 'aum')
    ret_12M = factor_category_calculation(row['funds'], category_tmp, cate, return_factor_tmp, 'return_factor_12M')
    ret_36M = factor_category_calculation(row['funds'], category_tmp, cate, return_factor_tmp, 'return_factor_36M')
    vol_12M = factor_category_calculation(row['funds'], category_tmp, cate, volatility_factor_tmp,
                                          'volatility_factor_12M')
    vol_36M = factor_category_calculation(row['funds'], category_tmp, cate, volatility_factor_tmp,
                                          'volatility_factor_36M')
    mdd_12M = factor_category_calculation(row['funds'], category_tmp, cate, mdd_factor_tmp,
                                          'maximum_drawdown_factor_12M')
    mdd_36M = factor_category_calculation(row['funds'], category_tmp, cate, mdd_factor_tmp,
                                          'maximum_drawdown_factor_36M')

    for fund in row['funds']:
        if fund in flag_tmp.index:
            if flag_tmp.loc[fund, 'active_flag'] == 1:
                # factor_output.loc[fund, 'company_' + cate] = row['provider']
                factor_output.loc[fund, 'company_num_funds_' + cate] = len(aum)
                if len(aum):
                    factor_output.loc[fund, 'company_total_aum_' + cate] = factor_calculation_method(aum, method='sum')
                    factor_output.loc[fund, 'company_average_aum_' + cate] = 1.0 * factor_calculation_method(aum,
                                                                                                            method='sum') / len(
                        aum)
                else:
                    factor_output.loc[fund, 'company_total_aum_' + cate] = np.nan
                    factor_output.loc[fund, 'company_average_aum_' + cate] = np.nan
                factor_output.loc[fund, 'return_factor_12M_' + cate] = factor_calculation_method(ret_12M)
                factor_output.loc[fund, 'return_factor_36M_' + cate] = factor_calculation_method(ret_36M)
                factor_output.loc[fund, 'volatility_factor_12M_' + cate] = factor_calculation_method(vol_12M)
                factor_output.loc[fund, 'volatility_factor_36M_' + cate] = factor_calculation_method(vol_36M)
                factor_output.loc[fund, 'maximum_drawdown_factor_12M_' + cate] = factor_calculation_method(mdd_12M)
                factor_output.loc[fund, 'maximum_drawdown_factor_36M_' + cate] = factor_calculation_method(mdd_36M)
            else:
                continue
        else:
            continue

    return factor_output

def company_calculator(category_list, row, category_tmp, aum_tmp, return_factor_tmp, volatility_factor_tmp,
                       mdd_factor_tmp, flag_tmp):
    result_ls = []
    for cate in category_list:
        factor_output = pd.DataFrame()
        # dataset_output = pd.DataFrame()
        tmp1 = cate_calculator(cate, row, category_tmp, aum_tmp, return_factor_tmp, volatility_factor_tmp, mdd_factor_tmp, flag_tmp, factor_output)
        result_ls.append(tmp1)

    # 整理结果
    factor_tmp_list = []
    for (i, cate) in enumerate(category_list):
        factor_tmp_list.append(result_ls[i])

    # 不同cate结果concat在一起
    factor_tmp = pd.concat(factor_tmp_list, axis=1)
    # merge column 'company'
    factor_tmp['company'] = row['provider']

    return factor_tmp

def company_factors_mapping(start_date, end_date):
    providers = company_mapping()

    aum_path = os.path.join(addpath.historical_path, 'aum_info')
    in_process_path = os.path.join(addpath.temp_data_path, 'total_years')
    characteristic_path = os.path.join(addpath.historical_path, 'nav_factor')
    return_path = os.path.join(characteristic_path, "return_factor")
    volatility_path = os.path.join(characteristic_path, "volatility_factor")
    mdd_path = os.path.join(characteristic_path, "maximum_drawdown_factor")
    category_data_path = os.path.join(addpath.historical_path, 'categorization', 'fund_category')

    factor_save_path = os.path.join(addpath.historical_path, 'fund_company_factor')

    if not os.path.exists(factor_save_path):
        os.makedirs(factor_save_path)
    category_list = ['Alternative', 'Bond', 'Money', 'Equity', 'Others']

    dates = pd.date_range(start_date, end_date, freq='M').tolist()
    for date in dates:
        # print(date)
        start_time = datetime.datetime.now()
        date = datetime.datetime.strftime(date, "%Y-%m-%d")

        aum_tmp = pd.read_csv(os.path.join(aum_path, str(date)+'.csv'), index_col=0)
        return_factor_tmp = pd.read_csv(os.path.join(return_path, str(date)+'.csv'), index_col=0)
        volatility_factor_tmp = pd.read_csv(os.path.join(volatility_path, str(date) + '.csv'), index_col=0)
        mdd_factor_tmp = pd.read_csv(os.path.join(mdd_path, str(date) + '.csv'), index_col=0)
        # 用于分类和判断基金是否还活着
        category_tmp = pd.read_csv(os.path.join(category_data_path, str(date)+'.csv'), index_col=0)
        flag_tmp = pd.read_csv(os.path.join(in_process_path, str(date)+'.csv'), index_col=0)

        factor_output_list = []
        pool = multiprocessing.Pool()
        result_ls = []

        print('{} multiprocess begins'.format(date))
        for index, row in providers.iterrows():
            result_ls.append(pool.apply_async(company_calculator, args=(category_list, row, category_tmp, aum_tmp,
                                                                        return_factor_tmp, volatility_factor_tmp,
                                                                        mdd_factor_tmp, flag_tmp, )))
        pool.close()
        pool.join()

        for index, row in providers.iterrows():
            factor_output_list.append(result_ls[index].get())
        factor_output = pd.concat(factor_output_list, axis=0)
        factor_output.to_csv(os.path.join(factor_save_path, str(date)+'.csv'))

        end_time = datetime.datetime.now()
        print("%s finished in %s sec" % ((str(date) + '.csv'), str(end_time - start_time)))

if __name__ == "__main__":
    start_date = "2014-12-31"
    end_date = "2020-12-31"
    # year_factors(start_date, end_date)
    company_factors_mapping(start_date, end_date)
