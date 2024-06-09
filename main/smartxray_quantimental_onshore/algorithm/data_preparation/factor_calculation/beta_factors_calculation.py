from constant import *
import numpy as np
import pandas as pd
from os import makedirs
import datetime, os
from dateutil.relativedelta import relativedelta
import multiprocessing
from algorithm import addpath
np.seterr(divide='ignore', invalid='ignore')

def replace_dot(x):
    if x == '.':
        return np.nan
    else:
        return x


def prepare_ts_factor():
    tsfactor_raw_data_path = os.path.join(addpath.historical_path, 'beta_factor', 'raw')
    tsfactor_pro_data_path = os.path.join(addpath.historical_path, 'beta_factor', 'processed')
    market_index = market_index_onshore

    daily_data_path = os.path.join(tsfactor_raw_data_path, "daily.csv")
    daily_data = pd.read_csv(daily_data_path, parse_dates=[0], index_col=0)
    for i in daily_data.columns:
        daily_data[i] = daily_data[i].map(replace_dot)
    daily_data = daily_data.ffill()
    daily_data['CNTERM'] = daily_data['GCNY10YR Index'] - daily_data['GCNY1YR Index']
    daily_data = daily_data.astype(float)

    monthly_data_path = os.path.join(tsfactor_raw_data_path, "monthly.csv")
    monthly_data = pd.read_csv(monthly_data_path, parse_dates=[0], index_col=0)
    for i in monthly_data.columns:
        monthly_data[i] = monthly_data[i].map(replace_dot)
        if i == 'CPI YOY Index' or i == 'CNCPIYOY Index':
            monthly_data[i] = monthly_data[i].shift()
    monthly_data = monthly_data.ffill()
    monthly_data = monthly_data.astype(float)
    monthly_data = monthly_data.resample('1m').last()

    quarterly_data_path = os.path.join(tsfactor_raw_data_path, "quarterly.csv")
    quarterly_data = pd.read_csv(quarterly_data_path, parse_dates=[0], index_col=0)
    for i in quarterly_data.columns:
        quarterly_data[i] = quarterly_data[i].map(replace_dot)
    quarterly_data = quarterly_data.ffill()
    quarterly_data = quarterly_data.astype(float)
    quarterly_data = quarterly_data.resample('Q').last()

    volume_data_path = os.path.join(tsfactor_raw_data_path, "volume.csv")
    volume_data = pd.read_csv(volume_data_path, parse_dates=[0], index_col=0)
    for i in volume_data.columns:
        volume_data[i] = volume_data[i].map(replace_dot)
    volume_data = volume_data.ffill()
    volume_data = volume_data.astype(float)

    for i in market_index:
        if i in volume_data.columns:
            daily_data[i + '_logvolume'] = volume_data[i].map(np.log)
            daily_data[i + '_amihud'] = abs(daily_data[i].pct_change()) / volume_data[i].map(np.log) * 1000000
        else:
            pass
        monthly_data[i + '_rv'] = daily_data[i].pct_change().resample('1M').std()

    for i in daily_data.columns:
        daily_data[i + '_change'] = daily_data[i] - daily_data[i].shift()
        daily_data[i + '_growth'] = daily_data[i].pct_change()

    for i in monthly_data.columns:
        monthly_data[i + '_change'] = monthly_data[i] - monthly_data[i].shift()
        monthly_data[i + '_growth'] = monthly_data[i].pct_change()

    for i in quarterly_data.columns:
        quarterly_data[i + '_change'] = quarterly_data[i] - quarterly_data[i].shift()
        quarterly_data[i + '_growth'] = quarterly_data[i].pct_change()

    daily_data = daily_data.replace([np.inf, -np.inf], np.nan)
    monthly_data = monthly_data.replace([np.inf, -np.inf], np.nan)
    quarterly_data = quarterly_data.replace([np.inf, -np.inf], np.nan)

    daily_data.to_csv(os.path.join(tsfactor_pro_data_path, "daily.csv"))
    monthly_data.to_csv(os.path.join(tsfactor_pro_data_path, "monthly.csv"))
    quarterly_data.to_csv(os.path.join(tsfactor_pro_data_path, "quarterly.csv"))


def betas_helper(chunk, chunk_number, chunk_output_path, num_funds, chunk_size, ms_secids, navpath, trading_calender,
                 daily_factor_raw, monthly_factor_raw, quarterly_factor_raw, daily_factor_list_for_beta_estimation,
                 monthly_factor_list_for_beta_estimation, quarterly_factor_list_for_beta_estimation):
    output_path = os.path.join(chunk_output_path, "chunk_" + str(chunk))
    if os.path.exists(output_path):
        pass
    else:
        makedirs(output_path)

    if chunk == chunk_number - 1:
        ub = num_funds
    else:
        ub = (chunk + 1) * chunk_size
    lb = chunk * chunk_size

    nav_list = []
    for ms_secid in ms_secids[lb:ub]:
        try:
            nav_tmp_path = os.path.join(navpath, str(ms_secid) + ".csv")
            nav_tmp = pd.read_csv(nav_tmp_path, parse_dates=[0], index_col=0)
            nav_tmp = nav_tmp[['close']].rename(columns={'close': ms_secid})
            nav_tmp = nav_tmp.ffill()
            nav_list.append(nav_tmp)
        except:
            print("Fail for " + str(ms_secid))
    nav = pd.concat(nav_list, axis=1)
    trading_days = [d for d in nav.index if d in trading_calender]
    nav = nav.loc[trading_days, :]

    daily_returns = nav.pct_change()
    daily_returns = daily_returns.dropna(axis=0, how='all')
    daily_beta_raw = pd.merge(daily_returns, daily_factor_raw, left_index=True, right_index=True)
    daily_beta_raw = daily_beta_raw.dropna(axis=0, how='all')

    monthly_returns = nav.resample('1M').last().ffill().pct_change()
    monthly_returns = monthly_returns.dropna(axis=0, how='all')
    monthly_beta_raw = pd.merge(monthly_returns, monthly_factor_raw, left_index=True, right_index=True)
    monthly_beta_raw = monthly_beta_raw.dropna(axis=0, how='all')

    quarterly_returns = nav.resample('Q').last().ffill().pct_change()
    quarterly_returns = quarterly_returns.dropna(axis=0, how='all')
    quarterly_beta_raw = pd.merge(quarterly_returns, quarterly_factor_raw, left_index=True, right_index=True)
    quarterly_beta_raw = quarterly_beta_raw.dropna(axis=0, how='all')

    daily_rolling_calender_window = 365
    daily_min_p = 189
    daily_3M_rolling_calender_window = 90
    daily_3M_min_p = 21

    monthly_rolling_calender_window = 1826
    monthly_min_p = 36
    quarterly_rolling_calender_window = 1826
    quarterly_min_p = 15

    start_date = daily_beta_raw.index[0] + datetime.timedelta(days=daily_rolling_calender_window - 1)
    end_date = daily_beta_raw.resample('1M').last().index[-1]  # TODO
    dates = pd.date_range(start_date, end_date, freq='M').tolist()

    for date in dates:
        ub_date = date
        lb_date = ub_date - datetime.timedelta(days=daily_rolling_calender_window - 1)
        daily_datain = daily_beta_raw.loc[lb_date: ub_date, :]

        daily_ret = daily_datain.loc[:, daily_returns.columns]
        daily_fs = daily_datain.loc[:, daily_factor_list_for_beta_estimation]

        daily_ret_count = daily_ret.count()
        daily_fs_count = daily_fs.count()
        daily_ret_mean = daily_ret.fillna(0).sum() / daily_ret_count
        daily_fs_mean = daily_fs.fillna(0).sum() / daily_fs_count

        daily_ret = daily_ret - daily_ret_mean
        daily_fs = daily_fs - daily_fs_mean

        daily_ret_mx = daily_ret.fillna(0).loc[:, :].values
        daily_fs_mx = daily_fs.fillna(0).loc[:, :].values

        daily_ret_indictor = daily_ret.replace(0, 1).loc[:, :].values
        daily_ret_indictor = daily_ret_indictor / daily_ret_indictor
        daily_fs_indictor = daily_fs.replace(0, 1).loc[:, :].values
        daily_fs_indictor = daily_fs_indictor / daily_fs_indictor

        daily_ret_indictor[np.isnan(daily_ret_indictor)] = 0
        daily_fs_indictor[np.isnan(daily_fs_indictor)] = 0

        daily_count = np.dot(daily_ret_indictor.transpose(), daily_fs_indictor)

        daily_screen = np.dot(daily_ret_indictor.transpose(), daily_fs_indictor)
        daily_screen[daily_screen < daily_min_p] = np.nan
        daily_screen = daily_screen / daily_screen

        x = np.dot(daily_ret_mx.transpose(), daily_fs_mx) / (daily_count - 1)
        y = np.dot(daily_fs_mx.transpose(), daily_fs_mx) / (
                    np.dot(daily_fs_indictor.transpose(), daily_fs_indictor) - 1)
        var = [y[i, i] for i in range(len(y))]
        var = np.array(var)

        daily_factor_list_for_beta_estimation_12M = [i + '_12M' for i in daily_factor_list_for_beta_estimation]
        daily_betas_tmp = x / var * daily_screen
        daily_betas_tmp = pd.DataFrame(daily_betas_tmp, columns=daily_factor_list_for_beta_estimation_12M,
                                       index=daily_returns.columns)

        ub_date = date
        lb_date = ub_date - datetime.timedelta(days=daily_3M_rolling_calender_window - 1)
        daily_3M_datain = daily_beta_raw.loc[lb_date: ub_date, :]

        daily_3M_ret = daily_3M_datain.loc[:, daily_returns.columns]
        daily_3M_fs = daily_3M_datain.loc[:, daily_factor_list_for_beta_estimation]

        daily_3M_ret_count = daily_3M_ret.count()
        daily_3M_fs_count = daily_3M_fs.count()
        daily_3M_ret_mean = daily_3M_ret.fillna(0).sum() / daily_3M_ret_count
        daily_3M_fs_mean = daily_3M_fs.fillna(0).sum() / daily_3M_fs_count

        daily_3M_ret = daily_3M_ret - daily_3M_ret_mean
        daily_3M_fs = daily_3M_fs - daily_3M_fs_mean

        daily_3M_ret_mx = daily_3M_ret.fillna(0).loc[:, :].values
        daily_3M_fs_mx = daily_3M_fs.fillna(0).loc[:, :].values

        daily_3M_ret_indictor = daily_3M_ret.replace(0, 1).loc[:, :].values
        daily_3M_ret_indictor = daily_3M_ret_indictor / daily_3M_ret_indictor
        daily_3M_fs_indictor = daily_3M_fs.replace(0, 1).loc[:, :].values
        daily_3M_fs_indictor = daily_3M_fs_indictor / daily_3M_fs_indictor

        daily_3M_ret_indictor[np.isnan(daily_3M_ret_indictor)] = 0
        daily_3M_fs_indictor[np.isnan(daily_3M_fs_indictor)] = 0

        daily_3M_count = np.dot(daily_3M_ret_indictor.transpose(), daily_3M_fs_indictor)

        daily_3M_screen = np.dot(daily_3M_ret_indictor.transpose(), daily_3M_fs_indictor)
        daily_3M_screen[daily_3M_screen < daily_3M_min_p] = np.nan
        daily_3M_screen = daily_3M_screen / daily_3M_screen

        x = np.dot(daily_3M_ret_mx.transpose(), daily_3M_fs_mx) / (daily_3M_count - 1)
        y = np.dot(daily_3M_fs_mx.transpose(), daily_3M_fs_mx) / (
                    np.dot(daily_3M_fs_indictor.transpose(), daily_3M_fs_indictor) - 1)
        var = [y[i, i] for i in range(len(y))]
        var = np.array(var)

        daily_factor_list_for_beta_estimation_3M = [i + '_3M' for i in daily_factor_list_for_beta_estimation]
        daily_3M_betas_tmp = x / var * daily_3M_screen
        daily_3M_betas_tmp = pd.DataFrame(daily_3M_betas_tmp, columns=daily_factor_list_for_beta_estimation_3M,
                                          index=daily_returns.columns)

        ub_date = date
        lb_date = ub_date - datetime.timedelta(days=monthly_rolling_calender_window - 1)
        monthly_datain = monthly_beta_raw.loc[lb_date: ub_date, :]

        monthly_ret = monthly_datain.loc[:, monthly_returns.columns]
        monthly_fs = monthly_datain.loc[:, monthly_factor_list_for_beta_estimation]

        monthly_ret_count = monthly_ret.count()
        monthly_fs_count = monthly_fs.count()
        monthly_ret_mean = monthly_ret.fillna(0).sum() / monthly_ret_count
        monthly_fs_mean = monthly_fs.fillna(0).sum() / monthly_fs_count

        monthly_ret = monthly_ret - monthly_ret_mean
        monthly_fs = monthly_fs - monthly_fs_mean

        monthly_ret_mx = monthly_ret.fillna(0).loc[:, :].values
        monthly_fs_mx = monthly_fs.fillna(0).loc[:, :].values

        monthly_ret_indictor = monthly_ret.replace(0, 1).loc[:, :].values
        monthly_ret_indictor = monthly_ret_indictor / monthly_ret_indictor
        monthly_fs_indictor = monthly_fs.replace(0, 1).loc[:, :].values
        monthly_fs_indictor = monthly_fs_indictor / monthly_fs_indictor

        monthly_ret_indictor[np.isnan(monthly_ret_indictor)] = 0
        monthly_fs_indictor[np.isnan(monthly_fs_indictor)] = 0

        monthly_count = np.dot(monthly_ret_indictor.transpose(), monthly_fs_indictor)

        monthly_screen = np.dot(monthly_ret_indictor.transpose(), monthly_fs_indictor)
        monthly_screen[monthly_screen < monthly_min_p] = np.nan
        monthly_screen = monthly_screen / monthly_screen

        x = np.dot(monthly_ret_mx.transpose(), monthly_fs_mx) / (monthly_count - 1)
        y = np.dot(monthly_fs_mx.transpose(), monthly_fs_mx) / (
                    np.dot(monthly_fs_indictor.transpose(), monthly_fs_indictor) - 1)
        var = [y[i, i] for i in range(len(y))]
        var = np.array(var)

        monthly_betas_tmp = x / var * monthly_screen
        monthly_betas_tmp = pd.DataFrame(monthly_betas_tmp, columns=monthly_factor_list_for_beta_estimation,
                                         index=monthly_returns.columns)

        ub_date = datetime.datetime(date.year, date.month - (date.month - 1) % 3 + 2, 1) + relativedelta(months=-2,
                                                                                                         days=-1)
        lb_date = ub_date - datetime.timedelta(days=quarterly_rolling_calender_window - 1)
        quarterly_datain = quarterly_beta_raw.loc[lb_date: ub_date, :]
        quarterly_ret = quarterly_datain.loc[:, quarterly_returns.columns]
        quarterly_fs = quarterly_datain.loc[:, quarterly_factor_list_for_beta_estimation]

        quarterly_ret_count = quarterly_ret.count()
        quarterly_fs_count = quarterly_fs.count()
        quarterly_ret_mean = quarterly_ret.fillna(0).sum() / quarterly_ret_count
        quarterly_fs_mean = quarterly_fs.fillna(0).sum() / quarterly_fs_count

        quarterly_ret = quarterly_ret - quarterly_ret_mean
        quarterly_fs = quarterly_fs - quarterly_fs_mean

        quarterly_ret_mx = quarterly_ret.fillna(0).loc[:, :].values
        quarterly_fs_mx = quarterly_fs.fillna(0).loc[:, :].values

        quarterly_ret_indictor = quarterly_ret.replace(0, 1).loc[:, :].values
        quarterly_ret_indictor = quarterly_ret_indictor / quarterly_ret_indictor
        quarterly_fs_indictor = quarterly_fs.replace(0, 1).loc[:, :].values
        quarterly_fs_indictor = quarterly_fs_indictor / quarterly_fs_indictor

        quarterly_ret_indictor[np.isnan(quarterly_ret_indictor)] = 0
        quarterly_fs_indictor[np.isnan(quarterly_fs_indictor)] = 0

        quarterly_count = np.dot(quarterly_ret_indictor.transpose(), quarterly_fs_indictor)

        quarterly_screen = np.dot(quarterly_ret_indictor.transpose(), quarterly_fs_indictor)
        quarterly_screen[quarterly_screen < quarterly_min_p] = np.nan
        quarterly_screen = quarterly_screen / quarterly_screen

        x = np.dot(quarterly_ret_mx.transpose(), quarterly_fs_mx) / (quarterly_count - 1)
        y = np.dot(quarterly_fs_mx.transpose(), quarterly_fs_mx) / (
                    np.dot(quarterly_fs_indictor.transpose(), quarterly_fs_indictor) - 1)
        var = [y[i, i] for i in range(len(y))]
        var = np.array(var)

        quarterly_betas_tmp = x / var * quarterly_screen
        quarterly_betas_tmp = pd.DataFrame(quarterly_betas_tmp, columns=quarterly_factor_list_for_beta_estimation,
                                           index=quarterly_returns.columns)

        betas_tmp = pd.concat([daily_betas_tmp, daily_3M_betas_tmp, monthly_betas_tmp, quarterly_betas_tmp], axis=1)
        betas_tmp = betas_tmp.dropna(how='all', axis=0)
        betas_tmp.to_csv(os.path.join(output_path, date.strftime("%Y-%m-%d") + ".csv"))

    print('Chunk ' + str(chunk) + ' done!')


def construct_betas(end_date):
    summary_path = addpath.bundle_path
    tsfactorpath = os.path.join(addpath.historical_path, 'beta_factor', 'processed')
    chunk_output_path = os.path.join(addpath.historical_path, 'beta_factor', 'beta_temp')
    savepath = os.path.join(addpath.historical_path, 'beta_factor')
    navpath = os.path.join(addpath.bundle_path, "daily")
    daily_factor_list_for_beta_estimation = [fac for fac in daily_factor_level] + [fac + '_change' for fac in daily_factor_change] + [fac + '_growth' for fac in daily_factor_growth]
    monthly_factor_list_for_beta_estimation = [fac for fac in monthly_factor_level] + [fac + '_change' for fac in monthly_factor_change] + [fac + '_growth' for fac in monthly_factor_growth]
    quarterly_factor_list_for_beta_estimation = [fac for fac in quarterly_factor_level] + [fac + '_change' for fac in quarterly_factor_change] + [fac + '_growth' for fac in quarterly_factor_growth]

    if os.path.exists(savepath):
        pass
    else:
        makedirs(savepath)

    ticker_path = os.path.join(summary_path, "Summary.csv")
    ms_secids = pd.read_csv(ticker_path)
    ms_secids = ms_secids['Name'].tolist()

    daily_factor_path = os.path.join(tsfactorpath, "daily.csv")
    daily_factor_raw = pd.read_csv(daily_factor_path, parse_dates=[0], index_col=0)
    daily_factor_raw = daily_factor_raw.loc[:, daily_factor_list_for_beta_estimation]

    monthly_factor_path = os.path.join(tsfactorpath, "monthly.csv")
    monthly_factor_raw = pd.read_csv(monthly_factor_path, parse_dates=[0], index_col=0)
    monthly_factor_list_for_beta_estimation = [col for col in monthly_factor_list_for_beta_estimation if
                                              col in monthly_factor_raw.columns]
    monthly_factor_raw = monthly_factor_raw.loc[:, monthly_factor_list_for_beta_estimation]
    monthly_factor_raw = monthly_factor_raw.resample('1M').last().ffill()
    monthly_factor_raw = monthly_factor_raw.astype(float)

    quarterly_factor_path = os.path.join(tsfactorpath, "quarterly.csv")
    quarterly_factor_raw = pd.read_csv(quarterly_factor_path, parse_dates=[0], index_col=0)
    quarterly_factor_list_for_beta_estimation = [col for col in quarterly_factor_list_for_beta_estimation if
                                              col in quarterly_factor_raw.columns]
    quarterly_factor_raw = quarterly_factor_raw.loc[:, quarterly_factor_list_for_beta_estimation]
    quarterly_factor_raw = quarterly_factor_raw.resample('Q').last().ffill()
    quarterly_factor_raw = quarterly_factor_raw.astype(float)

    trading_calender = pd.read_csv(os.path.join(summary_path, "Trading_Calendar.csv"), parse_dates=[0], index_col=0)
    trading_calender = trading_calender.index.tolist()

    num_funds = len(ms_secids)
    chunk_size = 800
    chunk_number = int(num_funds / chunk_size) + 1

    pool = multiprocessing.Pool()

    for chunk in range(chunk_number):
        pool.apply_async(betas_helper,
                   args=(chunk, chunk_number, chunk_output_path, num_funds, chunk_size, ms_secids, navpath,
                         trading_calender, daily_factor_raw, monthly_factor_raw, quarterly_factor_raw,
                         daily_factor_list_for_beta_estimation,
                         monthly_factor_list_for_beta_estimation, quarterly_factor_list_for_beta_estimation,))
    pool.close()
    pool.join()
    print('Sub-processes done!')

    dates = pd.date_range('2013-12-31', end_date, freq='M').tolist()
    for date in dates:
        betas_list = []
        for chunk in range(chunk_number):
            try:
                read_path = os.path.join(chunk_output_path, "chunk_" + str(chunk))
                betas_tmp = pd.read_csv(os.path.join(read_path, date.strftime("%Y-%m-%d") + ".csv"), index_col=0)
                betas_list.append(betas_tmp)
            except:
                print("No data for Chunk " + str(chunk) + " on date " + date.strftime("%Y-%m-%d"))
        betas = pd.concat(betas_list)
        betas = betas.dropna(axis=0, how='all')
        betas.to_csv(os.path.join(savepath, date.strftime("%Y-%m-%d") + ".csv"))


if __name__ == "__main__":
    # prepare_ts_factor()
    construct_betas(end_date='2020-12-31')