import numpy as np
import pandas as pd
from os import makedirs, listdir
import os
import datetime
import multiprocessing
from constant import *
from algorithm import addpath
np.seterr(divide='ignore', invalid='ignore')

def rsj(datain):
    demean_data = datain - datain.mean()
    pos_data = pd.DataFrame(np.where(demean_data > 0, demean_data, np.nan))
    pos_count = pos_data.count()
    pos_var = (pos_data ** 2).sum() / (pos_count - 1)
    neg_data = pd.DataFrame(np.where(demean_data < 0, demean_data, np.nan))
    neg_count = neg_data.count()
    neg_var = (neg_data ** 2).sum() / (neg_count - 1)
    rsj = pos_var - neg_var
    rsj.index = datain.columns
    return rsj


def nav_factor_calculator(hrz, nav, nav_monthly):
    print('Start: horizon {}'.format(hrz))
    start = datetime.datetime.now()
    if hrz != 'YTD':
        return_factor_tmp = nav_monthly / nav_monthly.shift(hrz) - 1
        date_index = return_factor_tmp.index.tolist()
        start_date = date_index[hrz]
        return_factor_tmp = return_factor_tmp.dropna(how='all')
        return_factor_tmp_ann = (1 + return_factor_tmp) ** (12 / hrz) - 1
    else:
        date_index = nav_monthly.index.tolist()
        start_date = date_index[12]
        return_factor_tmp_list = []
        return_factor_tmp_ann_list = []
    end_date = date_index[-1]
    dates = pd.date_range(start_date, end_date, freq='M').tolist()
    # volatility_factor_tmp_hrz_list = []
    volatility_factor_tmp_list = []
    maximumdrawdown_factor_tmp_list = []
    maximum_daily_drop_factor_tmp_list = []
    skew_factor_tmp_list = []
    rsj_factor_tmp_list = []
    momentum_return_factor_tmp_list = []
    momentum_return_factor_tmp_ann_list = []
    momentum_volatility_factor_tmp_list = []
    reversal_return_factor_tmp_list = []
    reversal_return_factor_tmp_ann_list = []
    reversal_volatility_factor_tmp_list = []
    for date in dates:
        # print('date {}'.format(date))
        ub_date = date
        if hrz != 'YTD':
            lb_date = date_index[date_index.index(date) - hrz]
        else:
            lb_date = datetime.datetime(date.year - 1, 12, 31)
            return_factor_tmp = pd.DataFrame(
                nav_monthly.loc[ub_date, :] / nav_monthly.loc[lb_date, :] - 1).transpose()
            return_factor_tmp.index = [date]
            return_factor_tmp_list.append(return_factor_tmp)
            return_factor_tmp_ann = (1 + return_factor_tmp) ** (365 / (date - lb_date).days) - 1
            return_factor_tmp_ann.index = [date]
            return_factor_tmp_ann_list.append(return_factor_tmp_ann)

        if hrz in [6, 9, 12, 18]:
            lb_date_momentum = date_index[date_index.index(date) - hrz]
            ub_date_momentum = date_index[date_index.index(date) - 3]
            momentum_return_factor_tmp = pd.DataFrame(
                nav_monthly.loc[ub_date_momentum, :] / nav_monthly.loc[lb_date_momentum, :] - 1).transpose()
            momentum_return_factor_tmp_ann = (1 + momentum_return_factor_tmp) ** (
                        365 / (ub_date_momentum - lb_date_momentum).days) - 1
            momentum_nav_daily_tmp = nav.loc[lb_date_momentum: ub_date_momentum, :]
            momentum_return_daily_tmp = momentum_nav_daily_tmp / momentum_nav_daily_tmp.shift() - 1
            momentum_vol_tmp = pd.DataFrame(momentum_return_daily_tmp.std() * (245 ** 0.5)).transpose()
        else:
            momentum_return_factor_tmp = pd.DataFrame(index=[date], columns=nav_monthly.columns)
            momentum_return_factor_tmp_ann = pd.DataFrame(index=[date], columns=nav_monthly.columns)
            momentum_vol_tmp = pd.DataFrame(index=[date], columns=nav_monthly.columns)
        momentum_return_factor_tmp.index = [date]
        momentum_return_factor_tmp_ann.index = [date]
        momentum_vol_tmp.index = [date]
        momentum_return_factor_tmp_list.append(momentum_return_factor_tmp)
        momentum_return_factor_tmp_ann_list.append(momentum_return_factor_tmp_ann)
        momentum_volatility_factor_tmp_list.append(momentum_vol_tmp)


        if hrz != 'YTD' and hrz > 18:
            lb_date_reversal = date_index[date_index.index(date) - hrz]
            ub_date_reversal = date_index[date_index.index(date) - 18]
            reversal_return_factor_tmp = pd.DataFrame(
                nav_monthly.loc[ub_date_reversal, :] / nav_monthly.loc[lb_date_reversal, :] - 1).transpose()
            reversal_return_factor_tmp_ann = (1 + reversal_return_factor_tmp) ** (
                    365 / (ub_date_reversal - lb_date_reversal).days) - 1
            reversal_nav_daily_tmp = nav.loc[lb_date_reversal: ub_date_reversal, :]
            reversal_return_daily_tmp = reversal_nav_daily_tmp / reversal_nav_daily_tmp.shift() - 1
            reversal_vol_tmp = pd.DataFrame(reversal_return_daily_tmp.std() * (245 ** 0.5)).transpose()
        else:
            reversal_return_factor_tmp = pd.DataFrame(index=[date], columns=nav_monthly.columns)
            reversal_return_factor_tmp_ann = pd.DataFrame(index=[date], columns=nav_monthly.columns)
            reversal_vol_tmp = pd.DataFrame(index=[date], columns=nav_monthly.columns)
        reversal_return_factor_tmp.index = [date]
        reversal_return_factor_tmp_ann.index = [date]
        reversal_vol_tmp.index = [date]
        reversal_return_factor_tmp_list.append(reversal_return_factor_tmp)
        reversal_return_factor_tmp_ann_list.append(reversal_return_factor_tmp_ann)
        reversal_volatility_factor_tmp_list.append(reversal_vol_tmp)

        nav_daily_tmp = nav.loc[lb_date: ub_date, :]
        return_daily_tmp = nav_daily_tmp / nav_daily_tmp.shift() - 1
        # return_daily_tmp_count = return_daily_tmp.count()
        # vol_tmp_hrz = pd.DataFrame(return_daily_tmp.std() * (return_daily_tmp_count ** 0.5)).transpose()
        # vol_tmp_hrz.index = [date]
        # volatility_factor_tmp_hrz_list.append(vol_tmp_hrz)
        vol_tmp = pd.DataFrame(return_daily_tmp.std() * (245 ** 0.5)).transpose()
        vol_tmp.index = [date]
        volatility_factor_tmp_list.append(vol_tmp)
        mdailydrop_tmp = pd.DataFrame(return_daily_tmp.min())
        mdailydrop_tmp.iloc[:, 0] = mdailydrop_tmp.iloc[:, 0].map(lambda x: 0 if x > 0 else x)
        mdailydrop_tmp = mdailydrop_tmp.transpose()
        mdailydrop_tmp.index = [date]
        mdailydrop_tmp = - mdailydrop_tmp
        maximum_daily_drop_factor_tmp_list.append(mdailydrop_tmp)
        skew_tmp = pd.DataFrame(return_daily_tmp.skew()).transpose()
        skew_tmp.index = [date]
        skew_factor_tmp_list.append(skew_tmp)
        drawdown_tmp = nav_daily_tmp / nav_daily_tmp.cummax() - 1
        mdd_tmp = pd.DataFrame(drawdown_tmp.min()).transpose()
        mdd_tmp.index = [date]
        mdd_tmp = - mdd_tmp
        maximumdrawdown_factor_tmp_list.append(mdd_tmp)
        rsj_tmp = pd.DataFrame(rsj(return_daily_tmp)).transpose()
        rsj_tmp.index = [date]
        rsj_factor_tmp_list.append(rsj_tmp)
    if hrz == 'YTD':
        return_factor_tmp = pd.concat(return_factor_tmp_list)
        return_factor_tmp.replace([np.inf, -np.inf], np.nan, inplace=True)
        return_factor_tmp_ann = pd.concat(return_factor_tmp_ann_list)
        return_factor_tmp_ann.replace([np.inf, -np.inf], np.nan, inplace=True)

    volatility_factor_tmp = pd.concat(volatility_factor_tmp_list)
    volatility_factor_tmp[volatility_factor_tmp < 0.0000001] = np.nan
    volatility_factor_tmp.replace([np.inf, -np.inf], np.nan, inplace=True)

    # volatility_factor_tmp_hrz = pd.concat(volatility_factor_tmp_hrz_list)
    # volatility_factor_tmp_hrz[volatility_factor_tmp_hrz < 0.0000001] = np.nan
    # volatility_factor_tmp_hrz.replace([np.inf, -np.inf], np.nan, inplace=True)

    maximum_daily_drop_factor_tmp = pd.concat(maximum_daily_drop_factor_tmp_list)
    maximum_daily_drop_factor_tmp[maximum_daily_drop_factor_tmp < 0.0000001] = np.nan
    maximum_daily_drop_factor_tmp.replace([np.inf, -np.inf], np.nan, inplace=True)

    skew_factor_tmp = pd.concat(skew_factor_tmp_list)
    skew_factor_tmp.replace([np.inf, -np.inf], np.nan, inplace=True)

    rsj_factor_tmp = pd.concat(rsj_factor_tmp_list)
    rsj_factor_tmp.replace([np.inf, -np.inf], np.nan, inplace=True)

    maximumdrawdown_factor_tmp = pd.concat(maximumdrawdown_factor_tmp_list)
    maximumdrawdown_factor_tmp.replace([np.inf, -np.inf], np.nan, inplace=True)

    sharperatio_factor_tmp = return_factor_tmp_ann / volatility_factor_tmp

    momentum_return_factor_tmp = pd.concat(momentum_return_factor_tmp_list)
    momentum_return_factor_tmp.replace([np.inf, -np.inf], np.nan, inplace=True)
    momentum_return_factor_tmp_ann = pd.concat(momentum_return_factor_tmp_ann_list)
    momentum_return_factor_tmp_ann.replace([np.inf, -np.inf], np.nan, inplace=True)
    momentum_volatility_factor_tmp = pd.concat(momentum_volatility_factor_tmp_list)
    momentum_volatility_factor_tmp[momentum_volatility_factor_tmp < 0.0000001] = np.nan
    momentum_volatility_factor_tmp.replace([np.inf, -np.inf], np.nan, inplace=True)
    try:
        momentum_sharperatio_factor_tmp = momentum_return_factor_tmp_ann / momentum_volatility_factor_tmp
    except:
        momentum_sharperatio_factor_tmp = momentum_volatility_factor_tmp

    reversal_return_factor_tmp = pd.concat(reversal_return_factor_tmp_list)
    reversal_return_factor_tmp.replace([np.inf, -np.inf], np.nan, inplace=True)
    reversal_return_factor_tmp_ann = pd.concat(reversal_return_factor_tmp_ann_list)
    reversal_return_factor_tmp_ann.replace([np.inf, -np.inf], np.nan, inplace=True)
    reversal_volatility_factor_tmp = pd.concat(reversal_volatility_factor_tmp_list)
    reversal_volatility_factor_tmp[reversal_volatility_factor_tmp < 0.0000001] = np.nan
    reversal_volatility_factor_tmp.replace([np.inf, -np.inf], np.nan, inplace=True)
    try:
        reversal_sharperatio_factor_tmp = reversal_return_factor_tmp_ann / reversal_volatility_factor_tmp
    except:
        reversal_sharperatio_factor_tmp = reversal_volatility_factor_tmp

    end = datetime.datetime.now()
    print('End：horizon {} in {}'.format(hrz, str(end - start)))
    return return_factor_tmp, volatility_factor_tmp, sharperatio_factor_tmp, maximumdrawdown_factor_tmp, \
           maximum_daily_drop_factor_tmp, skew_factor_tmp, rsj_factor_tmp, momentum_return_factor_tmp, \
           momentum_sharperatio_factor_tmp, reversal_return_factor_tmp, reversal_sharperatio_factor_tmp


def nav_factor_calulation(start_date, end_date):
    summary_path = addpath.bundle_path
    # category_data_path = os.path.join(addpath.historical_path, 'categorization')
    savepath = os.path.join(addpath.historical_path, 'nav_factor')
    if not os.path.exists(savepath):
        os.makedirs(savepath)
    navpath = os.path.join(addpath.bundle_path, "daily")

    trading_calender = pd.read_csv(os.path.join(summary_path, "Trading_Calendar.csv"), parse_dates=[0], index_col=0)
    trading_calender = trading_calender.index.tolist()

    # config_file_path = join(addpath.config_path, "config.conf")
    # config = configparser.ConfigParser()
    # config.read(config_file_path)
    # data_start_date = str(config['factor_info']['nav_factor_start_date'])
    # data_end_date = str(config['update_info']['data_end_date'])

    # ticker_path = join(addpath.reference_data_path, "cn_fund_full.csv")
    ms_secids = pd.read_csv(addpath.ref_path)
    ms_secids = ms_secids['ms_secid'].tolist()
    nav_list = []
    count = 0
    start = datetime.datetime.now()
    for ms_secid in ms_secids:
        nav_tmp_path = os.path.join(navpath, str(ms_secid) + ".csv")
        try:
            nav_tmp = pd.read_csv(nav_tmp_path, parse_dates=[0], index_col=0)
        except:
            count += 1
            # print("%i: %s does not exist " % (count, ms_secid))
            continue
        nav_tmp = nav_tmp[['close']].rename(columns={'close': ms_secid})
        nav_tmp = nav_tmp[nav_tmp.index >= datetime.datetime.strptime(start_date, "%Y-%m-%d") - datetime.timedelta(days=1800)]
        nav_tmp = nav_tmp[nav_tmp.index <= datetime.datetime.strptime(end_date, "%Y-%m-%d")]
        nav_tmp = nav_tmp.ffill()
        nav_list.append(nav_tmp)
    end = datetime.datetime.now()
    print('Read bundles finished in {}'.format(end-start))
    nav = pd.concat(nav_list, axis=1)
    trading_days = [d for d in nav.index if d in trading_calender]
    nav = nav.loc[trading_days, :]
    nav = nav.dropna(how='all')
    nav = nav.sort_index()
    del nav_list

    nav_monthly = nav.resample('1M').last()

    return_factor = {}
    volatility_factor = {}
    sharperatio_factor = {}
    maximumdrawdown_factor = {}
    maximum_daily_drop_factor = {}
    skew_factor = {}
    rsj_factor = {}
    momentum_return_factor = {}
    momentum_sharperatio_factor = {}
    reversal_return_factor = {}
    reversal_sharperatio_factor = {}

    horizon = [1, 3, 6, 9, 12, 18, 24, 30, 36, 48, 60, 'YTD']
    horizon1 = horizon[:6]
    pool = multiprocessing.Pool(len(horizon1))
    res_l = []
    for hrz in horizon1:
        res = pool.apply_async(nav_factor_calculator, args=(hrz, nav, nav_monthly,))
        res_l.append(res)
    pool.close()
    pool.join()

    horizon2 = horizon[6:]
    pool = multiprocessing.Pool(len(horizon2))
    res_l2 = []
    for hrz in horizon2:
        res = pool.apply_async(nav_factor_calculator, args=(hrz, nav, nav_monthly,))
        res_l2.append(res)
    pool.close()
    pool.join()

    res_l += res_l2

    for (i, hrz) in enumerate(horizon):
        return_factor[hrz] = res_l[i].get()[0]
        volatility_factor[hrz] = res_l[i].get()[1]
        sharperatio_factor[hrz] = res_l[i].get()[2]
        maximumdrawdown_factor[hrz] = res_l[i].get()[3]
        maximum_daily_drop_factor[hrz] = res_l[i].get()[4]
        skew_factor[hrz] = res_l[i].get()[5]
        rsj_factor[hrz] = res_l[i].get()[6]
        momentum_return_factor[hrz] = res_l[i].get()[7]
        momentum_sharperatio_factor[hrz] = res_l[i].get()[8]
        reversal_return_factor[hrz] = res_l[i].get()[9]
        reversal_sharperatio_factor[hrz] = res_l[i].get()[10]


    rfs = {
        'return_factor': return_factor,
        'volatility_factor': volatility_factor,
        'sharperatio_factor': sharperatio_factor,
        'maximum_drawdown_factor': maximumdrawdown_factor,
        'maximum_daily_drop_factor': maximum_daily_drop_factor,
        'skew_factor': skew_factor,
        'rsj_factor': rsj_factor,
        'momentum_return_factor': momentum_return_factor,
        'momentum_sharperatio_factor': momentum_sharperatio_factor,
        'longterm_reversal_return_factor': reversal_return_factor,
        'longterm_reversal_sharperatio_factor': reversal_sharperatio_factor
    }

    dates = pd.date_range(start_date, end_date, freq='M').tolist()
    for rf in rfs:
        output_path = os.path.join(savepath, rf)

        if os.path.exists(output_path):
            pass
        else:
            makedirs(output_path)
        # benchmark_list = []
        for date in dates:
            # category_path = os.path.join(category_data_path, '{}.csv'.format(date))
            # category = pd.read_csv(category_path, index_col=0)
            # date = datetime.datetime.strptime(date, '%Y-%m-%d')
            rst_list = []
            # benchmark_tmp_list = []
            for hrz in horizon:
                try:
                    tmp = pd.DataFrame(rfs[rf][hrz].loc[str(date.date()),:])
                    tmp.columns = ['variable']
                    tmp = tmp.dropna()

                    # category = category.dropna()
                    #
                    # tmp = pd.merge(tmp, category, how='left', left_index=True, right_index=True)
                    # tmp['category'] = tmp['category'].fillna('Equity')
                    # tmp['category_rank'] = tmp.groupby('category').variable.rank(pct=True)
                    # tmp = tmp.reset_index()
                    # category_variable = tmp.groupby('category').variable.median()
                    # category_variable = category_variable.reset_index()
                    # category_variable = category_variable.rename(columns={'variable': 'category_variable'})
                    # tmp = pd.merge(tmp, category_variable, how='left', left_index=True, left_on='category', right_on='category')
                    # tmp['category_adjusted_variable'] = tmp['variable'] - tmp['category_variable']
                    # tmp = tmp.set_index('index')
                    if hrz != 'YTD':
                        tmp = tmp.rename(columns={'variable': rf + '_' + str(hrz) + 'M'})
                                                  # 'category_rank': 'category_rank_pct_' + rf + '_' + str(hrz) + 'M',
                                                  # 'category_adjusted_variable': 'category_adjusted_' + rf + '_' + str(hrz) + 'M'})
                        rst_list.append(tmp.loc[:, [rf + '_' + str(hrz) + 'M']])
                                                # 'category_rank_pct_' + rf + '_' + str(hrz) + 'M',
                                                # 'category_adjusted_' + rf + '_' + str(hrz) + 'M']])
                        # category_variable['date'] = date
                        # category_variable = category_variable.rename(columns={'category_variable': rf + '_' + str(hrz) + 'M'})
                    else:
                        tmp = tmp.rename(columns={'variable': rf + '_' + hrz})
                                                  # 'category_rank': 'category_rank_pct_' + rf + '_' + hrz,
                                                  # 'category_adjusted_variable': 'category_adjusted_' + rf + '_' + hrz})
                        rst_list.append(tmp.loc[:, [rf + '_' + hrz]])
                                                # 'category_rank_pct_' + rf + '_' + hrz,
                                                # 'category_adjusted_' + rf + '_' + hrz]])
                        # category_variable['date'] = date
                        # category_variable = category_variable.rename(columns={'category_variable': rf + '_' + hrz})
                    # category_variable = category_variable.set_index(['category', 'date'])
                    # benchmark_tmp_list.append(category_variable)
                except:
                    tmp = pd.DataFrame(index=ms_secids)
                    if hrz != 'YTD':
                        tmp[rf + '_' + str(hrz) + 'M'] = np.nan
                        # tmp['category_rank_pct_' + rf + '_' + str(hrz) + 'M'] = np.nan
                        # tmp['category_adjusted_' + rf + '_' + str(hrz) + 'M'] = np.nan
                    else:
                        tmp[rf + '_' + hrz] = np.nan
                        # tmp['category_rank_pct_' + rf + '_' + hrz] = np.nan
                        # tmp['category_adjusted_' + rf + '_' + hrz] = np.nan
                    rst_list.append(tmp)
            # try:
            #     benchmark_tmp = pd.concat(benchmark_tmp_list, axis=1)
            #     benchmark_list.append(benchmark_tmp)
            # except:
            #     pass

            rst = pd.concat(rst_list, axis=1)
            rst = rst.dropna(how='all', axis=0)
            file_path = os.path.join(output_path, date.strftime("%Y-%m-%d") + '.csv')
            rst.to_csv(file_path)
        # try:
        #     benchmark = pd.concat(benchmark_list)
        #     if os.path.exists(addpath.benchmark_path):
        #         pass
        #     else:
        #         makedirs(addpath.benchmark_path)
        #     benchmark_path = os.path.join(addpath.benchmark_path, rf + '.csv')
        #     benchmark.to_csv(benchmark_path)
        # except:
        #     continue


if __name__ == "__main__":
    nav_factor_calulation(start_date='2014-12-31', end_date='2020-12-31')
