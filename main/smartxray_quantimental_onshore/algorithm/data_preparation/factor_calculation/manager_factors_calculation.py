import numpy as np
import pandas as pd
import configparser
import os
import datetime
import calendar
import multiprocessing
from constant import *
from algorithm import addpath


def pm_data_helper(pm_id_list, fund_manager_map, savepath, return_list, nav_path, categories, end_date):
    trading_calender = pd.read_csv(os.path.join(addpath.bundle_path, "Trading_Calendar.csv"), parse_dates=[0],
                                   index_col=0)
    trading_calender = trading_calender.index.tolist()

    for id in pm_id_list:
        print(id)
        temp_map = fund_manager_map[fund_manager_map['fund_manager_id'] == id]
        temp_map = temp_map.sort_values(by='fund_manager_joinstartdate')
        temp_map_list = []
        for secid in list(set(temp_map['ms_secid'].tolist())):
            tmp_map = temp_map[temp_map['ms_secid'] == secid]
            if tmp_map.shape[0] == 1:
                temp_map_list.append(tmp_map)
            else:
                # make sure for each fund under a single manager, there is either one record or multiple record with counters
                tmp_map = tmp_map.sort_values(by='fund_manager_joinstartdate')
                dt_range = []
                for idx in tmp_map.index:
                    dt_range = dt_range + pd.date_range(tmp_map.loc[idx, 'fund_manager_joinstartdate'],
                                                        tmp_map.loc[idx, 'fund_manager_joinenddate'], freq='D').tolist()
                dt_range = list(set(dt_range))
                if len(dt_range) > 0:
                    dt_range.sort()
                    counter = 0
                    career_start_tmp = tmp_map.loc[tmp_map.index[0], 'fund_manager_careerstartyear']
                    tmp_map = pd.DataFrame()
                    start_tmp = dt_range[0]
                    for dt_id in range(len(dt_range)):
                        if dt_id < len(dt_range) - 1:
                            if dt_range[dt_id] == dt_range[dt_id + 1] - datetime.timedelta(days=1):
                                pass
                            else:
                                tmp_map.loc[counter, 'fund_manager_id'] = id
                                tmp_map.loc[counter, 'fund_manager_joinstartdate'] = start_tmp
                                tmp_map.loc[counter, 'fund_manager_joinenddate'] = dt_range[dt_id]
                                tmp_map.loc[counter, 'fund_manager_careerstartyear'] = career_start_tmp
                                tmp_map.loc[counter, 'ms_secid'] = secid
                                start_tmp = dt_range[dt_id + 1]
                                counter += 1
                        else:
                            tmp_map.loc[counter, 'fund_manager_id'] = id
                            tmp_map.loc[counter, 'fund_manager_joinstartdate'] = start_tmp
                            tmp_map.loc[counter, 'fund_manager_joinenddate'] = dt_range[dt_id]
                            tmp_map.loc[counter, 'fund_manager_careerstartyear'] = career_start_tmp
                            tmp_map.loc[counter, 'ms_secid'] = secid
                    temp_map_list.append(tmp_map)
        # prompt if no fund under this manager
        if len(temp_map_list) == 0:
            print("Fail for " + str(id))
            continue

        temp_map = pd.concat(temp_map_list)
        temp_map = temp_map.reset_index()
        temp_map = temp_map.drop(columns='index')

        fund_number = temp_map.groupby('fund_manager_joinstartdate').fund_manager_id.count()
        fund_number = fund_number.reset_index()
        fund_number['fund_number'] = fund_number['fund_manager_id'].cumsum()
        fund_number = fund_number.drop(columns='fund_manager_id')
        fund_number['index'] = fund_number['fund_manager_joinstartdate'].map(
            lambda x: datetime.datetime(x.year, x.month, calendar.monthrange(x.year, x.month)[1]))
        fund_number = fund_number.set_index('index')
        fund_number = fund_number.drop(columns='fund_manager_joinstartdate')
        fund_number = fund_number.groupby(fund_number.index).max()

        individual_list = []
        for idx in temp_map.index:
            period = pd.date_range(temp_map.loc[idx, 'fund_manager_joinstartdate'],
                                   temp_map.loc[idx, 'fund_manager_joinenddate'] + datetime.timedelta(days=30),
                                   freq='M')
            individual_tmp = pd.DataFrame(index=period)
            individual_tmp['fund_manager_id'] = id
            individual_tmp['ms_secid'] = temp_map.loc[idx, 'ms_secid']
            individual_tmp['current_fund_startdate'] = temp_map.loc[idx, 'fund_manager_joinstartdate']
            individual_list.append(individual_tmp)
        individual = pd.concat(individual_list)
        individual = individual.sort_index()
        individual = individual[individual.index <= end_date]

        individual['fund_number'] = fund_number['fund_number']
        individual['fund_number'] = individual['fund_number'].ffill()
        # check whether the careerstartyear documented, if not, select the earliest time
        career_start = temp_map['fund_manager_careerstartyear'].dropna()
        if len(career_start) == 0:
            career_start = individual.index.min()
        else:
            career_start = datetime.datetime(int(career_start.iloc[0]), 1, 1)
        individual['career_start_year'] = career_start
        individual['career_experience'] = individual.index - individual['career_start_year']
        individual['career_experience'] = individual['career_experience'].map(lambda x: x.days)

        pm_months = individual.index.drop_duplicates()
        pm_months = list(pm_months)
        gap = np.zeros(len(pm_months))
        for i in range(len(pm_months) - 1):
            mid_1 = pm_months[i + 1].year * 12 + pm_months[i + 1].month
            mid_0 = pm_months[i].year * 12 + pm_months[i].month
            if mid_1 - mid_0 > 1:
                gap[i + 1] = gap[i] + (pm_months[i + 1] - pm_months[i]).days
            else:
                gap[i + 1] = gap[i]
        individual = individual.reset_index()

        total_tenure = individual.groupby('index').fund_manager_id.count()
        total_tenure = total_tenure.reset_index()
        total_tenure['tenure'] = total_tenure['fund_manager_id'] * 30
        total_tenure['cum_tenure'] = total_tenure['tenure'].cumsum()
        total_tenure['cum_tenure'] = total_tenure['cum_tenure'].fillna(0)
        total_tenure = total_tenure.drop(columns=['fund_manager_id', 'tenure'])
        individual = pd.merge(individual, total_tenure, how='left', left_on='index',
                              right_on='index')
        individual['average_tenure'] = individual['cum_tenure'] / individual['fund_number']

        for idx in individual.index:
            individual.loc[idx, 'pm_experience'] = (individual.loc[idx, 'index'] - individual['index'].min()).days - \
                                                   gap[pm_months.index(individual.loc[idx, 'index'])]
            try: # try to add the aum and fund category of each date
                individual.loc[idx, 'aum'] = return_list[individual.loc[idx, 'index']].loc[
                    individual.loc[idx, 'ms_secid'], 'aum']
                individual.loc[idx, 'eq_bond'] = return_list[individual.loc[idx, 'index']].loc[
                    individual.loc[idx, 'ms_secid'], 'category']
            except:
                individual.loc[idx, 'aum'] = np.nan
                individual.loc[idx, 'eq_bond'] = np.nan

        indi_agg_list = []
        indi_agg_header = individual.loc[:,
                          ['fund_manager_id', 'index', 'career_start_year', 'career_experience', 'pm_experience',
                           'average_tenure', 'fund_number']].drop_duplicates()
        indi_agg_header = indi_agg_header.set_index('index')
        indi_agg_list.append(indi_agg_header)

        indi_agg_current_fund_tenure = pd.DataFrame(index=list(set(indi_agg_header.index.tolist())), columns=list(set(temp_map['ms_secid'].tolist())))
        indi_agg_current_fund_tenure = indi_agg_current_fund_tenure.sort_index()
        for idx in indi_agg_current_fund_tenure.index:
            for col in indi_agg_current_fund_tenure.columns:
                tmp = temp_map[temp_map['ms_secid'] == col]
                for idxx in tmp.index:
                    if tmp.loc[idxx, 'fund_manager_joinstartdate'] <= idx <= tmp.loc[idxx, 'fund_manager_joinenddate']:
                        indi_agg_current_fund_tenure.loc[idx, col] = (idx - tmp.loc[idxx, 'fund_manager_joinstartdate']).days

        for col in indi_agg_current_fund_tenure.columns:
            indi_agg_current_fund_tenure = indi_agg_current_fund_tenure.rename(
                columns={col: 'current_fund_tenure_' + col})
        indi_agg_current_fund_tenure = indi_agg_current_fund_tenure.sort_index()
        indi_agg_list.append(indi_agg_current_fund_tenure)

        check = individual[['eq_bond']].dropna()
        # calculate aum and past_mean_aum by category
        for eq_bond in ['Equity', 'Bond', 'Money', 'Alternative']:
            individual_agg = individual[['index']].drop_duplicates()
            individual_agg = individual_agg.set_index('index')

            if check.shape[0] >= 1:
                individual_tmp = individual[individual['eq_bond'] == eq_bond]
                individual_agg[eq_bond + '_aum'] = individual_tmp.groupby('index').aum.mean()
                individual_agg = individual_agg.dropna(how='all')
                individual_agg[eq_bond + '_past_mean_aum'] = individual_agg[eq_bond + '_aum'].cumsum() / range(1, len(
                    individual_agg) + 1)
                indi_agg_list.append(individual_agg)

        individual_performance = pd.concat(indi_agg_list, axis=1)
        individual_performance = individual_performance.dropna(axis=1, how='all')

        individual_return_list = []
        for idx in temp_map.index:
            period = pd.date_range(temp_map.loc[idx, 'fund_manager_joinstartdate'],
                                   temp_map.loc[idx, 'fund_manager_joinenddate'] + datetime.timedelta(days=30),
                                   freq='D')
            individual_tmp = pd.DataFrame(index=period)
            try:
                nav_tmp = pd.read_csv(os.path.join(nav_path, temp_map.loc[idx, 'ms_secid'] + '.csv'), parse_dates=[0],
                                      index_col=0)
                trading_days = [d for d in nav_tmp.index if d in trading_calender]
                nav_tmp = nav_tmp.loc[trading_days, :]
                nav_tmp['close'] = nav_tmp['close'].ffill()
                nav_tmp['return'] = nav_tmp['close'].pct_change()
                individual_tmp[temp_map.loc[idx, 'ms_secid']] = nav_tmp['return']
            except:
                individual_tmp[temp_map.loc[idx, 'ms_secid']] = np.nan
            individual_return_list.append(individual_tmp)
        individual_return = pd.concat(individual_return_list, axis=1)
        individual_return = individual_return.dropna(how='all')
        individual_return = individual_return.dropna(how='all', axis=1)
        individual_return = individual_return.fillna(0)

        ctg = pd.DataFrame(index=individual_return.index, columns=individual_return.columns)

        ctg_index = [index for index in categories.index if index in individual_return.index]
        ctg_columns = [col for col in categories.columns if col in individual_return.columns]
        ctg_tmp = categories.loc[ctg_index, ctg_columns]
        ctg_tmp = ctg_tmp.sort_index()

        for col in ctg:
            try:
                ctg[col] = ctg_tmp[col]
            except:
                pass

        individual_return_categorized = pd.DataFrame(index=individual_return.index,
                                                     columns=['Equity', 'Bond', 'Money', 'Alternative'])

        for ct in ['Equity', 'Bond', 'Money', 'Alternative']:
            tmp_ctg_return = individual_return * np.where(ctg == ct, 1, np.nan)
            tmp_ctg_return = tmp_ctg_return.dropna(how='all')
            if tmp_ctg_return.shape[0] > 0:
                individual_return_categorized[ct] = tmp_ctg_return.mean(axis=1)
            else:
                individual_return_categorized[ct] = np.nan

        individual_return_categorized = individual_return_categorized.fillna(0)
        individual_return_categorized = individual_return_categorized.sort_index()
        for col in individual_return_categorized.columns:
            idx = 0
            stop = 0
            while stop == 0 and idx < individual_return_categorized.shape[0]:
                if idx < individual_return_categorized.shape[0] - 1:
                    if individual_return_categorized.loc[individual_return_categorized.index[idx], col] == 0 and \
                            individual_return_categorized.loc[individual_return_categorized.index[idx + 1], col] == 0:
                        individual_return_categorized.loc[individual_return_categorized.index[idx], col] = np.nan
                        idx += 1
                    else:
                        stop = 1
                else:
                    if individual_return_categorized.loc[individual_return_categorized.index[idx], col] == 0:
                        individual_return_categorized.loc[individual_return_categorized.index[idx], col] = np.nan
                        idx += 1
                    else:
                        stop = 1

            idx = individual_return_categorized.shape[0] - 1
            stop = 0
            while stop == 0 and idx >= 0:
                if idx > 0:
                    if individual_return_categorized.loc[individual_return_categorized.index[idx], col] == 0 and \
                            individual_return_categorized.loc[individual_return_categorized.index[idx - 1], col] == 0:
                        individual_return_categorized.loc[individual_return_categorized.index[idx], col] = np.nan
                        idx -= 1
                    else:
                        stop = 1
                else:
                    if individual_return_categorized.loc[individual_return_categorized.index[idx], col] == 0:
                        individual_return_categorized.loc[individual_return_categorized.index[idx], col] = np.nan
                        idx -= 1
                    else:
                        stop = 1

        individual_pv_categorized = (individual_return_categorized + 1).cumprod()

        horizon = [365, 1095]
        individual_perf_metrics_list = []
        if individual_performance.shape[0] > 6 and individual_pv_categorized.shape[0] > 0:
            for date in individual_performance.index[6:]:
                individual_perf_metrics_tmp = []
                for hrz in horizon:
                    ub_date = date
                    lb_date = date - datetime.timedelta(days=hrz)
                    if lb_date >= individual_pv_categorized.index[0]:
                        nav_daily_tmp = individual_pv_categorized.loc[lb_date: ub_date, :]
                        nav_fill = nav_daily_tmp.ffill().bfill()
                        if nav_fill.shape[0] >= int(hrz * 0.3):
                            nav_fill_count = nav_fill.count()
                            return_hrz = pd.DataFrame(
                                (nav_fill.iloc[-1] / nav_fill.iloc[0]) ** (245 / (nav_fill_count - 1)) - 1).transpose()
                            return_hrz.replace([np.inf, -np.inf], np.nan, inplace=True)
                            return_hrz.index = [date]

                            return_daily_tmp = nav_fill / nav_fill.shift() - 1
                            return_daily_tmp_count = nav_fill.count()
                            vol_tmp_hrz = pd.DataFrame(nav_fill.std() * (return_daily_tmp_count ** 0.5)).transpose()
                            vol_tmp_hrz[vol_tmp_hrz < 0.0000001] = np.nan
                            vol_tmp_hrz.replace([np.inf, -np.inf], np.nan, inplace=True)
                            vol_tmp_hrz.index = [date]

                            sharperatio = return_hrz / vol_tmp_hrz

                            vol_tmp = pd.DataFrame(return_daily_tmp.std() * (245 ** 0.5)).transpose()
                            vol_tmp[vol_tmp < 0.0000001] = np.nan
                            vol_tmp.replace([np.inf, -np.inf], np.nan, inplace=True)
                            vol_tmp.index = [date]

                            drawdown_tmp = nav_fill / nav_fill.cummax() - 1
                            mdd_tmp = pd.DataFrame(drawdown_tmp.min()).transpose()
                            mdd_tmp = - mdd_tmp
                            mdd_tmp[mdd_tmp < 0.0000001] = np.nan
                            mdd_tmp.replace([np.inf, -np.inf], np.nan, inplace=True)
                            mdd_tmp.index = [date]

                            return_hrz.columns = [col + '_ann_return_' + str(hrz) for col in return_hrz.columns]
                            vol_tmp.columns = [col + '_ann_volatility_' + str(hrz) for col in vol_tmp.columns]
                            sharperatio.columns = [col + '_ann_sharperatio_' + str(hrz) for col in sharperatio.columns]
                            mdd_tmp.columns = [col + '_mdd_' + str(hrz) for col in mdd_tmp.columns]

                            individual_perf_metrics_tmp.append(return_hrz)
                            individual_perf_metrics_tmp.append(vol_tmp)
                            individual_perf_metrics_tmp.append(sharperatio)
                            individual_perf_metrics_tmp.append(mdd_tmp)
                        else:
                            return_hrz = pd.DataFrame(columns=individual_pv_categorized.columns, index=[date])
                            vol_tmp = pd.DataFrame(columns=individual_pv_categorized.columns, index=[date])
                            sharperatio = pd.DataFrame(columns=individual_pv_categorized.columns, index=[date])
                            mdd_tmp = pd.DataFrame(columns=individual_pv_categorized.columns, index=[date])

                            return_hrz.columns = [col + '_ann_return_' + str(hrz) for col in return_hrz.columns]
                            vol_tmp.columns = [col + '_ann_volatility_' + str(hrz) for col in vol_tmp.columns]
                            sharperatio.columns = [col + '_ann_sharperatio_' + str(hrz) for col in sharperatio.columns]
                            mdd_tmp.columns = [col + '_mdd_' + str(hrz) for col in mdd_tmp.columns]

                            individual_perf_metrics_tmp.append(return_hrz)
                            individual_perf_metrics_tmp.append(vol_tmp)
                            individual_perf_metrics_tmp.append(sharperatio)
                            individual_perf_metrics_tmp.append(mdd_tmp)
                    else:
                        return_hrz = pd.DataFrame(columns=individual_pv_categorized.columns, index=[date])
                        vol_tmp = pd.DataFrame(columns=individual_pv_categorized.columns, index=[date])
                        sharperatio = pd.DataFrame(columns=individual_pv_categorized.columns, index=[date])
                        mdd_tmp = pd.DataFrame(columns=individual_pv_categorized.columns, index=[date])

                        return_hrz.columns = [col + '_ann_return_' + str(hrz) for col in return_hrz.columns]
                        vol_tmp.columns = [col + '_ann_volatility_' + str(hrz) for col in vol_tmp.columns]
                        sharperatio.columns = [col + '_ann_sharperatio_' + str(hrz) for col in sharperatio.columns]
                        mdd_tmp.columns = [col + '_mdd_' + str(hrz) for col in mdd_tmp.columns]

                        individual_perf_metrics_tmp.append(return_hrz)
                        individual_perf_metrics_tmp.append(vol_tmp)
                        individual_perf_metrics_tmp.append(sharperatio)
                        individual_perf_metrics_tmp.append(mdd_tmp)
                individual_perf_metrics_list.append(pd.concat(individual_perf_metrics_tmp, axis=1))

            individual_perf_metrics = pd.concat(individual_perf_metrics_list)
            individual = pd.concat([individual_performance, individual_perf_metrics], axis=1)
        else:
            individual = individual_performance.copy()

        # try:
        #     individual_perf_metrics = pd.concat(individual_perf_metrics_list)
        # except:
        #     print("fail for " + str(id))
        individual = individual.sort_index()
        output_path = os.path.join(savepath, str(id) + '.csv')
        individual.to_csv(output_path)
        print("Successful for PM with ID " + str(id))


def manager_individual_data(start, end):
    # config_file_path = os.path.join(addpath.config_path, "config.conf")
    # config = configparser.ConfigParser()
    # config.read(config_file_path)

    start_date = datetime.datetime.strptime(start, "%Y-%m-%d")
    end_date = datetime.datetime.strptime(end, "%Y-%m-%d")

    manager_raw_path = os.path.join(addpath.historical_path, "fund_manager")
    category_path = os.path.join(addpath.categorization_path, 'fund_category')
    aum_path = os.path.join(addpath.historical_path, "aum_info")
    nav_path = os.path.join(addpath.bundle_path, "daily")
    savepath = os.path.join(addpath.temp_data_path, "pm_temps")

    if os.path.exists(savepath):
        pass
    else:
        os.makedirs(savepath)

    dates = pd.date_range(start_date, end_date, freq='M').tolist()
    aum_list = {}
    for date in dates:
        aum_factors = pd.read_csv(os.path.join(aum_path, date.strftime("%Y-%m-%d") + ".csv"), index_col=0)
        category_tmp = pd.read_csv(os.path.join(category_path, date.strftime("%Y-%m-%d") + ".csv"), index_col=0)
        category_tmp = category_tmp.dropna()
        aum_list[date] = pd.concat([aum_factors, category_tmp], axis=1)

    category_list = []
    for date in dates:
        category_tmp = pd.read_csv(os.path.join(category_path, date.strftime("%Y-%m-%d") + ".csv"),
                                   index_col=0)[['category']].transpose()
        category_tmp.index = [date]
        category_list.append(category_tmp)
    categories = pd.concat(category_list)
    categories = categories.resample('D').last().bfill()
    print('Read data finished')
    fund_manager_map_path = os.path.join(manager_raw_path, "manager_mapping_raw.csv")
    fund_manager_map = pd.read_csv(fund_manager_map_path,
                                   parse_dates=['fund_manager_joinstartdate', 'fund_manager_joinenddate'], index_col=0)
    fund_manager_map.index.name = 'ms_secid'
    fund_manager_map = fund_manager_map.reset_index()
    fund_manager_map = fund_manager_map.loc[:,
                       ['fund_manager_id', 'fund_manager_joinstartdate', 'fund_manager_joinenddate',
                        'fund_manager_careerstartyear', 'ms_secid']]
    fund_manager_map = fund_manager_map.dropna(subset=['fund_manager_id', 'fund_manager_joinstartdate'])
    fund_manager_map['fund_manager_id'] = fund_manager_map['fund_manager_id'].map(lambda x: int(x))

    manager_id_list = list(set(fund_manager_map['fund_manager_id'].tolist()))
    # manager_id_list = [150821, 189421, 192576, 135443, 121265, 195922, 177098, 187686, 170975, 139077, 148490,
    #                    154429, 106086, 5043, 160940, 160994, 156192, 156194]

    fund_manager_map['fund_manager_joinenddate'] = fund_manager_map['fund_manager_joinenddate'].fillna(datetime.datetime.strptime(datetime.datetime.today().strftime("%Y-%m-%d"),"%Y-%m-%d"))

    num_pms = len(manager_id_list)
    chunk_size = 1200
    chunk_number = int(num_pms / chunk_size) + 1

    pm_id_list = {}
    for chunk in range(chunk_number):
        if chunk == chunk_number - 1:
            ub = num_pms
        else:
            ub = (chunk + 1) * chunk_size
        lb = chunk * chunk_size
        pm_id_list[chunk] = manager_id_list[lb:ub]

    print('Sub-processes begins!')
    for chunk in range(chunk_number):
    # for chunk in [1, 2, 16, 17]:
        p = multiprocessing.Process(target=pm_data_helper, args=(
        pm_id_list[chunk], fund_manager_map, savepath, aum_list, nav_path, categories, end_date))
        p.start()
    print('Individual factor finished')

    # num_pms = len(manager_id_list)
    # chunk_size = 500
    # chunk_number = int(num_pms / chunk_size) + 1
    #
    # pool = multiprocessing.Pool()
    #
    # for chunk in range(chunk_number):
    #     if chunk == chunk_number - 1:
    #         ub = num_pms
    #     else:
    #         ub = (chunk + 1) * chunk_size
    #     lb = chunk * chunk_size
    #     pm_id_list = manager_id_list[lb:ub].copy()
    #
    #     pool.apply(pm_data_helper, args=(pm_id_list, fund_manager_map, savepath, return_list,))
    #     print('Sub-processes begin!')
    #
    # pool.close()
    # pool.join()
    # print('Sub-processes done!')


def manager_individual_data_rank_helper(files, file_dict, start_date, end_date, data_dict, savepath):
    for file in files:
        dates = [date for date in file_dict[file[:-4]].index if start_date <= date <= end_date]
        rank_data_list = []
        for date in dates:
            rank_data_tmp = pd.DataFrame(data_dict[date][data_dict[date].index == float(file[:-4])])
            rank_data_tmp['date'] = date
            rank_data_tmp = rank_data_tmp.set_index('date')
            rank_data_list.append(rank_data_tmp)
        if len(rank_data_list) != 0:
            rank_data = pd.concat(rank_data_list)
            rank_data = rank_data.sort_index()
            output_data = pd.concat([file_dict[file[:-4]], rank_data], axis=1)
        else:
            output_data = file_dict[file[:-4]]
        output_data.to_csv(os.path.join(savepath, file))


def manager_individual_data_rank(start, end):
    config_file_path = os.path.join(addpath.config_path, "config.conf")
    config = configparser.ConfigParser()
    config.read(config_file_path)

    start_date = datetime.datetime.strptime(start, "%Y-%m-%d")
    end_date = datetime.datetime.strptime(end, "%Y-%m-%d")

    raw_path = os.path.join(addpath.temp_data_path, "pm_temps")
    savepath = os.path.join(addpath.historical_path, 'fund_manager_dataset')

    if os.path.exists(savepath):
        pass
    else:
        os.makedirs(savepath)

    comparing_variables = ['fund_manager_id', 'Equity_aum', 'Equity_past_mean_aum', 'Bond_aum', 'Bond_past_mean_aum',
                           'Money_aum', 'Money_past_mean_aum', 'Alternative_aum', 'Alternative_past_mean_aum',
                           'Equity_ann_return_365', 'Bond_ann_return_365', 'Money_ann_return_365',
                           'Alternative_ann_return_365',
                           'Equity_ann_volatility_365', 'Bond_ann_volatility_365', 'Money_ann_volatility_365',
                           'Alternative_ann_volatility_365',
                           'Equity_ann_sharperatio_365', 'Bond_ann_sharperatio_365', 'Money_ann_sharperatio_365',
                           'Alternative_ann_sharperatio_365',
                           'Equity_mdd_365', 'Bond_mdd_365', 'Money_mdd_365', 'Alternative_mdd_365',
                           'Equity_ann_return_1095', 'Bond_ann_return_1095', 'Money_ann_return_1095',
                           'Alternative_ann_return_1095',
                           'Equity_ann_volatility_1095', 'Bond_ann_volatility_1095', 'Money_ann_volatility_1095',
                           'Alternative_ann_volatility_1095',
                           'Equity_ann_sharperatio_1095', 'Bond_ann_sharperatio_1095', 'Money_ann_sharperatio_1095',
                           'Alternative_ann_sharperatio_1095',
                           'Equity_mdd_1095', 'Bond_mdd_1095', 'Money_mdd_1095', 'Alternative_mdd_1095']

    dates = pd.date_range(start_date, end_date, freq='M').tolist()
    tmp_data_dict = {}
    for date in dates:
        tmp_data_dict[date] = []
    files = os.listdir(raw_path)

    file_dict = {}
    for file in files:
        tmp = pd.read_csv(os.path.join(raw_path, file), parse_dates=[0], index_col=0)
        columns = [col for col in tmp.columns if col in comparing_variables]
        file_dict[file[:-4]] = tmp.copy()
        tmp = tmp.loc[:, columns]
        for date in tmp.index:
            if date in dates:
                tmp1 = pd.DataFrame(tmp.loc[date]).transpose()
                tmp1 = tmp1.set_index('fund_manager_id')
                tmp_data_dict[date].append(tmp1)

    data_dict = {}
    for date in dates:
            data_dict[date] = pd.concat(tmp_data_dict[date])
            data_dict[date] = data_dict[date].rank(ascending=False, pct=True)
            columns = [col + '_rank_in_category' for col in data_dict[date].columns]
            data_dict[date].columns = columns

    num_pms = len(files)
    chunk_size = 1200
    chunk_number = int(num_pms / chunk_size) + 1

    pm_id_list = {}
    for chunk in range(chunk_number):
        if chunk == chunk_number - 1:
            ub = num_pms
        else:
            ub = (chunk + 1) * chunk_size
        lb = chunk * chunk_size
        pm_id_list[chunk] = files[lb:ub]

    for chunk in range(chunk_number):
        # for chunk in [1, 2, 16, 17]:
        p = multiprocessing.Process(target=manager_individual_data_rank_helper, args=(
            pm_id_list[chunk], file_dict, start_date, end_date, data_dict, savepath, ))
        p.start()
        print('Sub-processes begins!')

    # for file in files:
    #     dates = [date for date in file_dict[file[:-4]].index if start_date <= date <= end_date]
    #     rank_data_list = []
    #     for date in dates:
    #         rank_data_tmp = pd.DataFrame(data_dict[date][data_dict[date].index == float(file[:-4])])
    #         rank_data_tmp['date'] = date
    #         rank_data_tmp = rank_data_tmp.set_index('date')
    #         rank_data_list.append(rank_data_tmp)
    #     rank_data = pd.concat(rank_data_list)
    #     rank_data = rank_data.sort_index()
    #     output_data = pd.concat([file_dict[file[:-4]], rank_data], axis=1)
    #     output_data.to_csv(os.path.join(savepath, file))


def manager_factor_helper(fund_id_list_tmp, fund_manager_map, individual_path):
    result_list = []
    manager_data_list = os.listdir(individual_path)
    manager_data_list = [pm_id[:-4] for pm_id in manager_data_list]

    for id in fund_id_list_tmp:
        temp_map = fund_manager_map[fund_manager_map['ms_secid'] == id]
        fund_manager_list = []
        individual_performance_list = []
        for idx in temp_map.index:
            if str(int(temp_map.loc[idx, 'fund_manager_id'])) in manager_data_list:
                period = pd.date_range(temp_map.loc[idx, 'fund_manager_joinstartdate'],
                                       temp_map.loc[idx, 'fund_manager_joinenddate'] + datetime.timedelta(days=30),
                                       freq='M')
                fund_manager_tmp = pd.DataFrame(index=period)
                fund_manager_tmp['fund_manager_id'] = temp_map.loc[idx, 'fund_manager_id']
                fund_manager_list.append(fund_manager_tmp)

                file_path = os.path.join(individual_path, str(int(temp_map.loc[idx, 'fund_manager_id'])) + '.csv')
                try:
                    indiv = pd.read_csv(file_path, parse_dates=[0], index_col=0)
                    indiv['fund_manager_id'] = temp_map.loc[idx, 'fund_manager_id']
                    individual_performance_list.append(indiv)
                except:
                    print("Fail for " + str(int(temp_map.loc[idx, 'fund_manager_id'])))

        if len(individual_performance_list) == 0:
            print("Fail for " + str(id))
            continue

        individual_performance = pd.concat(individual_performance_list)
        individual_performance.index.name = 'index'
        individual_performance = individual_performance.reset_index()
        fund_manager = pd.concat(fund_manager_list)
        fund_manager = fund_manager.sort_index()
        fund_manager.index.name = 'index'
        fund_manager = fund_manager.reset_index()
        columns_for_indi = individual_performance.columns.tolist()
        for col in individual_performance.columns:
            if len(col) > 20:
                if col[:20] == 'current_fund_tenure_' and col != 'current_fund_tenure_' + id:
                    columns_for_indi.remove(col)
        individual_performance_tmp = individual_performance.loc[:, columns_for_indi]
        fund_manager = pd.merge(fund_manager, individual_performance_tmp, how='left',
                                left_on=['index', 'fund_manager_id'], right_on=['index', 'fund_manager_id'])

        fund_manager_av_perf = fund_manager.drop(columns='fund_manager_id').groupby('index').mean()
        fund_manager_av_perf = fund_manager_av_perf.rename(columns={'current_fund_tenure_' + id: 'current_pm_tenure'})
        fund_manager_av_perf['ms_secid'] = id
        # fund_manager_av_perf.to_csv(os.path.join(addpath.temp_data_path, 'pm_fund_temps', id + '.csv'))

        result_list.append(fund_manager_av_perf)

    return result_list


def fund_level_manager_factor(start, end):
    # config_file_path = os.path.join(addpath.config_path, "config.conf")
    # config = configparser.ConfigParser()
    # config.read(config_file_path)

    manager_raw_path = os.path.join(addpath.historical_path, "fund_manager")
    individual_path = os.path.join(addpath.historical_path, 'fund_manager_dataset')
    savepath = os.path.join(addpath.historical_path, 'fund_manager_factor')

    if os.path.exists(savepath):
        pass
    else:
        os.makedirs(savepath)

    fund_manager_map_path = os.path.join(manager_raw_path, "manager_mapping_raw.csv")
    fund_manager_map = pd.read_csv(fund_manager_map_path,
                                   parse_dates=['fund_manager_joinstartdate', 'fund_manager_joinenddate'], index_col=0)
    fund_manager_map.index.name = 'ms_secid'
    fund_manager_map = fund_manager_map.reset_index()
    fund_manager_map = fund_manager_map.loc[:,
                       ['fund_manager_id', 'fund_manager_joinstartdate', 'fund_manager_joinenddate',
                        'fund_manager_careerstartyear', 'ms_secid']]
    fund_manager_map = fund_manager_map.dropna(subset=['fund_manager_id', 'fund_manager_joinstartdate'])
    fund_manager_map['fund_manager_id'] = fund_manager_map['fund_manager_id'].map(lambda x: int(x))

    fund_id_list = list(set(fund_manager_map['ms_secid'].tolist()))
    fund_manager_map['fund_manager_joinenddate'] = fund_manager_map['fund_manager_joinenddate'].fillna(
        datetime.datetime.strptime(datetime.datetime.today().strftime("%Y-%m-%d"), "%Y-%m-%d"))

    num_pms = len(fund_id_list)
    chunk_size = 800
    chunk_number = int(num_pms / chunk_size) + 1

    fund_id_dict = {}
    for chunk in range(chunk_number):
        if chunk == chunk_number - 1:
            ub = num_pms
        else:
            ub = (chunk + 1) * chunk_size
        lb = chunk * chunk_size
        fund_id_dict[chunk] = fund_id_list[lb:ub]

    pool = multiprocessing.Pool()
    fund_level_manager_factors_list = []
    for chunk in range(chunk_number):
        # for chunk in [1, 2, 16, 17]:
        # pool.apply_async(manager_factor_helper, args=(fund_id_dict[chunk], fund_manager_map, individual_path, ))
        fund_level_manager_factors_list.append(pool.apply_async(manager_factor_helper, args=(fund_id_dict[chunk], fund_manager_map, individual_path, )))
        print('Sub-processes begins!')
    pool.close()  # 关闭进程池，表示不能再往进程池中添加进程，需要在join之前调用
    pool.join()  # 等待进程池中的所有进程执行完毕
    print("Sub-process(es) done.")

    fund_level_manager_factors_list_new = []
    for res in fund_level_manager_factors_list:
        fund_level_manager_factors_list_new = fund_level_manager_factors_list_new + res.get()
    fund_level_manager_factors = pd.concat(fund_level_manager_factors_list_new)

    dates = pd.date_range(start, end, freq='M')
    for date in dates:
        mf = fund_level_manager_factors[fund_level_manager_factors.index == date]
        output_path = os.path.join(savepath, date.strftime('%Y-%m-%d') + '.csv')
        mf = mf.set_index('ms_secid')
        mf.to_csv(output_path)


if __name__ == '__main__':
    start = '2014-12-31'
    end = '2020-12-31'
    # manager_individual_data(start, end)
    # manager_individual_data_rank(start, end)
    fund_level_manager_factor(start, end)
