from algorithm import addpath
import multiprocessing
import numpy as np
import pandas as pd
import os
from datetime import datetime, timedelta
from calendar import monthrange


def sfp_formation_helper(industry_level, factor_metric, effect_direction, start, end):
    industry_map = pd.read_csv(os.path.join(addpath.config_path, "ashare_symbol_list.csv"), index_col='Stkcd')[
        [industry_level]]
    industry_list = list(set(industry_map[industry_level].tolist()))

    iu_path = os.path.join(addpath.data_path, "Ashare", "investment_universe")

    formation_dates = pd.date_range(start, end, freq='1M')
    formation_dates = [formation_date for formation_date in formation_dates if formation_date.month in [1, 4, 7, 10]]

    cs_factor_path = os.path.join(addpath.data_path, "Ashare", "cs_factors")
    sfp_path = os.path.join(addpath.data_path, "Ashare", "single_factor_portfolios", industry_level)
    if os.path.exists(sfp_path):
        pass
    else:
        os.makedirs(sfp_path)

    for industry in industry_list:
        portfolio_selected_return_qtr = {}
        portfolio_other_return_qtr = {}
        portfolio_selected_return_semiyear = {}
        portfolio_other_return_semiyear = {}
        portfolio_selected_return_annyear = {}
        portfolio_other_return_annyear = {}
        for date in formation_dates[8:]:
            if date.month == 5:
                iu = pd.read_csv(os.path.join(iu_path, (date - timedelta(days=1)).strftime('%Y-%m-%d') + '.csv'))['symbol'].tolist()
            else:
                iu = pd.read_csv(os.path.join(iu_path, date.strftime('%Y-%m-%d') + '.csv'))['symbol'].tolist()
            factors = pd.read_csv(os.path.join(cs_factor_path, date.strftime('%Y-%m-%d') + '.csv'), index_col=0)
            industry_map_tmp = industry_map[industry_map[industry_level] == industry]
            iu = [s for s in iu if s in factors.index and s in industry_map_tmp.index]

            try:
                factor = factors.loc[iu, [factor_metric, 'fwd_return_qtr', 'fwd_return_semiyear', 'fwd_return_annyear']]
                factor = factor.dropna()
                if factor.shape[0] >= 4:
                    if effect_direction == 'Positive':
                        factor['rank'] = factor[factor_metric].rank(pct=True, ascending=True)
                    else:
                        factor['rank'] = factor[factor_metric].rank(pct=True, ascending=False)
                    factor.index.name = 'ticker'
                    portfolio_top = factor[factor['rank'] >= 0.75]
                    if portfolio_top.shape[0] == 0:
                        print("No data for " + factor_metric + " on " + date.strftime('%Y-%m-%d'))
                    else:
                        portfolio_selected_return_qtr[date] = portfolio_top['fwd_return_qtr'].mean()
                        portfolio_selected_return_semiyear[date] = portfolio_top['fwd_return_semiyear'].mean()
                        portfolio_selected_return_annyear[date] = portfolio_top['fwd_return_annyear'].mean()
                        portfolio_bottom = factor[factor['rank'] < 0.75]
                        portfolio_other_return_qtr[date] = portfolio_bottom['fwd_return_qtr'].mean()
                        portfolio_other_return_semiyear[date] = portfolio_bottom['fwd_return_semiyear'].mean()
                        portfolio_other_return_annyear[date] = portfolio_bottom['fwd_return_annyear'].mean()
                else:
                    print("No data for " + factor_metric + " on " + date.strftime('%Y-%m-%d'))
            except:
                print("No data for " + factor_metric  + " on " + date.strftime('%Y-%m-%d'))

        portfolio_selected_ret_qtr = pd.DataFrame(portfolio_selected_return_qtr.values(), index=portfolio_selected_return_qtr.keys(), columns=[factor_metric + '_selected_qtr'])
        portfolio_selected_ret_semiyear = pd.DataFrame(portfolio_selected_return_semiyear.values(), index=portfolio_selected_return_semiyear.keys(), columns=[factor_metric + '_selected_semiyear'])
        portfolio_selected_ret_annyear = pd.DataFrame(portfolio_selected_return_annyear.values(), index=portfolio_selected_return_annyear.keys(), columns=[factor_metric + '_selected_annyear'])
        portfolio_other_ret_qtr = pd.DataFrame(portfolio_other_return_qtr.values(), index=portfolio_other_return_qtr.keys(), columns=[factor_metric + '_other_qtr'])
        portfolio_other_ret_semiyear = pd.DataFrame(portfolio_other_return_semiyear.values(), index=portfolio_other_return_semiyear.keys(), columns=[factor_metric + '_other_semiyear'])
        portfolio_other_ret_annyear = pd.DataFrame(portfolio_other_return_annyear.values(), index=portfolio_other_return_annyear.keys(), columns=[factor_metric + '_other_annyear'])

        portfolio_return = pd.concat([portfolio_selected_ret_qtr, portfolio_selected_ret_semiyear,
                                      portfolio_selected_ret_annyear, portfolio_other_ret_qtr,
                                      portfolio_other_ret_semiyear, portfolio_other_ret_annyear], axis=1)
        portfolio_return = portfolio_return.sort_index()
        portfolio_return.index.name = 'date'
        portfolio_return[factor_metric + '_difference_qtr'] = portfolio_return[factor_metric + '_selected_qtr'] - portfolio_return[factor_metric + '_other_qtr']
        portfolio_return[factor_metric + '_difference_semiyear'] = portfolio_return[factor_metric + '_selected_semiyear'] - portfolio_return[factor_metric + '_other_semiyear']
        portfolio_return[factor_metric + '_difference_annyear'] = portfolio_return[factor_metric + '_selected_annyear'] - portfolio_return[factor_metric + '_other_annyear']
        portfolio_return.to_csv(os.path.join(sfp_path, factor_metric + "_" + industry + ".csv"))


def sfp_formation(industry_level, start, end):
    factor_list = pd.read_csv(os.path.join(addpath.config_path, "ashare_factor_list.csv"), index_col='factor_list')
    pool = multiprocessing.Pool()
    for factor_metric in factor_list.index.tolist():
        effect_direction = factor_list.loc[factor_metric, 'effect_direction']
        # sfp_formation_helper(factor_metric, effect_direction)
        pool.apply_async(sfp_formation_helper, args=(industry_level, factor_metric, effect_direction, start, end, ))
        print('Sub-processes begins!')
    pool.close()
    pool.join()
    print('Sub-processes done!')


def sfp_analysis_helper(industry_level, formation_date, industry):
    sfp_analysis_path = os.path.join(addpath.data_path, "Ashare", "sfp_analysis", industry_level, formation_date.strftime("%Y-%m-%d"))
    if os.path.exists(sfp_analysis_path):
        pass
    else:
        os.makedirs(sfp_analysis_path)
    sfp_path = os.path.join(addpath.data_path, "Ashare", "single_factor_portfolios", industry_level)
    factor_list = pd.read_csv(os.path.join(addpath.config_path, "ashare_factor_list.csv"), index_col='factor_list')
    pr_analysis_list = []

    for factor_metric in factor_list.index.tolist():
        try:
            portfolio_return = pd.read_csv(os.path.join(sfp_path, factor_metric + "_" + industry + ".csv"), index_col=0, parse_dates=[0])
            sample_period_start = datetime(formation_date.year - 5, formation_date.month, formation_date.day)

            if formation_date.month == 1:
                sample_period_end_qtr = datetime(formation_date.year - 1, 10, 31)
            elif formation_date.month == 4:
                sample_period_end_qtr = datetime(formation_date.year, 1, 31)
            elif formation_date.month == 7:
                sample_period_end_qtr = datetime(formation_date.year, 4, 30)
            else:
                sample_period_end_qtr = datetime(formation_date.year, 7, 31)
            portfolio_return_qtr = portfolio_return[sample_period_start <= portfolio_return.index]
            portfolio_return_qtr = portfolio_return_qtr[portfolio_return_qtr.index <= sample_period_end_qtr]
            dates_qtr = [d for d in portfolio_return_qtr.index if d.month in [1, 4, 7, 10]]
            portfolio_return_qtr = portfolio_return_qtr.loc[dates_qtr, :]
            pr_mean_qtr = portfolio_return_qtr.mean()
            pr_std_qtr = portfolio_return_qtr.std()
            pr_qtr = pd.concat([pr_mean_qtr, pr_std_qtr], axis=1)
            pr_qtr.columns = ['pr_mean_qtr', 'pr_std_qtr']
            pr_qtr['pr_tstat_qtr'] = pr_qtr['pr_mean_qtr'] / (pr_qtr['pr_std_qtr'] / (pr_qtr.shape[0] ** 0.5))
            pr_qtr['pr_num_qtr'] = pr_qtr.shape[0]
            pr_qtr = pr_qtr.loc[[factor_metric + '_selected_qtr', factor_metric + '_other_qtr', factor_metric + '_difference_qtr'], :]
            pr_qtr = pr_qtr.rename(index={factor_metric + '_selected_qtr': factor_metric + '_selected',
                                          factor_metric + '_other_qtr': factor_metric + '_other',
                                          factor_metric + '_difference_qtr': factor_metric + '_difference'})

            if formation_date.month == 1:
                sample_period_end_sa = datetime(formation_date.year - 1, 7, 31)
            elif formation_date.month == 4:
                sample_period_end_sa = datetime(formation_date.year - 1, 10, 31)
            elif formation_date.month == 7:
                sample_period_end_sa = datetime(formation_date.year, 1, 31)
            else:
                sample_period_end_sa = datetime(formation_date.year, 4, 30)
            portfolio_return_sa = portfolio_return[sample_period_start <= portfolio_return.index]
            portfolio_return_sa = portfolio_return_sa[portfolio_return_sa.index <= sample_period_end_sa]
            dates_sa = [d for d in portfolio_return_sa.index if d.month in [4, 10]]
            portfolio_return_sa = portfolio_return_sa.loc[dates_sa, :]
            pr_mean_sa = portfolio_return_sa.mean()
            pr_std_sa = portfolio_return_sa.std()
            pr_sa = pd.concat([pr_mean_sa, pr_std_sa], axis=1)
            pr_sa.columns = ['pr_mean_sa', 'pr_std_sa']
            pr_sa['pr_tstat_sa'] = pr_sa['pr_mean_sa'] / (pr_sa['pr_std_sa'] / (pr_sa.shape[0] ** 0.5))
            pr_sa['pr_num_sa'] = pr_sa.shape[0]
            pr_sa = pr_sa.loc[
                     [factor_metric + '_selected_semiyear', factor_metric + '_other_semiyear', factor_metric + '_difference_semiyear'],
                     :]
            pr_sa = pr_sa.rename(index={factor_metric + '_selected_semiyear': factor_metric + '_selected',
                                          factor_metric + '_other_semiyear': factor_metric + '_other',
                                          factor_metric + '_difference_semiyear': factor_metric + '_difference'})

            if formation_date.month == 1:
                sample_period_end_ann = datetime(formation_date.year - 1, 1, 31)
            elif formation_date.month == 4:
                sample_period_end_ann = datetime(formation_date.year - 1, 4, 30)
            elif formation_date.month == 7:
                sample_period_end_ann = datetime(formation_date.year - 1, 7, 31)
            else:
                sample_period_end_ann = datetime(formation_date.year - 1, 10, 31)
            portfolio_return_ann = portfolio_return[sample_period_start <= portfolio_return.index]
            portfolio_return_ann = portfolio_return_ann[portfolio_return_ann.index <= sample_period_end_ann]
            dates_ann = [d for d in portfolio_return_ann.index if d.month == 4]
            portfolio_return_ann = portfolio_return_ann.loc[dates_ann, :]
            pr_mean_ann = portfolio_return_ann.mean()
            pr_std_ann = portfolio_return_ann.std()
            pr_ann = pd.concat([pr_mean_ann, pr_std_ann], axis=1)
            pr_ann.columns = ['pr_mean_ann', 'pr_std_ann']
            pr_ann['pr_tstat_ann'] = pr_ann['pr_mean_ann'] / (pr_ann['pr_std_ann'] / (pr_ann.shape[0] ** 0.5))
            pr_ann['pr_num_ann'] = pr_ann.shape[0]
            pr_ann = pr_ann.loc[
                     [factor_metric + '_selected_annyear', factor_metric + '_other_annyear', factor_metric + '_difference_annyear'],
                     :]
            pr_ann = pr_ann.rename(index={factor_metric + '_selected_annyear': factor_metric + '_selected',
                                          factor_metric + '_other_annyear': factor_metric + '_other',
                                          factor_metric + '_difference_annyear': factor_metric + '_difference'})

            pr_analysis_tmp = pd.concat([pr_qtr, pr_sa, pr_ann], axis=1)
            pr_analysis_tmp.index.name = 'validation_metrics'
            pr_analysis_list.append(pr_analysis_tmp)
        except:
            print("No data for " + factor_metric  + " on " + formation_date.strftime('%Y-%m-%d') + "for " + industry)
    pr_analysis = pd.concat(pr_analysis_list)
    pr_analysis.to_csv(os.path.join(sfp_analysis_path, industry + ".csv"))


def sfp_analysis(industry_level, start, end):
    industry_map = pd.read_csv(os.path.join(addpath.config_path, "ashare_symbol_list.csv"), index_col='Stkcd')[
        [industry_level]]
    industry_list = list(set(industry_map[industry_level].tolist()))

    formation_dates = pd.date_range(start, end, freq='1M')
    formation_dates = [formation_date for formation_date in formation_dates if formation_date.month in [1, 4, 7, 10]]

    pool = multiprocessing.Pool()
    for formation_date in formation_dates:
        for industry in industry_list:
            # sfp_analysis_helper(formation_date, industry)
            pool.apply_async(sfp_analysis_helper, args=(industry_level, formation_date, industry, ))
            print('Sub-processes begins!')
    pool.close()
    pool.join()
    print('Sub-processes done!')


def factor_selection_helper(industry_level, formation_date, horizon, threshold):
    selected_factor_path = os.path.join(addpath.data_path, "Ashare", "selected_factor", industry_level, horizon)
    if os.path.exists(selected_factor_path):
        pass
    else:
        os.makedirs(selected_factor_path)
    industry_map = pd.read_csv(os.path.join(addpath.config_path, "ashare_symbol_list.csv"), index_col='Stkcd')[
        [industry_level]]
    industry_list = list(set(industry_map[industry_level].tolist()))
    factor_list = pd.read_csv(os.path.join(addpath.config_path, "ashare_factor_list.csv"), index_col='factor_list')
    sfp_analysis_path = os.path.join(addpath.data_path, "Ashare", "sfp_analysis", industry_level, formation_date.strftime("%Y-%m-%d"))

    factor_selected_list = []
    for industry in industry_list:
        pr_analysis = pd.read_csv(os.path.join(sfp_analysis_path, industry + ".csv"), index_col='validation_metrics')
        pr_analysis_index = [idx for idx in pr_analysis.index if idx[-11:] == "_difference"]
        if horizon == "qtr":
            pr_analysis_column = ['pr_mean_qtr', 'pr_std_qtr', 'pr_tstat_qtr', 'pr_num_qtr']
            tstat_name = 'pr_tstat_qtr'
            mean_name = 'pr_mean_qtr'
        elif horizon == "semiyear":
            pr_analysis_column = ['pr_mean_sa', 'pr_std_sa', 'pr_tstat_sa', 'pr_num_sa']
            tstat_name = 'pr_tstat_sa'
            mean_name = 'pr_mean_sa'
        elif horizon == "annyear":
            pr_analysis_column = ['pr_mean_ann', 'pr_std_ann', 'pr_tstat_ann', 'pr_num_ann']
            tstat_name = 'pr_tstat_ann'
            mean_name = 'pr_mean_ann'

        filter_range = pr_analysis.loc[pr_analysis_index, pr_analysis_column]
        filter_range = filter_range[filter_range[tstat_name] >= threshold]
        if filter_range.shape[0] > 0:
            filter_range.index = [idx[:-11] for idx in filter_range.index]
            filter_range.index.name = "factor_metric"
            filter_range['industry'] = industry
            filter_range['factor_display_name'] = factor_list['factor_display_name']
            filter_range['effect_direction'] = factor_list['effect_direction']
            filter_range['factor_category'] = factor_list['factor_category']
            filter_range['rank_tstat'] = filter_range.groupby('factor_display_name')[tstat_name].rank(ascending=False)
            filter_range = filter_range[filter_range['rank_tstat'] == 1]
            filter_range['rank_mean'] = filter_range.groupby('factor_display_name')[mean_name].rank(ascending=False)
            filter_range = filter_range[filter_range['rank_mean'] == 1]
            factor_selected_list.append(filter_range)
    factor_selected = pd.concat(factor_selected_list)
    factor_selected.to_csv(os.path.join(selected_factor_path, formation_date.strftime("%Y-%m-%d") + ".csv"), encoding='UTF-8-sig')


def factor_selection(industry_level, horizon, threshold, start, end):
    formation_dates = pd.date_range(start, end, freq='1M')
    if horizon == "qtr":
        formation_dates = [formation_date for formation_date in formation_dates if formation_date.month in [1, 4, 7, 10]]
    elif horizon == "semiyear":
        formation_dates = [formation_date for formation_date in formation_dates if formation_date.month in [4, 10]]
    elif horizon == "annyear":
        formation_dates = [formation_date for formation_date in formation_dates if formation_date.month == 4]

    pool = multiprocessing.Pool()
    for formation_date in formation_dates:
        pool.apply_async(factor_selection_helper, args=(industry_level, formation_date, horizon, threshold,))
        print('Sub-processes begins!')
    pool.close()
    pool.join()
    print('Sub-processes done!')


if __name__ == "__main__":
    factor_metric = "RNDOE"
    sfp_formation("SW2", '2010-02-28', '2020-12-31')
    sfp_analysis("SW2", '2013-04-30', '2020-12-31')
    # horizon = "semiyear"
    horizon = "qtr"
    # horizon = "annyear"
    # ann_or_fq = "ann"
    threshold = 2.0
    factor_selection("SW2", horizon, threshold, '2013-04-30', '2020-12-31')