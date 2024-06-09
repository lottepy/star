from algorithm import addpath
import multiprocessing
import numpy as np
import pandas as pd
import os
from datetime import datetime, timedelta


def sfp_formation_helper(factor_metric, effect_direction):
    industry_map = pd.read_csv(os.path.join(addpath.config_path, "ashare_symbol_list.csv"), index_col='Stkcd')[
        ['AQM_Category']]
    industry_list = list(set(industry_map['AQM_Category'].tolist()))

    iu_path = os.path.join(addpath.data_path, "Ashare", "investment_universe")

    formation_dates = os.listdir(iu_path)
    formation_dates = [datetime.strptime(formation_date[:-4], "%Y-%m-%d") for formation_date in formation_dates]
    formation_dates_ann = list(set(datetime(date.year, 5, 1) for date in formation_dates))
    formation_dates = formation_dates + formation_dates_ann
    formation_dates.sort()

    cs_factor_path = os.path.join(addpath.data_path, "Ashare", "cs_factors")
    sfp_path = os.path.join(addpath.data_path, "Ashare", "single_factor_portfolios")
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
            industry_map_tmp = industry_map[industry_map['AQM_Category'] == industry]
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


def sfp_formation():
    factor_list = pd.read_csv(os.path.join(addpath.config_path, "ashare_factor_list.csv"), index_col='factor_list')
    pool = multiprocessing.Pool()
    for factor_metric in factor_list.index.tolist():
        effect_direction = factor_list.loc[factor_metric, 'effect_direction']
        # sfp_formation_helper(factor_metric, effect_direction)
        pool.apply_async(sfp_formation_helper, args=(factor_metric, effect_direction, ))
        print('Sub-processes begins!')
    pool.close()
    pool.join()
    print('Sub-processes done!')


def sfp_analysis_helper(formation_date, industry):
    sfp_analysis_path = os.path.join(addpath.data_path, "Ashare", "sfp_analysis", formation_date.strftime("%Y-%m-%d"))
    if os.path.exists(sfp_analysis_path):
        pass
    else:
        os.makedirs(sfp_analysis_path)
    sfp_path = os.path.join(addpath.data_path, "Ashare", "single_factor_portfolios")
    factor_list = pd.read_csv(os.path.join(addpath.config_path, "ashare_factor_list.csv"), index_col='factor_list')
    pr_analysis_list = []

    sample_period_start = formation_date - timedelta(days=1850)
    sample_period_end = formation_date - timedelta(days=100)

    for factor_metric in factor_list.index.tolist():
        try:
            portfolio_return = pd.read_csv(os.path.join(sfp_path, factor_metric + "_" + industry + ".csv"), index_col=0, parse_dates=[0])
            portfolio_return = portfolio_return[sample_period_start <= portfolio_return.index]
            portfolio_return = portfolio_return[portfolio_return.index < sample_period_end]
            dates_w_annreport = [d for d in portfolio_return.index if d.month != 5]
            dates_w_fqreport = [d for d in portfolio_return.index if d.month != 4]

            portfolio_return_w_annreport = portfolio_return.loc[dates_w_annreport, :]
            pr_mean_w_annreport = portfolio_return_w_annreport.mean()
            pr_std_w_annreport = portfolio_return_w_annreport.std()
            pr_w_annreport = pd.concat([pr_mean_w_annreport, pr_std_w_annreport], axis=1)
            pr_w_annreport.columns = ['pr_mean_w_annreport', 'pr_std_w_annreport']
            pr_w_annreport['pr_tstat_w_annreport'] = pr_w_annreport['pr_mean_w_annreport'] / (pr_w_annreport['pr_std_w_annreport'] / (pr_w_annreport.shape[0] ** 0.5))
            pr_w_annreport['pr_num_w_annreport'] = pr_w_annreport.shape[0]

            portfolio_return_w_fqreport = portfolio_return.loc[dates_w_fqreport, :]
            pr_mean_w_fqreport = portfolio_return_w_fqreport.mean()
            pr_std_w_fqreport = portfolio_return_w_fqreport.std()
            pr_w_fqreport = pd.concat([pr_mean_w_fqreport, pr_std_w_fqreport], axis=1)
            pr_w_fqreport.columns = ['pr_mean_w_fqreport', 'pr_std_w_fqreport']
            pr_w_fqreport['pr_tstat_w_fqreport'] = pr_w_fqreport['pr_mean_w_fqreport'] / (pr_w_fqreport['pr_std_w_fqreport'] / (pr_w_fqreport.shape[0] ** 0.5))
            pr_w_fqreport['pr_num_w_fqreport'] = pr_w_fqreport.shape[0]

            pr_analysis_tmp = pd.concat([pr_w_annreport, pr_w_fqreport], axis=1)
            pr_analysis_tmp.index.name = 'factor_horizon'
            pr_analysis_list.append(pr_analysis_tmp)
        except:
            print("No data for " + factor_metric  + " on " + formation_date.strftime('%Y-%m-%d') + "for " + industry)
    pr_analysis = pd.concat(pr_analysis_list)
    pr_analysis.to_csv(os.path.join(sfp_analysis_path, industry + ".csv"))


def sfp_analysis():
    industry_map = pd.read_csv(os.path.join(addpath.config_path, "ashare_symbol_list.csv"), index_col='Stkcd')[
        ['AQM_Category']]
    industry_list = list(set(industry_map['AQM_Category'].tolist()))

    iu_path = os.path.join(addpath.data_path, "Ashare", "investment_universe")
    formation_dates = os.listdir(iu_path)
    formation_dates = [datetime.strptime(formation_date[:-4], "%Y-%m-%d") for formation_date in formation_dates]
    formation_dates.sort()

    pool = multiprocessing.Pool()
    for formation_date in formation_dates:
        for industry in industry_list:
            # sfp_analysis_helper(formation_date, industry)
            pool.apply_async(sfp_analysis_helper, args=(formation_date, industry, ))
            print('Sub-processes begins!')
    pool.close()
    pool.join()
    print('Sub-processes done!')


def factor_selection_helper(formation_date, horizon, ann_or_fq, threshold):
    selected_factor_path = os.path.join(addpath.data_path, "Ashare", "selected_factor")
    if os.path.exists(selected_factor_path):
        pass
    else:
        os.makedirs(selected_factor_path)
    industry_map = pd.read_csv(os.path.join(addpath.config_path, "ashare_symbol_list.csv"), index_col='Stkcd')[
        ['AQM_Category']]
    industry_list = list(set(industry_map['AQM_Category'].tolist()))
    factor_list = pd.read_csv(os.path.join(addpath.config_path, "ashare_factor_list.csv"), index_col='factor_list')
    sfp_analysis_path = os.path.join(addpath.data_path, "Ashare", "sfp_analysis", formation_date.strftime("%Y-%m-%d"))

    factor_selected_list = []
    for industry in industry_list:
        pr_analysis = pd.read_csv(os.path.join(sfp_analysis_path, industry + ".csv"), index_col='factor_horizon')
        if horizon == "qtr":
            pr_analysis_index = [idx for idx in pr_analysis.index if idx[-15:] == "_difference_qtr"]
            surfix = "_difference_qtr"
        elif horizon == "semiyear":
            pr_analysis_index = [idx for idx in pr_analysis.index if idx[-20:] == "_difference_semiyear"]
            surfix = "_difference_semiyear"
        elif horizon == "annyear":
            pr_analysis_index = [idx for idx in pr_analysis.index if idx[-19:] == "_difference_annyear"]
            surfix = "_difference_annyear"
        if ann_or_fq == "ann":
            pr_analysis_column = [col for col in pr_analysis.columns if col[-9:] == "annreport"]
            mean_name = 'pr_mean_w_annreport'
            tstat_name = 'pr_tstat_w_annreport'
        elif ann_or_fq == "fq":
            pr_analysis_column = [col for col in pr_analysis.columns if col[-8:] == "fqreport"]
            mean_name = 'pr_mean_w_fqreport'
            tstat_name = 'pr_tstat_w_fqreport'
        filter_range = pr_analysis.loc[pr_analysis_index, pr_analysis_column]
        filter_range = filter_range[filter_range[tstat_name] >= threshold]
        if filter_range.shape[0] > 0:
            filter_range.index = [idx[:-len(surfix)] for idx in filter_range.index]
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


def factor_selection(horizon, ann_or_fq, threshold):
    iu_path = os.path.join(addpath.data_path, "Ashare", "investment_universe")
    formation_dates = os.listdir(iu_path)
    formation_dates = [datetime.strptime(formation_date[:-4], "%Y-%m-%d") for formation_date in formation_dates]
    formation_dates.sort()

    pool = multiprocessing.Pool()
    for formation_date in formation_dates[15:]:
        # factor_selection_helper(formation_date, horizon, ann_or_fq, threshold)
        pool.apply_async(factor_selection_helper, args=(formation_date, horizon, ann_or_fq, threshold,))
        print('Sub-processes begins!')
    pool.close()
    pool.join()
    print('Sub-processes done!')


if __name__ == "__main__":
    factor_metric = "RNDOE"
    sfp_formation()
    sfp_analysis()
    horizon = "semiyear"
    ann_or_fq = "ann"
    threshold = 2.0
    factor_selection(horizon, ann_or_fq, threshold)