from algorithm import addpath
import multiprocessing
import numpy as np
import pandas as pd
import os
from datetime import datetime, timedelta


def Ashare_selection(num_stock_cap, formation_date):
    selected_stock_path = os.path.join(addpath.data_path, "Ashare", "selected_stocks", "FF")
    factor_selected = pd.read_csv(os.path.join(addpath.config_path, "ashare_factor_selected.csv"), index_col='industry')
    factor_abs_criteria = pd.read_csv(os.path.join(addpath.config_path, "ashare_factor_abs_criteria.csv"), index_col='factor_metric')

    symbol_list_path = os.path.join(addpath.config_path, "ashare_symbol_list.csv")
    symbol_list = pd.read_csv(symbol_list_path, index_col='Stkcd')
    industry_map = symbol_list[['AQM_Category']]
    industry_list = list(set(industry_map['AQM_Category'].tolist()))

    iu_path = os.path.join(addpath.data_path, "Ashare", "investment_universe")
    cs_factor_path = os.path.join(addpath.data_path, "Ashare", "cs_factors")

    iu = pd.read_csv(os.path.join(iu_path, formation_date.strftime('%Y-%m-%d') + '.csv'))['symbol'].tolist()
    factors = pd.read_csv(os.path.join(cs_factor_path, formation_date.strftime('%Y-%m-%d') + '.csv'), index_col=0)
    factors = factors.replace(np.inf, np.nan)
    facs = factors.columns.tolist()
    facs.remove('AQM_Category')
    for fac in facs:
        factors[fac] = factors[fac].fillna(factors[fac].mean())

    first_round_rst_list = []
    for industry in industry_list:
        industry_map_tmp = industry_map[industry_map['AQM_Category'] == industry]
        iu_tmp = [s for s in iu if s in factors.index and s in industry_map_tmp.index]
        factor_selected_tmp = factor_selected[factor_selected.index == industry]
        factor_selected_tmp = factor_selected_tmp.set_index('factor_metric')
        factor = factors.loc[iu_tmp, factor_selected_tmp.index]
        factor.index.name = 'Stkcd'
        for factor_metric in factor_selected_tmp.index:
            if factor_selected_tmp.loc[factor_metric, 'effect_direction'] == 'Positive':
                factor[factor_metric + '_rank'] = factor[factor_metric].rank(pct=True, ascending=True)
            else:
                factor[factor_metric + '_rank'] = factor[factor_metric].rank(pct=True, ascending=False)
        for factor_metric in factor_selected_tmp.index:
            threshold = factor_selected_tmp.loc[factor_metric, 'threshold']
            factor = factor[factor[factor_metric + '_rank'] >= threshold]
        selected_tmp = factor.reset_index()
        selected_tmp = selected_tmp[['Stkcd']]
        selected_tmp['industry'] = industry
        first_round_rst_list.append(selected_tmp)
    first_round_rst = pd.concat(first_round_rst_list)
    fr_stk_list = first_round_rst['Stkcd'].to_list()

    factor = factors.loc[fr_stk_list, factor_abs_criteria.index.tolist() + ['discrete_accruals', 'unlock_proportion', 'EPTTM']]
    factor.index.name = 'Stkcd'

    factor['AQM_Category'] = symbol_list['AQM_Category']
    factor['rank'] = factor.groupby('AQM_Category').profitability_TTM.rank(ascending=False)
    factor = factor[factor['rank'] <= 8]

    for factor_metric in factor_abs_criteria.index:
        if factor_abs_criteria.loc[factor_metric, 'effect_direction'] == 'Positive':
            factor = factor[factor[factor_metric] >= factor_abs_criteria.loc[factor_metric, 'threshold']]
        else:
            factor = factor[factor[factor_metric] <= factor_abs_criteria.loc[factor_metric, 'threshold']]

    selected = factor.reset_index()
    selected = selected[['Stkcd', 'profitability_TTM', 'profitability_growth_TTM', 'discrete_accruals', 'EPTTM']]
    selected = selected.set_index('Stkcd')
    selected.index.name = 'Stkcd'

    selected['SW1'] = symbol_list['SW1']
    selected['SW2'] = symbol_list['SW2']
    selected['SW3'] = symbol_list['SW3']
    selected['SW_Detailed'] = symbol_list['SW_Detailed']
    selected['AQM_Category'] = symbol_list['AQM_Category']
    selected['profitability_TTM_zsocre'] = (selected['profitability_TTM'] - selected['profitability_TTM'].mean()) / selected['profitability_TTM'].std()
    selected['profitability_growth_TTM_zsocre'] = (selected['profitability_growth_TTM'] - selected['profitability_growth_TTM'].mean()) / selected['profitability_growth_TTM'].std()
    selected['discrete_accruals_zsocre'] = (selected['discrete_accruals'] - selected['discrete_accruals'].mean()) / selected['discrete_accruals'].std()
    selected['EPTTM_zsocre'] = (selected['EPTTM'] - selected['EPTTM'].mean()) / selected['EPTTM'].std()
    selected['score'] = selected['profitability_TTM_zsocre'] * 0.6 + selected['profitability_growth_TTM_zsocre'] * 0.2 - selected['discrete_accruals_zsocre'] * 0.05 + selected['EPTTM_zsocre'] * 0.15
    selected['rank'] = selected.score.rank(ascending=False)
    selected = selected[selected['rank'] <= num_stock_cap]

    selected.to_csv(os.path.join(selected_stock_path, formation_date.strftime("%Y-%m-%d") + ".csv"), encoding='UTF-8-sig')


def Ashare_selection_rw(industry_level, num_stock_cap, horizon, formation_date, threshold):
    selected_stock_path = os.path.join(addpath.data_path, "Ashare", "selected_stocks", industry_level, horizon)
    if os.path.exists(selected_stock_path):
        pass
    else:
        os.makedirs(selected_stock_path)

    factor_selected = pd.read_csv(os.path.join(addpath.data_path, "Ashare", "selected_factor", industry_level, horizon, formation_date.strftime("%Y-%m-%d") + ".csv"), index_col='industry')
    factor_abs_criteria = pd.read_csv(os.path.join(addpath.config_path, "ashare_factor_abs_criteria.csv"), index_col='factor_metric')

    symbol_list_path = os.path.join(addpath.config_path, "ashare_symbol_list.csv")
    symbol_list = pd.read_csv(symbol_list_path, index_col='Stkcd')
    industry_map = symbol_list[[industry_level]]
    industry_list = list(set(industry_map[industry_level].tolist()))
    industry_list = [industry for industry in industry_list if industry in factor_selected.index]

    iu_path = os.path.join(addpath.data_path, "Ashare", "investment_universe")
    cs_factor_path = os.path.join(addpath.data_path, "Ashare", "cs_factors")

    iu = pd.read_csv(os.path.join(iu_path, formation_date.strftime('%Y-%m-%d') + '.csv'))['symbol'].tolist()
    factors = pd.read_csv(os.path.join(cs_factor_path, formation_date.strftime('%Y-%m-%d') + '.csv'), index_col=0)
    factors = factors.replace(np.inf, np.nan)
    facs = factors.columns.tolist()
    facs.remove('AQM_Category')
    for fac in facs:
        factors[fac] = factors[fac].fillna(factors[fac].mean())

    first_round_rst_list = []
    for industry in industry_list:
        industry_map_tmp = industry_map[industry_map[industry_level] == industry]
        iu_tmp = [s for s in iu if s in factors.index and s in industry_map_tmp.index]
        factor_selected_tmp = factor_selected[factor_selected.index == industry]
        factor_selected_tmp = factor_selected_tmp.set_index('factor_metric')
        factor = factors.loc[iu_tmp, factor_selected_tmp.index]
        factor.index.name = 'Stkcd'
        for factor_metric in factor_selected_tmp.index:
            if factor_selected_tmp.loc[factor_metric, 'effect_direction'] == 'Positive':
                factor[factor_metric + '_rank'] = factor[factor_metric].rank(pct=True, ascending=True)
            else:
                factor[factor_metric + '_rank'] = factor[factor_metric].rank(pct=True, ascending=False)
        factor['score'] = 0
        for factor_metric in factor_selected_tmp.index:
            factor.loc[factor[factor[factor_metric + '_rank'] >= threshold].index, 'score'] = factor.loc[factor[factor[factor_metric + '_rank'] >= threshold].index, 'score'] + 1
        factor['intraind_rank'] = factor['score'].rank(ascending=False)
        factor = factor[factor['intraind_rank'] <= 10]
        selected_tmp = factor.reset_index()
        selected_tmp = selected_tmp[['Stkcd']]
        selected_tmp['industry'] = industry
        first_round_rst_list.append(selected_tmp)
    first_round_rst = pd.concat(first_round_rst_list)
    fr_stk_list = first_round_rst['Stkcd'].to_list()

    factor = factors.loc[fr_stk_list, factor_abs_criteria.index.tolist() + ['discrete_accruals', 'unlock_proportion', 'EPTTM', 'RND']]
    factor.index.name = 'Stkcd'

    factor[industry_level] = symbol_list[industry_level]
    factor['rank'] = factor.groupby(industry_level).profitability_TTM.rank(ascending=False)
    factor = factor[factor['rank'] <= 8]

    for factor_metric in factor_abs_criteria.index:
        if factor_abs_criteria.loc[factor_metric, 'effect_direction'] == 'Positive':
            factor = factor[factor[factor_metric] >= factor_abs_criteria.loc[factor_metric, 'threshold']]
        else:
            factor = factor[factor[factor_metric] <= factor_abs_criteria.loc[factor_metric, 'threshold']]

    selected = factor.reset_index()
    selected = selected[['Stkcd', 'profitability_TTM', 'profitability_growth_TTM', 'discrete_accruals', 'EPTTM', 'RND']]
    selected = selected.set_index('Stkcd')
    selected.index.name = 'Stkcd'

    selected['SW1'] = symbol_list['SW1']
    selected['SW2'] = symbol_list['SW2']
    selected['SW3'] = symbol_list['SW3']
    selected['SW_Detailed'] = symbol_list['SW_Detailed']
    selected['AQM_Category'] = symbol_list['AQM_Category']
    selected['profitability_TTM_zsocre'] = (selected['profitability_TTM'] - selected['profitability_TTM'].mean()) / selected['profitability_TTM'].std()
    selected['profitability_growth_TTM_zsocre'] = (selected['profitability_growth_TTM'] - selected['profitability_growth_TTM'].mean()) / selected['profitability_growth_TTM'].std()
    selected['discrete_accruals_zsocre'] = (selected['discrete_accruals'] - selected['discrete_accruals'].mean()) / selected['discrete_accruals'].std()
    selected['EPTTM_zsocre'] = (selected['EPTTM'] - selected['EPTTM'].mean()) / selected['EPTTM'].std()
    selected['RND_zsocre'] = ((selected['RND'] - selected['RND'].mean()) / selected['RND'].std()).fillna(0)
    selected['score'] = selected['profitability_TTM_zsocre'] * 0.6 + selected['profitability_growth_TTM_zsocre'] * 0.15 - selected['discrete_accruals_zsocre'] * 0.05 + selected['EPTTM_zsocre'] * 0.15 + selected['RND_zsocre'] * 0.05
    selected['rank'] = selected.score.rank(ascending=False)
    selected = selected[selected['rank'] <= num_stock_cap]

    selected.to_csv(os.path.join(selected_stock_path, formation_date.strftime("%Y-%m-%d") + ".csv"), encoding='UTF-8-sig')


if __name__ == "__main__":
    num_stock_cap = 100
    threshold = 0.7
    horizon = "qtr"
    # horizon = "semiyear"
    start = '2013-04-30'
    end = '2020-12-31'

    formation_dates = pd.date_range(start, end, freq='1M')
    if horizon == "qtr":
        formation_dates = [formation_date for formation_date in formation_dates if
                           formation_date.month in [1, 4, 7, 10]]
    elif horizon == "semiyear":
        formation_dates = [formation_date for formation_date in formation_dates if formation_date.month in [4, 10]]
    elif horizon == "annyear":
        formation_dates = [formation_date for formation_date in formation_dates if formation_date.month == 4]

    pool = multiprocessing.Pool()
    for formation_date in formation_dates:
        # pool.apply_async(Ashare_selection, args=(num_stock_cap, formation_date,))
        pool.apply_async(Ashare_selection_rw, args=("SW2", num_stock_cap, horizon, formation_date, threshold))
        print('Sub-processes begins!')
    pool.close()  # 关闭进程池，表示不能再往进程池中添加进程，需要在join之前调用
    pool.join()  # 等待进程池中的所有进程执行完毕
    print("Sub-process(es) done.")
