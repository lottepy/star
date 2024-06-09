from algorithm import addpath
import multiprocessing
import numpy as np
import pandas as pd
import os
from datetime import datetime, timedelta


def Ashare_selection(num_stock_cap, formation_date):
    threshold = 0.75
    selected_stock_path = os.path.join(addpath.data_path, "Ashare", "selected_stocks")
    selected_factor_path = os.path.join(addpath.data_path, "Ashare", "selected_factor")
    factor_selected = pd.read_csv(os.path.join(selected_factor_path, formation_date.strftime("%Y-%m-%d") + ".csv"), index_col='industry')
    factor_abs_criteria = pd.read_csv(os.path.join(addpath.config_path, "ashare_factor_abs_criteria.csv"), index_col='factor_metric')

    symbol_list_path = os.path.join(addpath.config_path, "ashare_symbol_list.csv")
    symbol_list = pd.read_csv(symbol_list_path, index_col='Stkcd')
    industry_map = symbol_list[['AQM_Category']]
    industry_list = list(set(industry_map['AQM_Category'].tolist()))

    iu_path = os.path.join(addpath.data_path, "Ashare", "investment_universe")
    cs_factor_path = os.path.join(addpath.data_path, "Ashare", "cs_factors")

    iu = pd.read_csv(os.path.join(iu_path, formation_date.strftime('%Y-%m-%d') + '.csv'))['symbol'].tolist()
    factors = pd.read_csv(os.path.join(cs_factor_path, formation_date.strftime('%Y-%m-%d') + '.csv'), index_col=0)

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
            factor = factor[factor[factor_metric + '_rank'] >= threshold]
        selected_tmp = factor.reset_index()
        selected_tmp = selected_tmp[['Stkcd']]
        selected_tmp['industry'] = industry
        first_round_rst_list.append(selected_tmp)
    first_round_rst = pd.concat(first_round_rst_list)
    fr_stk_list = first_round_rst['Stkcd'].to_list()

    factor = factors.loc[fr_stk_list, factor_abs_criteria.index]
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
    selected = selected[['Stkcd', 'profitability_TTM']]
    selected = selected.set_index('Stkcd')
    selected.index.name = 'Stkcd'

    selected['SW1'] = symbol_list['SW1']
    selected['SW2'] = symbol_list['SW2']
    selected['SW3'] = symbol_list['SW3']
    selected['SW_Detailed'] = symbol_list['SW_Detailed']
    selected['AQM_Category'] = symbol_list['AQM_Category']
    selected['rank'] = selected.profitability_TTM.rank(ascending=False)
    selected = selected[selected['rank'] <= 200]

    selected.to_csv(os.path.join(selected_stock_path, formation_date.strftime("%Y-%m-%d") + ".csv"), encoding='UTF-8-sig')


if __name__ == "__main__":
    iu_path = os.path.join(addpath.data_path, "Ashare", "investment_universe")
    formation_dates = os.listdir(iu_path)
    formation_dates = [datetime.strptime(formation_date[:-4], "%Y-%m-%d") for formation_date in formation_dates]

    num_stock_cap = 200

    pool = multiprocessing.Pool()
    for formation_date in formation_dates[15:]:
        pool.apply_async(Ashare_selection, args=(num_stock_cap, formation_date,))
        print('Sub-processes begins!')
    pool.close()  # 关闭进程池，表示不能再往进程池中添加进程，需要在join之前调用
    pool.join()  # 等待进程池中的所有进程执行完毕
    print("Sub-process(es) done.")
