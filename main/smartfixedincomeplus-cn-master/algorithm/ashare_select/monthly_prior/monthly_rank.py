from algorithm import addpath
import multiprocessing
import numpy as np
import pandas as pd
import os
from datetime import datetime, timedelta


def ashare_monthly_rank_helper(rebalance_date, horizon, industry_level):
    rank_path = os.path.join(addpath.data_path, "Ashare", "monthly_prior", "monthly_stock_rank", industry_level, horizon)
    if os.path.exists(rank_path):
        pass
    else:
        os.makedirs(rank_path)
    if 4 <= rebalance_date.month < 10:
        formation_date = datetime(rebalance_date.year, 4, 30)
    elif rebalance_date.month < 4:
        formation_date = datetime(rebalance_date.year - 1, 10, 31)
    elif rebalance_date.month >= 10:
        formation_date = datetime(rebalance_date.year, 10, 31)
    selected_stocks_path = os.path.join(addpath.data_path, "Ashare", "selected_stocks", industry_level, horizon)
    stock_list = pd.read_csv(os.path.join(selected_stocks_path, formation_date.strftime('%Y-%m-%d') + ".csv"),)['Stkcd'].tolist()
    selected_trading_factors_path = os.path.join(addpath.data_path, "Ashare", "monthly_prior", "selected_trading_factors", industry_level, horizon)
    analysis = pd.read_csv(os.path.join(selected_trading_factors_path, rebalance_date.strftime('%Y-%m-%d') + ".csv"), index_col=0)
    cs_factor_path = os.path.join(addpath.data_path, "Ashare", "monthly_prior", "cs_trading_factors")
    factors = pd.read_csv(os.path.join(cs_factor_path, rebalance_date.strftime('%Y-%m-%d') + '.csv'), index_col=0)
    factors = factors.loc[stock_list, analysis.index.tolist()]
    ranks = pd.DataFrame(index=factors.index)
    for fac in analysis.index:
        if analysis.loc[fac, 'ICs_mean'] > 0:
            ranks[fac + '_rank'] = factors[fac].rank(ascending=True)
        elif analysis.loc[fac, 'ICs_mean'] < 0:
            ranks[fac + '_rank'] = factors[fac].rank(ascending=False)
    ranks['score'] = ranks.apply(lambda x: x.sum(), axis=1)
    ranks.to_csv(os.path.join(rank_path, rebalance_date.strftime('%Y-%m-%d') + ".csv"))


def ashare_monthly_rank(start, end, horizon, industry_level):
    rebalance_dates = pd.date_range(start, end, freq='1M')
    pool = multiprocessing.Pool()
    for rebalance_date in rebalance_dates:
        # trading_factor_selection_helper(rebalance_date)
        pool.apply_async(ashare_monthly_rank_helper, args=(rebalance_date, horizon, industry_level, ))
        print('Sub-processes begins!')
    pool.close()
    pool.join()
    print('Sub-processes done!')


if __name__ == "__main__":
    start = '2013-04-30'
    end = '2020-11-30'
    horizon = "qtr"
    # horizon = "semiyear"
    # horizon = "FF"
    industry_level = "SW2"
    ashare_monthly_rank(start, end, horizon, industry_level)