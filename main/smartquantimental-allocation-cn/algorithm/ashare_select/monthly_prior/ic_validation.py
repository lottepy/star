from algorithm import addpath
import multiprocessing
import numpy as np
import pandas as pd
import os
from datetime import datetime, timedelta


def IC_helper(formation_date):
    factor_list_df = pd.read_csv(os.path.join(addpath.config_path, "ashare_trading_factor_list.csv"), index_col='factor_list')
    factor_list = factor_list_df.index.tolist()

    selected_stock_path = os.path.join(addpath.data_path, "Ashare", "selected_stocks", formation_date.strftime('%Y-%m-%d') + ".csv")
    selected_stocks = pd.read_csv(selected_stock_path)['Stkcd'].tolist()

    cs_factor_path = os.path.join(addpath.data_path, "Ashare", "monthly_prior", "cs_trading_factors")
    ic_path = os.path.join(addpath.data_path, "Ashare", "monthly_prior", "IC")
    if os.path.exists(ic_path):
        pass
    else:
        os.makedirs(ic_path)
    rebalance_start = datetime(formation_date.year - 1, formation_date.month, 1)
    rebalance_end = formation_date + timedelta(days=200)
    rebalance_dates = pd.date_range(rebalance_start, rebalance_end, freq='1M')
    ICs_list = []
    for rebalance_date in rebalance_dates:
        try:
            factors = pd.read_csv(os.path.join(cs_factor_path, rebalance_date.strftime('%Y-%m-%d') + '.csv'), index_col=0)
            fac = factor_list + ['ret_forward']
            factors = factors.loc[selected_stocks, fac]
            IC = factors.corr()
            IC = IC[['ret_forward']]
            IC = IC.rename(columns={'ret_forward': rebalance_date})
            ICs_list.append(IC)
        except:
            print("There's no data for " + rebalance_date.strftime('%Y-%m-%d'))
    ICs = pd.concat(ICs_list, axis=1)
    ICs = ICs.loc[factor_list, :]
    ICs = ICs.transpose()
    ICs.to_csv(os.path.join(ic_path, formation_date.strftime('%Y-%m-%d') + ".csv"))


def IC_formation():
    iu_path = os.path.join(addpath.data_path, "Ashare", "investment_universe")

    formation_dates = os.listdir(iu_path)
    formation_dates = [datetime.strptime(formation_date[:-4], "%Y-%m-%d") for formation_date in formation_dates]
    formation_dates_ann = list(set(datetime(date.year, 5, 1) for date in formation_dates))
    formation_dates = formation_dates + formation_dates_ann
    formation_dates.sort()

    pool = multiprocessing.Pool()
    for formation_date in formation_dates[15:-1]:
        # sfp_formation_helper(factor_metric, effect_direction)
        pool.apply_async(IC_helper, args=(formation_date, ))
        print('Sub-processes begins!')
    pool.close()
    pool.join()
    print('Sub-processes done!')


def trading_factor_selection_helper(rebalance_date):
    analysis_start = datetime(rebalance_date.year - 1, rebalance_date.month, 1)
    analysis_period = pd.date_range(analysis_start, freq='1M', periods=12)
    ic_path = os.path.join(addpath.data_path, "Ashare", "monthly_prior", "IC")
    selected_trading_factors_path = os.path.join(addpath.data_path, "Ashare", "monthly_prior", "selected_trading_factors")
    if 4 <= rebalance_date.month < 10:
        formation_date = datetime(rebalance_date.year, 4, 30)
    elif rebalance_date.month < 4:
        formation_date = datetime(rebalance_date.year - 1, 10, 31)
    elif rebalance_date.month >= 10:
        formation_date = datetime(rebalance_date.year, 10, 31)

    ICs = pd.read_csv(os.path.join(ic_path, formation_date.strftime('%Y-%m-%d') + ".csv"), index_col=0, parse_dates=[0])
    ICs = ICs.loc[analysis_period, :]
    ICs_mean = ICs.mean()
    IRs = ICs.mean() / (ICs.std() / (ICs.count()))
    analysis = pd.concat([ICs_mean, IRs], axis=1)
    analysis.columns = ['ICs_mean', 'IRs']
    analysis['ICs_mean_abs'] = analysis['ICs_mean'].abs()
    analysis['IRs_abs'] = analysis['IRs'].abs()
    analysis['IC_latest'] = ICs.iloc[-1, :]
    analysis['IC_latest_abs'] = analysis['IC_latest'].abs()
    analysis['IC_latest_IC_mean_interaction'] = analysis['IC_latest'] * analysis['ICs_mean']
    analysis = analysis[analysis['ICs_mean_abs'] >= 0.05]
    analysis = analysis[analysis['IRs_abs'] >= 4]
    analysis = analysis[analysis['IC_latest_IC_mean_interaction'] > 0]
    if analysis.shape[0] == 0:
        analysis = pd.concat([ICs_mean, IRs], axis=1)
        analysis.columns = ['ICs_mean', 'IRs']
        analysis['ICs_mean_abs'] = analysis['ICs_mean'].abs()
        analysis['IRs_abs'] = analysis['IRs'].abs()
        analysis['IC_latest'] = ICs.iloc[-1, :]
        analysis['IC_latest_abs'] = analysis['IC_latest'].abs()
        analysis['IC_latest_IC_mean_interaction'] = analysis['IC_latest'] * analysis['ICs_mean']
        analysis = analysis[analysis['ICs_mean_abs'] >= 0.05]
        analysis = analysis[analysis['IRs_abs'] >= 2.5]
        analysis = analysis[analysis['IC_latest_IC_mean_interaction'] > 0]
        if analysis.shape[0] == 0:
            analysis = pd.concat([ICs_mean, IRs], axis=1)
            analysis.columns = ['ICs_mean', 'IRs']
            analysis['ICs_mean_abs'] = analysis['ICs_mean'].abs()
            analysis['IRs_abs'] = analysis['IRs'].abs()
            analysis['IC_latest'] = ICs.iloc[-1, :]
            analysis['IC_latest_abs'] = analysis['IC_latest'].abs()
            analysis['IC_latest_IC_mean_interaction'] = analysis['IC_latest'] * analysis['ICs_mean']
            analysis = analysis[analysis['IC_latest_abs'] >= 0.05]
            analysis = analysis[analysis['IC_latest_IC_mean_interaction'] > 0]

    analysis.to_csv(os.path.join(selected_trading_factors_path, rebalance_date.strftime('%Y-%m-%d') + ".csv"))


def trading_factor_selection(start, end):
    rebalance_dates = pd.date_range(start, end, freq='1M')
    pool = multiprocessing.Pool()
    for rebalance_date in rebalance_dates:
        # trading_factor_selection_helper(rebalance_date)
        pool.apply_async(trading_factor_selection_helper, args=(rebalance_date, ))
        print('Sub-processes begins!')
    pool.close()
    pool.join()
    print('Sub-processes done!')


if __name__ == "__main__":
    # IC_formation()
    start = '2013-04-30'
    end = '2020-11-30'
    trading_factor_selection(start, end)
