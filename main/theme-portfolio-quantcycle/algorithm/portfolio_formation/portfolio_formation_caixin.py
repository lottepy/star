import numpy as np
import pandas as pd
import os
import datetime
from scipy.stats import skew
import calendar
import statsmodels.api as sm
from algorithm import addpath
from algorithm.portfolio_formation.investment_universe import investment_universe
from algorithm.portfolio_formation.factor_calculation import cal_factors
from algorithm.utils.helpers import cal_rebalancing_dates_backward
from algorithm.portfolio_formation.mf_ranking.multifactor_ranking import multifactor_ranking
from cvxopt import solvers
import cvxopt
import sys


def portfolio_formation_caixin(portfolio_name, number_assets, symbol_list, rebalance_freq, weighting_approach,
                               trading_data, financial_data, reference_data, formation_date, data_start, factors,
                               underlying_type, market_cap_threshold, upper_bound, lower_bound,para1,para2,para3,
                               factor1,factor2,factor3,j):
    investment_univ = investment_universe(symbol_list, trading_data, formation_date, underlying_type, market_cap_threshold)
    num_universe=len(investment_univ)

    #rebalancing_dates_bwd = cal_rebalancing_dates_backward(data_start, formation_date, rebalance_freq)
    last_period_factors_list = []
    for symbol in investment_univ:
        if factors[symbol].shape[0] > 0:
            tmp = pd.DataFrame(factors[symbol].iloc[-1, :]).transpose()
            last_period_factors_list.append(tmp)
    last_period_factors = pd.concat(last_period_factors_list)

    '''
	input_path = os.path.join(addpath.config_path, portfolio_name)
	sys.path.append(input_path)
	from theme_conditions import theme_conditions
    
	rankings = theme_conditions(last_period_factors, investment_univ,para1,para2,para3,factor1,factor2,factor3,j)
	
    criteria = {
        'MARKET_CAP': ['Positive', 0.99],
        'financial':{
            'GPRatio': ['Positive', 1],
            'EBTRatio': ['Positive', 1],
            'WoringQuality': ['Positive', 1],
            'EarningGrowth_exInv': ['Positive', 1],
            'ReturnofOperAsset': ['Positive', 1],
            'RevenueofOperAsset': ['Positive', 1],
            'ROA': ['Positive', 1],
            'ROE': ['Positive', 1],
            'rankpct':0.7
        },
        'risk':{
            'RealizedVol_12M': ['Negative', 1],
            'beta': ['Negative', 1],
            'ivol': ['Negative', 1],
            'semivar': ['Negative', 1],
            'rankpct':0.5
        }
    }
    '''
    criteria = {
        'MARKET_CAP': ['Positive', 0.99],
        'financial':{
            'CFOA': ['Positive', 1],
            'EARNVAR': ['Negative', 1],
            'ROA': ['Positive', 1],
            'ROE': ['Positive', 1],
            'rankpct':0.7
        },
        'risk':{
            'RealizedVol_12M': ['Negative', 1],
            'beta': ['Negative', 1],
            'ivol': ['Negative', 1],
            'semivar': ['Negative', 1],
            'rankpct':0.5
        }
    }

    selection_tmp = last_period_factors.copy()
    for criteriakey in list(criteria.keys())[1:]:

        selection_tmp[criteriakey + 'rank_pct']=0
        for rankkey in list(criteria[criteriakey].keys())[:-1]:
            # iqr=selection_tmp[rankkey].quantile(0.75)-selection_tmp[rankkey].quantile(0.25)
            # q_abnormal_L=selection_tmp[rankkey].quantile(0.25) - 1.5 * iqr
            # q_abnormal_U = selection_tmp[rankkey].quantile(0.75) + 1.5 * iqr
            # selection_tmp['date']=selection_tmp.index
            # selection_tmp.index=selection_tmp.loc[:,'symbol']
            # drop_index=selection_tmp[(selection_tmp[rankkey]<q_abnormal_L)|(selection_tmp[rankkey]>q_abnormal_U)].index
            # selection_tmp=selection_tmp.drop(index=drop_index)
            # selection_tmp.index=selection_tmp.loc[:,'date']
            if criteria[criteriakey][rankkey][0] == 'Positive':
                selection_tmp[rankkey+'rank_pct'] = selection_tmp[rankkey].rank(pct=True)
            else:
                # let all the factor effect be positive for further ranking
                selection_tmp[rankkey]=-selection_tmp[rankkey]
                selection_tmp[rankkey + 'rank_pct'] = selection_tmp[rankkey].rank(pct=True)
            selection_tmp[rankkey + 'rank_pct'].fillna(0,inplace=True)
            selection_tmp[criteriakey + 'rank_pct']=selection_tmp[criteriakey + 'rank_pct']+selection_tmp[rankkey + 'rank_pct']
        selection_tmp[criteriakey + 'rank_pct']=selection_tmp[criteriakey + 'rank_pct'].rank(pct=True)
        selection_tmp = selection_tmp[selection_tmp[criteriakey + 'rank_pct'] >= (1 - criteria[criteriakey]['rankpct'])]
        selection_tmp[criteriakey + 'rank'] = selection_tmp[criteriakey + 'rank_pct'].rank()

    rankings = selection_tmp.loc[:, ['symbol', 'MARKET_CAP',list(criteria.keys())[1] + 'rank_pct',list(criteria.keys())[2] \
                                     + 'rank_pct',list(criteria.keys())[2] + 'rank']]
    rankings = rankings.set_index('symbol')

    num_selected=len(rankings)
    portfolio_path = os.path.join(addpath.data_path, "strategy_temps", portfolio_name, "portfolios")

    portfolio_symbol = rankings[rankings[list(criteria.keys())[2] + 'rank'] > (num_selected-number_assets)]
    #portfolio_symbol.to_csv(os.path.join(portfolio_path, "factors" + formation_date.strftime("%Y-%m-%d") + ".csv"))

    if weighting_approach == "EQUALLY":
        portfolio_symbol['weight'] = 1 / portfolio_symbol.shape[0]
    # portfolio_symbol = portfolio_symbol.drop(columns=['rank', 'MARKET_CAP'])
    elif weighting_approach == "MARKET_CAP":
        portfolio_symbol['weight'] = portfolio_symbol['MARKET_CAP'] / portfolio_symbol['MARKET_CAP'].sum()
    # portfolio_symbol = portfolio_symbol.drop(columns=['rank', 'MARKET_CAP'])

    # P = np.eye(portfolio_symbol.shape[0])
    # q = -2 * np.array(portfolio_symbol['weight'].tolist()).reshape(portfolio_symbol.shape[0], 1)
    #
    # G = np.vstack([np.eye(portfolio_symbol.shape[0]), -np.eye(portfolio_symbol.shape[0])])
    # h = np.vstack([np.ones(portfolio_symbol.shape[0]).reshape(portfolio_symbol.shape[0], 1) * upper_bound,
    #                -np.ones(portfolio_symbol.shape[0]).reshape(portfolio_symbol.shape[0], 1) * lower_bound])
    # A = np.vstack([np.ones(portfolio_symbol.shape[0]).reshape(1, portfolio_symbol.shape[0])])
    # b = np.vstack([np.ones(1).reshape(1, 1)])
    #
    # solvers.options['show_progress'] = False
    # solvers.options['abstol'] = 1e-21
    # optimized = solvers.qp(
    #     cvxopt.matrix(P),
    #     cvxopt.matrix(q),
    #     cvxopt.matrix(G),
    #     cvxopt.matrix(h),
    #     cvxopt.matrix(A),
    #     cvxopt.matrix(b)
    # )
    #
    # portfolio_symbol['weight'] = optimized['x']

    return portfolio_symbol, num_universe, num_selected