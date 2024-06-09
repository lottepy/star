'''


'''
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from constants import *
from algorithm.utils.theme_backtest import theme_backtest
from algorithm.utils.helpers import cal_rebalancing_dates
# from algorithm.portfolio_formation.portfolio_formation import portfolio_formation
from algorithm.utils.result_analysis import result_metrics_calculation
from algorithm.utils.cul_turnover import cul_turnover
import itertools
from os.path import dirname, abspath, join, exists
from cvxopt import solvers
import cvxopt
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings("ignore")

def portfolio_sector_analysis(portfolio_name):
    underlying_type = PARAMETER_ALGO[portfolio_name]['underlying_type']
    rebalance_freq = 'MONTHLY'
    start = datetime.strptime(BACKTEST_SE[underlying_type]['start'], '%Y-%m-%d')
    end = datetime.strptime(BACKTEST_SE[underlying_type]['end'], '%Y-%m-%d')
    rebalancing_dates = cal_rebalancing_dates(start, end, rebalance_freq)
    criteria=PARAMETER_ALGO[portfolio_name]['criteria']
    if underlying_type == "US_STOCK":
        input_path = os.path.join(addpath.data_path, "us_data")
    elif underlying_type == "HK_STOCK":
        input_path = os.path.join(addpath.data_path, "hk_data")
    elif underlying_type == "CN_STOCK":
        input_path = os.path.join(addpath.data_path, "cn_data")
    portfolio_path = os.path.join(addpath.data_path, "strategy_temps", portfolio_name, "portfolios")
    if os.path.exists(portfolio_path):
        pass
    else:
        os.makedirs(portfolio_path)
    factor_path = os.path.join(input_path, 'CS_Factors')
    portfolio_path=os.path.join(addpath.data_path,"strategy_temps",portfolio_name,"portfolios")
    industry_data_df=pd.DataFrame(index=rebalancing_dates)
    data_dict = {}
    for factor in criteria.keys():
        data_dict[factor] = {}
        data_dict[factor]['portfolio'] = {}
        data_dict[factor]['mkt'] = {}


    for date in rebalancing_dates:
        portfolio=pd.read_csv(os.path.join(portfolio_path, date.strftime("%Y-%m-%d") + ".csv"),index_col=0)
        data_mkt = pd.read_csv(os.path.join(factor_path, date.strftime("%Y-%m-%d") + ".csv"), index_col=0)
        for ind in portfolio['industry']:
            industry_data_df.loc[date,ind]=portfolio[portfolio['industry']==ind]['weight'].sum()
        for factor in criteria.keys():
            data_dict[factor]['portfolio'][date]=np.sum(portfolio[factor]*portfolio['weight'])
            data_dict[factor]['mkt'][date]=np.sum(data_mkt[factor]*data_mkt['MARKET_CAP']/data_mkt['MARKET_CAP'].sum())
    for factor in criteria.keys():
        data_df = pd.DataFrame(data_dict[factor])
        data_df.plot(title=factor)
        plt.legend(loc='best')
        plt.show()
    industry_data_df = industry_data_df[industry_data_df.columns[industry_data_df.isna().sum().rank(ascending=True)<6]].fillna(0)
    industry_data_df.plot()
    plt.legend(loc='best')
    plt.show()

def sector_factors_analysis(underlying_type):
    if underlying_type == "US_STOCK":
        input_path = os.path.join(addpath.data_path, "us_data")
    elif underlying_type == "HK_STOCK":
        input_path = os.path.join(addpath.data_path, "hk_data")
    elif underlying_type == "CN_STOCK":
        input_path = os.path.join(addpath.data_path, "cn_data")
    factor_path=os.path.join(input_path,"CS_Factors")
    start='2009-12-31'
    end='2020-10-31'
    start=datetime.strptime(start, "%Y-%m-%d")
    end=datetime.strptime(end, "%Y-%m-%d")
    port_form_freq='MONTHLY'
    rebalancing_dates = cal_rebalancing_dates(start, end, port_form_freq)
    industry_mapping=pd.read_csv(os.path.join(addpath.data_path,"industry_mapping.csv"),index_col=0)
    factors_ind_dict={}
    if underlying_type == "CN_STOCK":
        symbol_list=pd.read_csv(os.path.join(input_path,"symbol_industry_all.csv"),index_col=0,dtype=object)
        symbol_list['SW1_NAME'] = industry_mapping.loc[symbol_list['SW1'], 'industry_name'].tolist()

    for date in rebalancing_dates:
        print(date)
        if underlying_type == "CN_STOCK":
            factors = pd.read_csv(os.path.join(factor_path, date.strftime("%Y-%m-%d") + ".csv"), index_col=0)
            factors['industry']=symbol_list.loc[factors.index,'SW1_NAME']
            factors=factors.drop(columns=['effective_date','ANNOUNCEMENT_DT', 'datadate'])
        elif underlying_type == "US_STOCK":
            factors = pd.read_csv(os.path.join(factor_path, date.strftime("%Y-%m-%d") + ".csv"), index_col=0).dropna(
                subset=['GICS_SECTOR'])
            factors['industry']=[str(int(ind)) for ind in factors['GICS_SECTOR']]
            factors['industry']=industry_mapping.loc[factors['industry'], 'industry_name'].tolist()
            factors=factors.drop(columns=['date','rp','tic','Ticker','ANNOUNCEMENT_DT', 'datadate'])
        factor_list=factors.columns.tolist()
        factor_list.remove('industry')
        for factor in factor_list:
            print(factor)
            if factor not in list(factors_ind_dict.keys()):
                factors_ind_dict[factor]={}

            ind_list=list(set(factors['industry'].tolist()))
            if np.nan in ind_list:
                ind_list.remove(np.nan)
            for ind in ind_list:
                if ind not in list(factors_ind_dict[factor].keys()):
                    factors_ind_dict[factor][ind]={}
                if 'ALL_MARKET' not in list(factors_ind_dict[factor].keys()):
                    factors_ind_dict[factor]['ALL_MARKET'] = {}
                factors_ind_dict[factor][ind][date]=sum(factors[factors['industry']==ind][factor]*factors[factors['industry']==ind]['MARKET_CAP'])\
                                                    /factors[factors['industry']==ind]['MARKET_CAP'].sum()
            factors_ind_dict[factor]['ALL_MARKET'][date]=sum(factors[factor]*factors['MARKET_CAP'])/factors['MARKET_CAP'].sum()

    research_data_path=os.path.join(addpath.data_path,'research_data')
    if os.path.exists(research_data_path):
        pass
    else:
        os.makedirs(research_data_path)
    writer=pd.ExcelWriter(os.path.join(research_data_path,underlying_type+"industry_factors.xlsx"))
    for factor in factors_ind_dict.keys():
        data_df = pd.DataFrame(factors_ind_dict[factor])
        if len(factor) > 31:
            factor=factor[-31:]
        data_df.to_excel(writer,sheet_name=factor,encoding='utf-8_sig')
        print(factor,'data has been written')
        # data_df.plot(title=factor)
        # plt.legend(loc='best')
        # plt.show()
    writer.save()








def portfolio_formation_for_backtest(portfolio_name):
    criteria=PARAMETER_ALGO[portfolio_name]['criteria']
    start=datetime(PARAMETER_ALGO[portfolio_name]['start_year'],
                PARAMETER_ALGO[portfolio_name]['start_month'],PARAMETER_ALGO[portfolio_name]['start_day'])
    end=datetime(PARAMETER_ALGO[portfolio_name]['end_year'],
                PARAMETER_ALGO[portfolio_name]['end_month'],PARAMETER_ALGO[portfolio_name]['end_day'])
    underlying_type=PARAMETER_ALGO[portfolio_name]['underlying_type']
    port_form_freq=PARAMETER_ALGO[portfolio_name]['port_form_freq']
    lookback_days=PARAMETER_ALGO[portfolio_name]['lookback_days']
    base_ccy=PARAMETER_ALGO[portfolio_name]['base_ccy']
    cash=PARAMETER_ALGO[portfolio_name]['ini_cash']
    ind_class=PARAMETER_ALGO[portfolio_name]['ind_class']
    number_assets=PARAMETER_ALGO[portfolio_name]['number_assets']
    num_top_ind=PARAMETER_ALGO[portfolio_name]['num_top_ind']
    weighting_approach=PARAMETER_ALGO[portfolio_name]['weighting_approach']
    lower_bound=PARAMETER_ALGO[portfolio_name]['lower_bound']
    upper_bound=PARAMETER_ALGO[portfolio_name]['upper_bound']
    rebalancing_dates = cal_rebalancing_dates(start, end, port_form_freq)

    if underlying_type == "US_STOCK":
        input_path = os.path.join(addpath.data_path, "us_data")
    elif underlying_type == "HK_STOCK":
        input_path = os.path.join(addpath.data_path, "hk_data")
    elif underlying_type == "CN_STOCK":
        input_path = os.path.join(addpath.data_path, "cn_data")
    portfolio_path = os.path.join(addpath.data_path, "strategy_temps", portfolio_name, "portfolios")
    if os.path.exists(portfolio_path):
        pass
    else:
        os.makedirs(portfolio_path)
    if portfolio_name =='US_5G':
        symbol_industry=pd.read_csv(os.path.join(input_path,"symbol_industry_5G.csv"),index_col=0)
    else:
        symbol_industry=pd.read_csv(os.path.join(input_path,"symbol_list.csv"),index_col=0)

    investment_univ_path = os.path.join(input_path, "investment_univ")
    factor_path = os.path.join(input_path, 'CS_Factors')
    num_universe_df=pd.DataFrame(index=rebalancing_dates,columns=['num_universe','num_selected','backup_list'])
    for date in rebalancing_dates:
        print(date)
        investment_univ = pd.read_csv(os.path.join(investment_univ_path, date.strftime("%Y-%m-%d") + ".csv"))[
            'symbol'].tolist()
        num_universe=len(investment_univ)
        factors=pd.read_csv(os.path.join(factor_path, date.strftime("%Y-%m-%d") + ".csv"),index_col=0)
        portfolio,num_selected,backup_list = portfolio_formation(portfolio_name, number_assets,symbol_industry, weighting_approach,
                                    date,investment_univ,factors,cash, upper_bound, lower_bound,criteria,ind_class,num_top_ind)
        num_universe_df.loc[date,'num_universe']=num_universe
        num_universe_df.loc[date, 'num_selected'] = num_selected
        num_universe_df.loc[date, 'backup_list'] = backup_list

        portfolio.to_csv(os.path.join(portfolio_path, date.strftime("%Y-%m-%d") + ".csv"))
    num_universe_df.to_csv(os.path.join(portfolio_path, "num_universe.csv"))


def portfolio_formation(portfolio_name, number_assets, symbol_industry, weighting_approach,
                        date, investment_univ, factors, cash, upper_bound, lower_bound, criteria, ind_class,
                        num_top_ind):
    symbol_list = symbol_industry.index.tolist()
    investment_univ = list(set(investment_univ) & set(factors.index.tolist()) & set(symbol_list))
    white_list=symbol_industry[symbol_industry['white_list']==1].index.tolist()
    white_list=list(set(white_list)&set(investment_univ))
    investment_univ=[symbol for symbol in investment_univ if symbol not in white_list]

    last_period_factors = factors.loc[investment_univ, :]
    last_period_factors['LOTPRICE'] = symbol_industry.loc[investment_univ, 'LOTSIZE'] * last_period_factors['CLOSE']
    last_period_factors = last_period_factors[last_period_factors['LOTPRICE'] < cash * upper_bound]

    ###################################
    # factor evaluation and ranking #
    ##################################

    selection_tmp = last_period_factors.copy()[list(criteria.keys())]
    selection_tmp['num_na'] = selection_tmp.isnull().sum(axis=1)
    selection_tmp = selection_tmp[selection_tmp['num_na'] <= 2]
    selection_tmp = selection_tmp.drop(['num_na'], axis=1)

    for col in selection_tmp.columns:
        selection_tmp[col] = selection_tmp[col].clip(lower=selection_tmp[col].quantile(0.025),
                                                     upper=selection_tmp[col].quantile(0.975))
        selection_tmp[col] = selection_tmp[col].fillna(selection_tmp[col].mean())

    selection_tmp['industry'] = symbol_industry[ind_class]
    selection_tmp=selection_tmp.dropna(subset=['industry'])

    selection_tmp['zscore'] = 0
    for col in list(criteria.keys()):

        if criteria[col][0] == 'Positive':
            selection_tmp['col_rank'] = selection_tmp[col].rank(ascending=False)
        else:
            selection_tmp['col_rank'] = selection_tmp[col].rank(ascending=True)
        factor_mean_tmp = selection_tmp['col_rank'].mean()
        factor_std_tmp = selection_tmp['col_rank'].std()
        selection_tmp['zscore'] = selection_tmp['zscore'] + (
                selection_tmp['col_rank'] - factor_mean_tmp) / factor_std_tmp * criteria[col][1]

    selection_tmp['rank'] = selection_tmp['zscore'].rank(ascending=True)

    #initial weight for z-score weighting
    selection_tmp['zscore']=-selection_tmp['zscore']
    selection_tmp['ini_weight'] = selection_tmp['zscore']+1
    temp=selection_tmp['zscore'][selection_tmp['zscore']<0]
    selection_tmp['ini_weight'][selection_tmp['zscore']<0]=1/(1-temp)
    selection_tmp['ini_weight']=selection_tmp['ini_weight']/selection_tmp['ini_weight'].sum()

    rankings = selection_tmp
    rankings['MARKET_CAP'] = last_period_factors.loc[rankings.index, 'MARKET_CAP']
    rankings['CLOSE'] = last_period_factors.loc[rankings.index, 'CLOSE']
    rankings['LOTPRICE'] = last_period_factors.loc[rankings.index, 'LOTPRICE']
    rankings['min_cash']=rankings['LOTPRICE']
    rankings['min_cash'][rankings['min_cash']<cash*lower_bound]=cash*lower_bound
    rankings=rankings.dropna()
    #########################
    # portfolio weighting  #
    #########################

    num_selected = len(rankings)
    rankings_main=rankings

    rankings_main['rank']=rankings_main['rank'].rank(ascending=True)
    portfolio_symbol = rankings_main[rankings_main['rank'] <= number_assets]

    if portfolio_symbol['min_cash'].sum() > cash*0.9:
        portfolio_symbol = rankings_main[rankings_main['rank'] <= number_assets*1.5]
        portfolio_symbol['trade_off'] = rankings['LOTPRICE'].rank(ascending=True) * rankings['rank']
        portfolio_symbol['trade_off'] = portfolio_symbol['trade_off'].rank(ascending=True)
        portfolio_symbol = portfolio_symbol[portfolio_symbol['trade_off'] <= number_assets]
        print('the minimum cash needed:', portfolio_symbol['min_cash'].sum())

    if portfolio_symbol['min_cash'].sum() > cash * 0.9:
        portfolio_symbol = rankings_main[rankings_main['rank'] <= number_assets*2]
        portfolio_symbol['trade_off'] = rankings['LOTPRICE'].rank(ascending=True) * rankings['rank']
        portfolio_symbol['trade_off'] = portfolio_symbol['trade_off'].rank(ascending=True)
        portfolio_symbol = portfolio_symbol[portfolio_symbol['trade_off'] <= number_assets]
        print('the minimum cash needed:', portfolio_symbol['min_cash'].sum())

    if portfolio_symbol['min_cash'].sum() > cash * 0.9:
        portfolio_symbol = rankings_main[rankings_main['rank'] <= number_assets*2]
        portfolio_symbol['trade_off'] = (rankings['LOTPRICE'].rank(ascending=True)**1.2) * rankings['rank']
        portfolio_symbol['trade_off'] = portfolio_symbol['trade_off'].rank(ascending=True)
        portfolio_symbol = portfolio_symbol[portfolio_symbol['trade_off'] <= number_assets]
        print('the minimum cash needed:', portfolio_symbol['min_cash'].sum())

    lotprice = portfolio_symbol['LOTPRICE'].values.reshape(portfolio_symbol.shape[0], 1)
    lotprice = np.array(lotprice, dtype=np.float)

    if weighting_approach == "EQUALLY":
        portfolio_symbol['weight'] = 1 / portfolio_symbol.shape[0]
        # portfolio_symbol = portfolio_symbol.drop(columns=['rank', 'MARKET_CAP'])
        P = np.eye(portfolio_symbol.shape[0])
        q = -2 * np.array(portfolio_symbol['weight'].tolist()).reshape(portfolio_symbol.shape[0], 1)
    elif weighting_approach == "MARKET_CAP":
        portfolio_symbol['weight'] = portfolio_symbol['MARKET_CAP'] / sum(portfolio_symbol['MARKET_CAP'])
        # portfolio_symbol = portfolio_symbol.drop(columns=['rank', 'MARKET_CAP'])
        P = np.eye(portfolio_symbol.shape[0])
        q = -2 * np.array(portfolio_symbol['weight'].tolist()).reshape(portfolio_symbol.shape[0], 1)
    elif weighting_approach == 'Z_SCORE':
        portfolio_symbol['weight'] = portfolio_symbol['ini_weight']/ sum(portfolio_symbol['ini_weight'])
        # portfolio_symbol = portfolio_symbol.drop(columns=['rank', 'MARKET_CAP'])
        P = np.eye(portfolio_symbol.shape[0])
        q = -2 * np.array(portfolio_symbol['weight'].tolist()).reshape(portfolio_symbol.shape[0], 1)

    portfolio_symbol['float_bound']=portfolio_symbol['MARKET_CAP']/portfolio_symbol['MARKET_CAP'].sum()*0.1
    G = np.vstack([np.eye(portfolio_symbol.shape[0]), -np.eye(portfolio_symbol.shape[0]), \
                   -np.eye(portfolio_symbol.shape[0]) * cash])
    h = np.vstack([(np.ones(portfolio_symbol.shape[0])*upper_bound+portfolio_symbol['float_bound'].values).reshape(portfolio_symbol.shape[0], 1),
                   -(np.ones(portfolio_symbol.shape[0])*lower_bound+portfolio_symbol['float_bound'].values).reshape(portfolio_symbol.shape[0], 1),\
                   -lotprice])
    A = np.vstack([np.ones(portfolio_symbol.shape[0]).reshape(1, portfolio_symbol.shape[0])])
    b = np.vstack([np.ones(1).reshape(1, 1)])

    # if sum(cash * lower_bound - lotprice[lotprice < cash * lower_bound]) > cash - sum(lotprice)[0]:
    #     G = np.vstack([np.eye(portfolio_symbol.shape[0]), \
    #                    -np.eye(portfolio_symbol.shape[0]) * cash])
    #     h = np.vstack([np.ones(portfolio_symbol.shape[0]).reshape(portfolio_symbol.shape[0], 1) * upper_bound,
    #                    -lotprice])

    solvers.options['show_progress'] = False
    solvers.options['abstol'] = 1e-21
    optimized = solvers.qp(
        cvxopt.matrix(P),
        cvxopt.matrix(q),
        cvxopt.matrix(G),
        cvxopt.matrix(h),
        cvxopt.matrix(A),
        cvxopt.matrix(b)
    )

    portfolio_symbol['weight'] = optimized['x']
    backup=rankings.drop(portfolio_symbol.index.tolist())
    backup['rank']=backup['rank'].rank(ascending=True)
    print('股票组合名单：',portfolio_symbol)
    print('备选股票名单:',backup[backup['rank']<=number_assets].index.tolist())
    return portfolio_symbol, num_selected,backup[backup['rank']<=number_assets].index.tolist()


if __name__ == "__main__":
    portfolio_name='US_5G'
    task="backtesting"
    print(portfolio_name)
    portfolio_formation_for_backtest(portfolio_name)
    pv, report=theme_backtest(portfolio_name,task)


