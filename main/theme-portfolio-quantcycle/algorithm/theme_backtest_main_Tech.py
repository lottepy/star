'''


'''

import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pickle
from algorithm import addpath
from algorithm.utils.portfolio_config_loader import portfolio_config_loader
from algorithm.utils.data_downloader import dm_data_downloader
from algorithm.utils.helpers import cal_rebalancing_dates
from algorithm.portfolio_formation.portfolio_formation import portfolio_formation
from algorithm.portfolio_formation.factor_calculation_US import cal_factors
from algorithm.utils.backtest_engine_config_generator import create_backtest_engine_files
from algorithm.backtest_engine import MagnumBacktestEngine
from algorithm.backtest_engine.utils.get_logger import get_logger
from algorithm.backtest_engine.utils.result_processer import ResultProcesser
from algorithm.utils.result_analysis import result_metrics_calculation
from algorithm.utils.result_analysis import exreturn_metrics_calculation
from algorithm.utils.cul_turnover import cul_turnover
from constants import *
import statsmodels.api as sm
import matplotlib.pyplot as plt
import warnings
from quantcycle.utils.run_once import run_once
from quantcycle.app.result_exporter.result_reader import ResultReader
import itertools
import ast
from os.path import dirname, abspath, join, exists

warnings.filterwarnings("ignore")


def theme_backtest(portfolio_name,factor1,factor2,factor3,factor4, factor5, factor6):
    symbol_list,symbol_industry, start, end, cash, commission, lookback_days, rebalance_freq, base_ccy, underlying_type, \
    number_assets, weighting_approach, market_cap_threshold, upper_bound, lower_bound = portfolio_config_loader(portfolio_name)

    # portfolio_formation_for_backtest_periods(portfolio_name, lookback_days, symbol_list,symbol_industry, underlying_type, start, end,
    #                                          rebalance_freq, weighting_approach, number_assets, market_cap_threshold,
    #                                          cash,upper_bound, lower_bound,factor1,factor2,factor3,factor4, factor5, factor6)

    rebalance_freq='QUARTERLY'
    strategy_config_path,strategy_allocation=create_backtest_engine_files(portfolio_name, start, end, rebalance_freq, underlying_type, base_ccy, cash,
                                 commission)

    run_once(strategy_config_path, strategy_allocation)
    # inspect results
    reader = ResultReader(join(addpath.result_path, portfolio_name,portfolio_name))
    reader.to_csv(join(addpath.result_path,portfolio_name))

    strategy_id = list(reader.get_all_sids())
    pnl_results = reader.get_strategy(id_list=strategy_id, fields=['pnl'])

    pnl_list = []
    for key in pnl_results:
        tmp_pnl = pnl_results[key][0]
        tmp_pnl.index = pd.to_datetime(tmp_pnl.index, unit='s')
        tmp_pnl = (tmp_pnl + cash) / cash
        pnl_list.append(tmp_pnl)

    pv = pd.concat(pnl_list, axis=1)
    pv.columns = [portfolio_name]

    report = result_metrics_calculation(pv)

    report.to_csv(join(addpath.result_path,portfolio_name, 'performance metrics.csv'))
    # pv=pv.resample('1D').last().ffill()
    pv.to_csv(join(addpath.result_path,portfolio_name, 'portfolio value.csv'))
    cul_turnover(rebalance_freq,start, end,portfolio_name,underlying_type,pv,cash)
    return pv, report





def portfolio_formation_for_backtest_periods(portfolio_name, lookback_days, symbol_list,symbol_industry, underlying_type,\
                                             start, end,rebalance_freq, weighting_approach, number_assets, market_cap_threshold,
                                             cash,upper_bound, lower_bound,factor1,factor2,factor3,factor4, factor5, factor6):
    rebalancing_dates = cal_rebalancing_dates(start, end, rebalance_freq)

    if underlying_type == "US_STOCK":
        input_path = os.path.join(addpath.data_path, "us_data")
    elif underlying_type == "HK_STOCK":
        input_path = os.path.join(addpath.data_path, "hk_data")
    elif underlying_type == "CN_STOCK":
        input_path = os.path.join(addpath.data_path, "cn_data")

    trading_data_all = {}
    financial_data_all = {}
    reference_data_all = {}
    t_all_s=datetime.now()
    # for symbol in symbol_list:
    #     tmp_td_path = os.path.join(input_path, "trading", symbol + ".csv")
    #     tmp_fd_path = os.path.join(input_path, "financials", symbol + ".csv")
    #     # tmp_rd_path = os.path.join(input_path, "reference", symbol + ".csv")
    #     trading_data_all[symbol] = pd.read_csv(tmp_td_path, parse_dates=[0], index_col=0)
    #     financial_data_all[symbol] = pd.read_csv(tmp_fd_path, parse_dates=[0], index_col=0)
    #     trading_data_all[symbol]['EQY_SH_OUT'] = trading_data_all[symbol]['EQY_SH_OUT'].ffill()
    #     # reference_data_all[symbol] = pd.read_csv(tmp_rd_path, parse_dates=[0], index_col=0)
    market_index_path = os.path.join(input_path, "reference", "market_index.csv")
    reference_data_all["market_index"] = pd.read_csv(market_index_path, parse_dates=[0], index_col=0)
    t_all_e = datetime.now()
    print('data reading'+f'{t_all_e - t_all_s}')

    # factors_all = {}
    # for symbol in symbol_list:
    #     print(symbol)
    #     t_all_s = datetime.now()
    #     factors_all[symbol] = cal_factors(symbol, trading_data_all, financial_data_all, reference_data_all, rebalance_freq)
    #     factor_output = open(os.path.join(input_path, 'factor.pkl'), 'wb')
    #     pickle.dump(factors_all, factor_output)
    #     factor_output.close()
    #     t_all_e = datetime.now()
    #     print('factor calculating' + f'{t_all_e - t_all_s}')

    # pkl_file = open(os.path.join(input_path, 'factor.pkl'), 'rb')
    # factors_all = pickle.load(pkl_file)
    # pkl_file.close()

    portfolio_path = os.path.join(addpath.data_path, "strategy_temps", portfolio_name, "portfolios")
    if os.path.exists(portfolio_path):
        pass
    else:

        os.makedirs(portfolio_path)

    num_universe_list=pd.DataFrame(index=rebalancing_dates,columns=list(['num_universe','num_selected']))
    for date in rebalancing_dates:
        print(date)
        data_start = date - timedelta(days=lookback_days)
        data_end = date
        trading_data = {}
        financial_data = {}
        reference_data = {}
        factors = {}
        # investment_univ_path = os.path.join(input_path, "investment_univ")
        # investment_univ = pd.read_csv(os.path.join(investment_univ_path, date.strftime("%Y-%m-%d") + ".csv"))[
        #     'ticker'].to_list()

        # for symbol in investment_univ:
        #     trading_data[symbol] = trading_data_all[symbol][trading_data_all[symbol].index <= data_end]
        #     trading_data[symbol] = trading_data[symbol][trading_data[symbol].index >= data_start]
        #
        #     financial_data[symbol] = financial_data_all[symbol][financial_data_all[symbol].index <= data_end]
        #     financial_data[symbol] = financial_data[symbol][financial_data[symbol].index >= data_start - timedelta(days=90)]
        #
        #     factors[symbol] = factors_all[symbol][factors_all[symbol].index <= data_end]
        #     factors[symbol] = factors[symbol][factors[symbol].index >= data_start - timedelta(days=90)]
        #
        #
        #     # reference_data[symbol] = reference_data_all[symbol][reference_data_all[symbol].index <= data_end]
        #     # reference_data[symbol] = reference_data[symbol][reference_data[symbol].index >= data_start]
        factor_path=os.path.join(input_path,'CS_Factors_new')
        factors=pd.read_csv(os.path.join(factor_path, date.strftime("%Y-%m-%d") + ".csv"),index_col=0)

        reference_data["market_index"] = reference_data_all["market_index"][reference_data_all["market_index"].index <= data_end]
        reference_data["market_index"] = reference_data["market_index"][reference_data["market_index"].index >= data_start]

        portfolio,num_universe,num_selected = portfolio_formation(portfolio_name, number_assets, symbol_list,symbol_industry, rebalance_freq, weighting_approach,
                                        trading_data, financial_data, reference_data, date, data_start, factors,
                                        underlying_type, market_cap_threshold,cash, upper_bound, lower_bound,factor1, factor2, factor3,factor4, factor5, factor6)
        num_universe_list.loc[date,'num_universe']=num_universe
        num_universe_list.loc[date, 'num_selected'] = num_selected
        portfolio.to_csv(os.path.join(portfolio_path, date.strftime("%Y-%m-%d") + ".csv"))
        print(portfolio)
        lotprice_product=list(itertools.combinations(portfolio['LOTPRICE'].to_list(), int(len(portfolio['LOTPRICE']) * 0.3)))
        sum_list=[]
        for ele in lotprice_product:
            sum_list.append(sum(ele))
        sum_pd=pd.DataFrame(sum_list,columns=['product_sum'])
        print('最低追加门槛：',sum_pd.max()[0])
        print('最低赎回门槛：',sum_pd.quantile(0.5)[0])
        portfolio['LOT_INT'] = portfolio['weight'] * cash / portfolio['LOTPRICE']
        portfolio['LOT_INT'] = portfolio['LOT_INT'].astype(int)
        min_reb_require=sum(portfolio['LOT_INT'] * portfolio['LOTPRICE'])
        minimum_holding=cash - (cash - min_reb_require) * 0.2
        print('最低持有：',minimum_holding)
        print('最低调仓金额',portfolio['LOTPRICE'].max()/0.2)
    num_universe_list.to_csv(os.path.join(portfolio_path, "num_universe.csv"))

if __name__ == "__main__":
    portfolio_name = "US_Tech"
    factor1_list = list(['ROE'])  #'ROE','ROE_SEMI','ROE_TTM'
    factor2_list = list(['ROEGROWTH_SEMI'])  #'ROEGROWTH','ROEGROWTH_SEMI','ROEGROWTH_TTM'
    factor3_list = list(['REVOAGROWTH'])
    #'ROEVAR','ROEVAR_SEMI','ROEVAR_TTM'

    factor4_list = list(['RnD_onEQUITY'])
    #'RnD_RATIO','RnD_onASSET','RnD_onEQUITY','RnD_RATIO_SEMI','RnD_onASSET_SEMI','RnD_onEQUITY_SEMI','RnD_RATIO_TTM','RnD_onASSET_TTM','RnD_onEQUITY_TTM'


    factor5_list = list(['OPCF_MARGIN'])
    #'GROSS_PROFIT_MARGIN','OP_MARGIN','NET_MARGIN','GROSS_PROFIT_MARGIN_SEMI','OP_MARGIN_SEMI','NET_MARGIN_SEMI','GROSS_PROFIT_MARGIN_TTM','OP_MARGIN_TTM','NET_MARGIN_TTM'

    factor6_list = list(['GPOA'])
    # 'GPOA','GPOE','GPOA_SEMI','GPOE_SEMI','GPOA_TTM','GPOE_TTM'

    factor_df = []
    df = pd.DataFrame()
    cumreturn_all=pd.DataFrame()

    for factor1 in factor1_list:
        for factor2 in factor2_list:
            for factor3 in factor3_list:
                for factor4 in factor4_list:
                    for factor5 in factor5_list:
                        for factor6 in factor6_list:

                            factor_list = factor1+'  ' + factor2+'  ' + factor3+'  ' +factor4+'  ' + factor5+'  ' + factor6
                            factor_df.append(factor_list)
                            cumreturn,report = theme_backtest(portfolio_name, factor1, factor2, factor3,factor4, factor5, factor6)
                            # cumreturn=cumreturn.drop(columns=['YYYY'])
                            df = pd.concat([df, report], axis=0)
                            cumreturn_all=pd.concat([cumreturn_all, cumreturn], axis=1)
                            df.to_csv(os.path.join(addpath.result_path, portfolio_name, 'report_all.csv'))
                            cumreturn_all.to_csv(os.path.join(addpath.result_path, portfolio_name, 'cumreturn_all.csv'))

                            file = open(os.path.join(addpath.result_path, portfolio_name, 'factor.txt'), 'w')

                            file.write(str(factor_df))

                            file.close()

                            # cumreturn_path = os.path.join(addpath.result_path, portfolio_name, 'portfolio value.csv')
                            # reference_path = os.path.join(addpath.data_path, 'us_data', 'reference',
                            #                               'market_index_cumreturn.csv')
                            #
                            # cum_return = pd.read_csv(cumreturn_path)
                            # reference = pd.read_csv(reference_path).iloc[:, 1]+1.
                            # cum_return=cum_return.iloc[:, 1]
                            # reference=(reference-reference.shift())/reference.shift()
                            # cum_return=(cum_return-cum_return.shift())/cum_return.shift()
                            # cum_return=cum_return.dropna()
                            # reference=reference.dropna()
                            # x = sm.add_constant(reference)
                            # y = cum_return
                            # model = sm.OLS(y, x).fit()
                            # print(model.summary())