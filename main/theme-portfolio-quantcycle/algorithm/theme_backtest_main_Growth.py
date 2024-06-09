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
warnings.filterwarnings("ignore")


def theme_backtest(portfolio_name,factor1,factor2,factor3,factor4, factor5, factor6):
    symbol_list,symbol_industry, start, end, cash, commission, lookback_days, rebalance_freq, base_ccy, underlying_type, \
    number_assets, weighting_approach, market_cap_threshold, upper_bound, lower_bound = portfolio_config_loader(portfolio_name)

    portfolio_formation_for_backtest_periods(portfolio_name, lookback_days, symbol_list,symbol_industry, underlying_type, start, end,
                                             rebalance_freq, weighting_approach, number_assets, market_cap_threshold,
                                             cash,upper_bound, lower_bound,factor1,factor2,factor3,factor4, factor5, factor6)

    create_backtest_engine_files(portfolio_name, start, end, rebalance_freq, underlying_type, base_ccy, cash,
                                 commission)

    bt_config_path = os.path.join(addpath.data_path, "strategy_temps", portfolio_name)
    logger = get_logger('backtest_engine', log_path='backtest_engine.log')
    t_all_s = datetime.now()
    backtest_engine = MagnumBacktestEngine()
    backtest_engine.find_local_data(os.path.join(addpath.data_path, "backtest_temps", "daily_data"), None,
                                    os.path.join(addpath.data_path, "backtest_temps", "stock_info",
                                                 "symbol_feature_df.csv"))  # optional
    # backtest_engine.find_local_ref_data('strategy_files/ref_factor')  # optional
    backtest_engine.load_parameters(os.path.join(bt_config_path, "parameters.json"))
    backtest_engine.load_data()
    backtest_engine.run()
    t_all_e = datetime.now()
    logger.info(f'回测+数据总用时{t_all_e - t_all_s}')

    result_processor = ResultProcesser(result_path=backtest_engine.params['result_output']['save_dir'])
    result_processor.get_strategy('virtual_0').to_csv(
        os.path.join(backtest_engine.params['result_output']['save_dir'], 'backtest_u_0_strategy_id_virtual_0.csv'))

    report,cumulative_return=result_metrics_calculation(portfolio_name, cash)
    exreturn_metrics_calculation(portfolio_name,underlying_type)

    rebalancing_dates = cal_rebalancing_dates(start, end, rebalance_freq)
    print(cash)
    cul_turnover(rebalancing_dates,start, end,portfolio_name,underlying_type,cash)
    return report,cumulative_return





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

    pkl_file = open(os.path.join(input_path, 'factor.pkl'), 'rb')
    factors_all = pickle.load(pkl_file)
    pkl_file.close()

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
        # print(portfolio)
    num_universe_list.to_csv(os.path.join(portfolio_path, "num_universe.csv"))

if __name__ == "__main__":
    portfolio_name = "US_Growth"
    factor1_list = list(['ROAGROWTH_SEMI'])
    #'ROAGROWTH','ROAGROWTH_SEMI','ROAGROWTH_TTM','ROEGROWTH','ROEGROWTH_SEMI','ROEGROWTH_TTM'
    factor2_list = list(['REVOAGROWTH_TTM'])
    #'REVOAGROWTH','REVOAGROWTH_SEMI','REVOAGROWTH_TTM','REVOEGROWTH','REVOEGROWTH_SEMI','REVOEGROWTH_TTM'
    factor3_list = list(['OPCFOAGROWTH_TTM'])
    #'OPCFOAGROWTH','OPCFOAGROWTH_SEMI','OPCFOAGROWTH_TTM','OPCFOEGROWTH','OPCFOEGROWTH_SEMI','OPCFOEGROWTH_TTM'

    factor4_list = list(['GPOAGROWTH'])
    #'GPOAGROWTH','GPOAGROWTH_SEMI','GPOAGROWTH_TTM','GPOEGROWTH','GPOEGROWTH_SEMI','GPOEGROWTH_TTM'
    factor5_list = list(['EBITDAOAGROWTH_SEMI'])
    #'EBITDAOAGROWTH','EBITDAOAGROWTH_SEMI','EBITDAOAGROWTH_TTM','EBITDAOEGROWTH','EBITDAOEGROWTH_SEMI','EBITDAOEGROWTH_TTM'
    factor6_list = list(['EBITOAGROWTH_TTM'])
    #'EBITOAGROWTH','EBITOAGROWTH_SEMI','EBITOAGROWTH_TTM','EBITOEGROWTH','EBITOEGROWTH_SEMI','EBITOEGROWTH_TTM','EBTOAGROWTH','EBTOAGROWTH_SEMI','EBTOAGROWTH_TTM','EBTOEGROWTH','EBTOEGROWTH_SEMI','EBTOEGROWTH_TTM'
    factor_df = []
    df = pd.DataFrame()
    cumreturn_all=pd.DataFrame()

    for factor1 in factor1_list:
        for factor2 in factor2_list:
            for factor3 in factor3_list:
                for factor4 in factor4_list:
                    for factor5 in factor5_list:
                        for factor6 in factor6_list:

                            factor_list = factor1 + factor2 + factor3 +factor4 + factor5 + factor6
                            factor_df.append(factor_list)
                            report,cumreturn = theme_backtest(portfolio_name, factor1, factor2, factor3,factor4, factor5, factor6)
                            cumreturn=cumreturn.drop(columns=['YYYY'])
                            df = pd.concat([df, report], axis=0)
                            cumreturn_all=pd.concat([cumreturn_all, cumreturn], axis=1)
                            df.to_csv(os.path.join(addpath.result_path, portfolio_name, 'report_all.csv'))
                            cumreturn_all.to_csv(os.path.join(addpath.result_path, portfolio_name, 'cumreturn_all.csv'))

                            file = open(os.path.join(addpath.result_path, portfolio_name, 'factor.txt'), 'w')

                            file.write(str(factor_df))

                            file.close()

                            cumreturn_path = os.path.join(addpath.result_path, portfolio_name, 'cumulative_return.csv')
                            reference_path = os.path.join(addpath.data_path, 'us_data', 'reference',
                                                          'market_index_cumreturn.csv')

                            cum_return = pd.read_csv(cumreturn_path)
                            reference = pd.read_csv(reference_path).iloc[:, 1]
                            x = sm.add_constant(reference)
                            y = cum_return.iloc[:, 1]
                            model = sm.OLS(y, x).fit()
                            print(model.summary())
