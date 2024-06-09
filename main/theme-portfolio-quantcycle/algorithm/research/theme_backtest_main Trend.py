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
from algorithm.portfolio_formation.factor_calculation import cal_factors
from algorithm.utils.backtest_engine_config_generator import create_backtest_engine_files
from algorithm.backtest_engine import MagnumBacktestEngine
from algorithm.backtest_engine.utils.get_logger import get_logger
from algorithm.backtest_engine.utils.result_processer import ResultProcesser
from algorithm.utils.result_analysis import result_metrics_calculation
from constants import *


def theme_backtest(portfolio_name,factor1,factor2,factor3,j):
    symbol_list, start, end, cash, commission, lookback_days, rebalance_freq, base_ccy, underlying_type, \
    number_assets, weighting_approach, market_cap_threshold, upper_bound, lower_bound = portfolio_config_loader(portfolio_name)

    market_cap_threshold=0.99
    number_assets=10
    upper_bound=0.2
    lower_bound=0.05
    para1=0.5
    para2=para1
    para3=para1

    portfolio_formation_for_backtest_periods(portfolio_name, lookback_days, symbol_list, underlying_type, start, end,
                                             rebalance_freq, weighting_approach, number_assets, market_cap_threshold,
                                             upper_bound, lower_bound,para1,para2,para3,factor1,factor2,factor3,j)

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

    report=result_metrics_calculation(portfolio_name, cash)
    return report





def portfolio_formation_for_backtest_periods(portfolio_name, lookback_days, symbol_list, underlying_type, start, end,
                                             rebalance_freq, weighting_approach, number_assets, market_cap_threshold,
                                             upper_bound, lower_bound,para1,para2,para3,factor1,factor2,factor3,j):
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
    for symbol in symbol_list:
        tmp_td_path = os.path.join(input_path, "trading", symbol + ".csv")
        tmp_fd_path = os.path.join(input_path, "financials", symbol + ".csv")
        # tmp_rd_path = os.path.join(input_path, "reference", symbol + ".csv")
        trading_data_all[symbol] = pd.read_csv(tmp_td_path, parse_dates=[0], index_col=0)
        financial_data_all[symbol] = pd.read_csv(tmp_fd_path, parse_dates=[0], index_col=0)
        trading_data_all[symbol]['EQY_SH_OUT'] = trading_data_all[symbol]['EQY_SH_OUT'].ffill()
        # reference_data_all[symbol] = pd.read_csv(tmp_rd_path, parse_dates=[0], index_col=0)
    market_index_path = os.path.join(input_path, "reference", "market_index.csv")
    reference_data_all["market_index"] = pd.read_csv(market_index_path, parse_dates=[0], index_col=0)

    # factors_all = {}
    # for symbol in symbol_list:
    #     factors_all[symbol] = cal_factors(symbol, trading_data_all, financial_data_all, reference_data_all, rebalance_freq)
    # factor_output = open(os.path.join(input_path, 'factor.pkl'), 'wb')
    # pickle.dump(factors_all, factor_output)
    # factor_output.close()

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
        for symbol in symbol_list:
            trading_data[symbol] = trading_data_all[symbol][trading_data_all[symbol].index <= data_end]
            trading_data[symbol] = trading_data[symbol][trading_data[symbol].index >= data_start]

            financial_data[symbol] = financial_data_all[symbol][financial_data_all[symbol].index <= data_end]
            financial_data[symbol] = financial_data[symbol][financial_data[symbol].index >= data_start - timedelta(days=90)]

            factors[symbol] = factors_all[symbol][factors_all[symbol].index <= data_end]
            factors[symbol] = factors[symbol][factors[symbol].index >= data_start - timedelta(days=90)]

            # reference_data[symbol] = reference_data_all[symbol][reference_data_all[symbol].index <= data_end]
            # reference_data[symbol] = reference_data[symbol][reference_data[symbol].index >= data_start]

        reference_data["market_index"] = reference_data_all["market_index"][reference_data_all["market_index"].index <= data_end]
        reference_data["market_index"] = reference_data["market_index"][reference_data["market_index"].index >= data_start]

        portfolio,num_universe,num_selected = portfolio_formation(portfolio_name, number_assets, symbol_list, rebalance_freq, weighting_approach,
                                        trading_data, financial_data, reference_data, date, data_start, factors,
                                        underlying_type, market_cap_threshold, upper_bound, lower_bound,para1,para2,para3,factor1,factor2,factor3,j)
        num_universe_list.loc[date,'num_universe']=num_universe
        num_universe_list.loc[date, 'num_selected'] = num_selected
        portfolio.to_csv(os.path.join(portfolio_path, date.strftime("%Y-%m-%d") + ".csv"))
    num_universe_list.to_csv(os.path.join(portfolio_path, "num_universe.csv"))

if __name__ == "__main__":

    portfolio_name = "CN_Liquidity"
    factor1_list=list(['REVS_1M','REVS10','REVS20','Momentum_2_7','Momentum_2_12','Momentum_2_15',
                       'Momentum_12_24','Momentum_12_36','Momentum_12_48','Momentum_12_60',
                       'Momentum_24_36','Momentum_24_48','Momentum_24_60','REVS_37_60'])
    factor2_list = list(['REVS10_path','REVS20_path','Momentum_2_7_path','Momentum_2_12_path'])  #path-dependent
    factor3_list = list(['PPReversal'])
    factor_df = df = pd.DataFrame(columns=list(['factor1', 'factor2', 'factor3', 'ranking']))
    factor_df = []
    df = pd.DataFrame()

    for factor1 in factor1_list:
        for factor2 in factor2_list:
            for factor3 in factor3_list:
                for j in list([0, 1, 2]):
                    factor_list = factor1 + factor2 + factor3 + str(j)
                    factor_df.append(factor_list)
                    report = theme_backtest(portfolio_name, factor1, factor2, factor3, j)
                    df = pd.concat([df, report], axis=0)

    df.to_csv(os.path.join(addpath.result_path, portfolio_name, 'report_all.csv'))
    file = open(os.path.join(addpath.result_path, portfolio_name, 'factor.txt'), 'w')

    file.write(str(factor_df))

    file.close()

