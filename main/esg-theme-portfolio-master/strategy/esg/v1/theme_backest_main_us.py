import pandas as pd
from datetime import datetime, timedelta
from strategy.esg.v1.utils.portfolio_config_loader import portfolio_config_loader
from strategy.esg.v1.portfolio_formation.portfolio_formation import portfolio_formation
from algorithm.utils.Theme_Helper import cal_rebalancing_dates
from algorithm.utils.backtest_engine_config_generator import create_backtest_engine_files
from algorithm.utils.result_analysis import result_metrics_calculation
from strategy.esg.v1.utils.cul_turnover import cul_turnover
from constants import *
from algorithm import addpath
import statsmodels.api as sm
import warnings
from quantcycle.utils.run_once import run_once
from quantcycle.app.result_exporter.result_reader import ResultReader
from os.path import join, exists
from os import makedirs

warnings.filterwarnings("ignore")


# def theme_backtest(portfolio_name,factor1,factor2,factor3,factor4, factor5, factor6):
def theme_backtest(portfolio_name, factor_list):
    symbol_list,symbol_industry, start, end, cash, commission, lookback_days, rebalance_freq, base_ccy, underlying_type, \
    number_assets, weighting_approach, market_cap_threshold, upper_bound, lower_bound = portfolio_config_loader(portfolio_name)

    portfolio_formation_for_backtest_periods(portfolio_name, lookback_days, symbol_list,symbol_industry, underlying_type, start, end,
                                             rebalance_freq, weighting_approach, number_assets, market_cap_threshold,
                                             cash, upper_bound, lower_bound, factor_list)
                                            #  cash,upper_bound, lower_bound,factor1,factor2,factor3,factor4, factor5, factor6)

    rebalance_freq='QUARTERLY'
    strategy_config_path,strategy_allocation=create_backtest_engine_files(portfolio_name, start, end, rebalance_freq, underlying_type, base_ccy, cash,
                                 commission)

    run_once(strategy_config_path, strategy_allocation)
    # inspect results
    reader = ResultReader(join(addpath.result_path, "esg", portfolio_name, portfolio_name))
    reader.to_csv(join(addpath.result_path, "esg", portfolio_name))

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

    report.to_csv(join(addpath.result_path, 'esg', portfolio_name, 'performance metrics.csv'))
    pv=pv.resample('1D').last().ffill()
    pv.to_csv(join(addpath.result_path, 'esg', portfolio_name, 'portfolio value.csv'))
    cul_turnover(rebalance_freq,start, end,portfolio_name,underlying_type,pv,cash)
    return pv, report


def portfolio_formation_for_backtest_periods(portfolio_name, lookback_days, symbol_list,symbol_industry, underlying_type,\
                                             start, end,rebalance_freq, weighting_approach, number_assets, market_cap_threshold,
                                             cash, upper_bound, lower_bound, factor_list):
                                             # cash,upper_bound, lower_bound,factor1,factor2,factor3,factor4, factor5, factor6):
    rebalancing_dates = cal_rebalancing_dates(start, end, rebalance_freq)

    if underlying_type == "US_STOCK":
        input_path = join(addpath.data_path, "us_data")
    elif underlying_type == "HK_STOCK":
        input_path = join(addpath.data_path, "hk_data")
    elif underlying_type == "CN_STOCK":
        input_path = join(addpath.data_path, "cn_data")

    trading_data_all = {}
    financial_data_all = {}
    reference_data_all = {}
    t_all_s=datetime.now()

    market_index_path = join(input_path, "reference", "market_index.csv")
    reference_data_all["market_index"] = pd.read_csv(market_index_path, parse_dates=[0], index_col=0)
    t_all_e = datetime.now()
    print('data reading'+f'{t_all_e - t_all_s}')

    portfolio_path = join(addpath.data_path, "strategy_temps", portfolio_name, "portfolios")
    if exists(portfolio_path):
        pass
    else:

        makedirs(portfolio_path)

    num_universe_list=pd.DataFrame(index=rebalancing_dates,columns=list(['num_universe','num_selected','num_have_esg']))
    for date in rebalancing_dates:
        print(date)
        data_start = date - timedelta(days=lookback_days)
        data_end = date
        trading_data = {}
        financial_data = {}
        reference_data = {}
        factors = {}

        factor_path=join(input_path,'CS_Factors')
        factors=pd.read_csv(join(factor_path, date.strftime("%Y-%m-%d") + ".csv"),index_col=0)

        reference_data["market_index"] = reference_data_all["market_index"][reference_data_all["market_index"].index <= data_end]
        reference_data["market_index"] = reference_data["market_index"][reference_data["market_index"].index >= data_start]

        portfolio,num_universe,num_selected,num_have_esg = portfolio_formation(portfolio_name, number_assets, symbol_list, symbol_industry, rebalance_freq, weighting_approach,
                                                                  trading_data, financial_data, reference_data, date, data_start, factors,
                                                                  underlying_type, market_cap_threshold, cash,
                                                                  upper_bound, lower_bound, factor_list)
        num_universe_list.loc[date,'num_universe']=num_universe
        num_universe_list.loc[date, 'num_selected'] = num_selected
        num_universe_list.loc[date, 'num_have_esg'] = num_have_esg
        portfolio.to_csv(join(portfolio_path, date.strftime("%Y-%m-%d") + ".csv"))
        # print(portfolio)
    num_universe_list.to_csv(join(portfolio_path, "num_universe.csv"))

if __name__ == "__main__":
    portfolio_name = "US_Tech"
    
    factor_df = []
    df = pd.DataFrame()
    cumreturn_all=pd.DataFrame()
    
    factor_list = ['ROE', 'ROEGROWTH_SEMI', 'REVOAGROWTH', 'RnD_onEQUITY', 'OPCF_MARGIN', 'GPOA']
    
    factor_df.append(factor_list)
    cumreturn,report = theme_backtest(portfolio_name, factor_list)
    # cumreturn=cumreturn.drop(columns=['YYYY'])
    df = pd.concat([df, report], axis=0)
    cumreturn_all=pd.concat([cumreturn_all, cumreturn], axis=1)
    df.to_csv(join(addpath.result_path, 'esg', portfolio_name, 'report_all.csv'))
    cumreturn_all.to_csv(join(addpath.result_path, 'esg', portfolio_name, 'cumreturn_all.csv'))
    
    file = open(join(addpath.result_path, 'esg', portfolio_name, 'factor.txt'), 'w')
    file.write(str(factor_df))
    file.close()
                          
                            
                            
    # portfolio_name = "US_Safety"
    #
    # factor_df = []
    # df = pd.DataFrame()
    # cumreturn_all=pd.DataFrame()
    #
    # factor_list = ['LEV', 'Z_SCORE', 'O_SCORE', 'GPOAVAR_SEMI', 'LONGTERM_LIABRATIO', 'CURRENT_RATIO']
    #
    # factor_df.append(factor_list)
    # cumreturn,report = theme_backtest(portfolio_name, factor_list)
    # # cumreturn=cumreturn.drop(columns=['YYYY'])
    # df = pd.concat([df, report], axis=0)
    # cumreturn_all=pd.concat([cumreturn_all, cumreturn], axis=1)
    # df.to_csv(os.path.join(addpath.result_path, 'smart_beta', portfolio_name, 'report_all.csv'))
    # cumreturn_all.to_csv(os.path.join(addpath.result_path, 'smart_beta', portfolio_name, 'cumreturn_all.csv'))
    #
    # file = open(os.path.join(addpath.result_path, 'smart_beta', portfolio_name, 'factor.txt'), 'w')
    # file.write(str(factor_df))
    # file.close()
                            
                            
    # portfolio_name = "US_Profit"

    # factor_df = []
    # df = pd.DataFrame()
    # cumreturn_all=pd.DataFrame()

    # factor_list = ['ROE', 'NIexACConASSET_TTM', 'GPOE', 'OPCFOE_SEMI', 'GROSS_PROFIT_MARGIN_TTM', 'EBITDAOA']

    # factor_df.append(factor_list)
    # cumreturn,report = theme_backtest(portfolio_name, factor_list)
    # # cumreturn=cumreturn.drop(columns=['YYYY'])
    # df = pd.concat([df, report], axis=0)
    # cumreturn_all=pd.concat([cumreturn_all, cumreturn], axis=1)
    # df.to_csv(os.path.join(addpath.result_path, 'smart_beta', portfolio_name, 'report_all.csv'))
    # cumreturn_all.to_csv(os.path.join(addpath.result_path, 'smart_beta', portfolio_name, 'cumreturn_all.csv'))

    # file = open(os.path.join(addpath.result_path, 'smart_beta', portfolio_name, 'factor.txt'), 'w')
    # file.write(str(factor_df))
    # file.close()
