import numpy as np
import pandas as pd
from os import makedirs
from os.path import exists, join
import datetime
from dateutil.relativedelta import relativedelta
from algorithm.addpath import config_path, data_path, result_path, root_path
from os.path import dirname, abspath
from algorithm.utils.quantcycle_helper import get_buy_share_number
import math
from datetime import datetime, timedelta
import configparser
from tabulate import tabulate
import numba as nb
from numba.typed import Dict, List
from numba import types
from numba.experimental import jitclass
from quantcycle.app.signal_generator.base_strategy import defaultjitclass, BaseStrategy
from quantcycle.app.pms_manager.portfolio_manager import PorfolioManager
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import quantcycle.engine.backtest_engine
try:
    pms_type = PorfolioManager.class_type.instance_type
except:
    pms_type = "PMS_type"

    
@defaultjitclass()
class smartxray_wl(BaseStrategy):
    def init(self):
        rat = self.strategy_param[0]
        start_year = self.strategy_param[1]
        start_month = self.strategy_param[2]
        start_day = self.strategy_param[3]
        end_year = self.strategy_param[4]
        end_month = self.strategy_param[5]
        end_day = self.strategy_param[6]
        sector_preference = self.strategy_param[7]
        region_preference = self.strategy_param[8]
        
        self.sector_preference = 'A' + str(int(sector_preference))
        self.region_preference = 'B' + str(int(region_preference))
        
        self.bt_start = datetime(int(start_year), int(start_month), int(start_day))
        self.bt_end = datetime(int(end_year), int(end_month), int(end_day))
        
        self.lot_size = 1
        self.trading_buffer = 0
        self.current_weight = np.zeros(len(self.metadata['main']['symbols']))
        
        blweight_path = join(data_path, "weights")
        info = str(int(rat * 100)) + "-{}-{}".format(self.sector_preference, self.region_preference)
        self.weight_path = join(blweight_path, "6M-12M", info)

        self.month = 10
        
        trading_dates = pd.read_csv( join(data_path, 'quantcycle_backtest', 'calendar', 'calendar.csv') , index_col=[0], parse_dates=[1])
        trading_dates["yyyy"] = trading_dates["date"].map(lambda x: x.year)
        trading_dates["mm"] = trading_dates["date"].map(lambda x: x.month)
        trading_date_list = trading_dates.groupby(["yyyy", "mm"]).date.max()
        trading_dates = trading_date_list.tolist()
        trading_dates = [i.date() for i in trading_dates]
        self.trading_dates = trading_dates[:-1]
        
        rebalance_dates_df = pd.read_csv(join(config_path, "rebalance_dates.csv"), parse_dates=[0])
        rebalance_dates_df["year"] = rebalance_dates_df["rebalance_dates"].map(lambda x: x.year)
        rebalance_dates_df["month"] = rebalance_dates_df["rebalance_dates"].map(lambda x: x.month)
        self.rebalance_dict = (rebalance_dates_df[["year", "month"]].groupby("year").apply(lambda x: x["month"].tolist()).to_dict())
 
        
    def on_data(self, data_dict: dict, time_dict: dict, ref_data_dict: dict, ref_time_dict: dict):
        
        current_date = datetime(time_dict['main'][-1, 3], time_dict['main'][-1, 4], time_dict['main'][-1, 5], 0, 0, 0)
        self.current_date = current_date
        close_list = [rec[:, -1] for rec in data_dict['main']]
        close_time = []
        for idx in range(len(close_list)):
            close_time.append(datetime(time_dict['main'][idx, 3], time_dict['main'][idx, 4], time_dict['main'][idx, 5], 0, 0, 0))
        close_df = pd.DataFrame(close_list, index=close_time, columns=self.metadata['main']['symbols'])
        
        trading_shares = self.rebalance(current_date, close_df)
        
        if len(trading_shares) == 0:
            trading_shares = np.zeros(len(self.metadata['main']['symbols'])).reshape(1, -1)
    
        return trading_shares

    def rebalance(self, current_date, close_df):
        """
        This function is the transaction module
        """
        trading_shares_list = pd.Series()
        
        if (current_date.date() in self.trading_dates and current_date.date() != self.bt_end and current_date.month in self.rebalance_dict[current_date.year]):
            if current_date.month != 12:
                weight_date = datetime(current_date.year, current_date.month + 1, 1) + timedelta(-1)
            if current_date.month == 12:
                weight_date = datetime(current_date.year, 12, 31)
            file_name = join(self.weight_path, str(weight_date.date()) + ".csv")
            rebalance_weight = pd.read_csv(file_name)
            rebalance_weight = rebalance_weight[rebalance_weight["Weight"] > 0]

            if (
                current_date.month == self.bt_start.month
                and current_date.year == self.bt_start.year
            ):
                self.last_weight = rebalance_weight
                self.last_weight = self.last_weight.rename(
                    columns={"Weight": "last_weight"}
                )
            else:
                rebalance_weight = pd.merge(
                    rebalance_weight,
                    self.last_weight,
                    left_on="Funds",
                    right_on="Funds",
                    how="outer",
                )
                rebalance_weight = rebalance_weight.fillna(0)
                # rebalance_weight['drift'] = abs(rebalance_weight['Weight'] - rebalance_weight['last_weight'])
                # rebalance_weight['Weight'] = np.where(rebalance_weight['drift'] >= 0.005, rebalance_weight['Weight'], rebalance_weight['last_weight'])
                # rebalance_weight['Weight'] = rebalance_weight['Weight'] / rebalance_weight['Weight'].sum()
                rebalance_weight = rebalance_weight.loc[:, ["Funds", "Weight"]]
                self.last_weight = rebalance_weight.loc[:, ["Funds", "Weight"]]
                self.last_weight = self.last_weight.rename(
                    columns={"Weight": "last_weight"}
                )
                self.last_weight = self.last_weight[self.last_weight["last_weight"] > 0]
                
            weighting = rebalance_weight.set_index('Funds').merge(close_df.T, how='right', left_index=True, right_index=True)['Weight'].values
            weighting = np.nan_to_num(weighting, 0)
            
            sharesToBuy = get_buy_share_number(
                self.portfolio_manager.pv,
                weighting,
                close_df.iloc[-1].fillna(0).values,
                self.lot_size,
                self.trading_buffer
            )

            trading_shares_list = sharesToBuy[0] - self.portfolio_manager.current_holding
            trading_shares_list = np.nan_to_num(trading_shares_list,0).astype('int64')
            trading_shares_list = trading_shares_list.reshape(1, -1)
            
            trading_log = []
            trading_log_head = [
                "Security",
                "Current position",
                "Prices",
                "New Weight",
                "Prior Weight",
                "New position"
            ]
            
            trading_log = np.transpose(np.array([
                self.metadata['main']['symbols'],
                self.portfolio_manager.current_holding.astype('int64'),
                close_df.iloc[-1].values,
                weighting,
                self.current_weight,
                sharesToBuy[0]
            ])).tolist()
            
            # print(current_date)
            # print(tabulate(trading_log, headers=trading_log_head, tablefmt='orgtbl'))
            
            self.current_weight = weighting
            

            self.month = current_date.month

            # price_df = data.history(symbols, ['price'], 1, '1d')['price']

            # 可以添加自己的交易逻辑和日志
            # for i in range(n_symbol):
            # 	if pd.isna(price_df[symbols[i]].iloc[-1]):
            # 		print(i)
            # 		pass
            # 	else:
            # 		print(i)
            # 		order_target_percent(symbols[i], weights[i])
            
        return trading_shares_list


    # results = run_algorithm(
    #     start=start,
    #     end=end,
    #     initialize=initialize,
    #     capital_base=initial_capital,
    #     handle_data=handle_data,
    #     bundle=bundle_name,
    #     trading_calendar=get_calendar("AQMHK"),
    #     aqm_params={"bm_symbol": aqm_benchmark},
    # )

    # # ------------------------------- #
    # # Display the back-testing result #
    # # ------------------------------- #

    # invest_period = (end - start).days / 365.0
    # annual_return = (
    #     math.pow(results.algorithm_period_return.ix[-1] + 1, 1 / invest_period) - 1
    # )

    # financial_indicator = [
    #     ["Portfolio annualized return", annual_return],
    #     ["Portfolio cumulative return", results.algorithm_period_return.ix[-1]],
    #     ["Sharpe ratio", results.sharpe.ix[-1]],
    #     ["Max Draw Down", results.max_drawdown.ix[-1]],
    #     ["Beta", results.beta.ix[-1]],
    #     ["Alpha", results.alpha.ix[-1]],
    #     ["Volatility", results.algo_volatility.ix[-1]],
    # ]
    # print(
    #     tabulate(financial_indicator, headers=["Indicator", "Value"], tablefmt="orgtbl")
    # )

    # return_index = results.copy()[["algorithm_period_return"]] + 1
    # return_index["algorithm_period_return"] = (
    #     return_index["algorithm_period_return"] + 1
    # )
    # return_SHSZ300 = results[["benchmark_period_return"]] + 1
    # return_index["Daily_Return"] = return_index["algorithm_period_return"].pct_change()
    # return_index["portfolio_value"] = results.copy()["portfolio_value"]
    # # 详细的回测数据输出在下表中
    # results.to_csv(result_path_rst)
    # # return_SHSZ300.to_csv(result_path_SHSZ300_return_index)
    # return_index.to_csv(result_path_return_index)

    # pv = results["portfolio_value"].to_frame()
    # pv = pv / pv.iloc[0]
    # report = result_metrics_calculation(pv)
    # report.to_csv(result_path_report)
    # # analyze(results=results)

    # # metrics_df = pd.DataFrame()
    # # metrics_df['Portfolio annualized return'] = [annual_return]
    # # metrics_df['Portfolio cumulative return'] = [results.algorithm_period_return.ix[-1]]
    # # metrics_df['Sharpe ratio'] = [results.sharpe.ix[-1]]
    # # metrics_df['Max Draw Down'] = [results.max_drawdown.ix[-1]]
    # # metrics_df['Beta'] = [results.beta.ix[-1]]
    # # metrics_df['Alpha'] = [results.alpha.ix[-1]]
    # # metrics_df['Volatility'] = [results.algo_volatility.ix[-1]]
    # #
    # return return_index
    # # , return_SHSZ300, metrics_df


# if __name__ == "__main__":
#     equity_pct_list = [0.07, 0.17, 0.27, 0.45, 0.65]

#     for equity_pct in equity_pct_list:
#         return1 = index_backtesting(
#             start="2016-02-29",
#             end="2020-11-30",
#             equity_pct=equity_pct,
#             sector_preference="A1",
#             region_preference="B1",
#             commission_fee=0,
#             initial_capital=1000000,
#             rebalance_freq="6M",
#             model_freq="12M",
#         )
