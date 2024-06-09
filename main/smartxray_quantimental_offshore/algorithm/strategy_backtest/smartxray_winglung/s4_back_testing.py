import numpy as np
import pandas as pd
from os import makedirs
from os.path import exists, join
import datetime
from algorithm.addpath import config_path, data_path, result_path, root_path
import sys

from os.path import dirname, abspath

if root_path not in sys.path:
    sys.path.insert(1, root_path)
from lib.commonalgo.bundles.base import BaseBundle, FuturesBundle

import calendar
import pytz
from zipline.api import (
    schedule_function,
    symbol,
    order,
    record,
    get_datetime,
    order_target_percent,
)
from zipline.finance import commission, slippage
from zipline.utils.events import date_rules, time_rules
import math
import configparser
import matplotlib.pyplot as plt
from tabulate import tabulate
from zipline import get_calendar
from lib.commonalgo.zipline_patch.run_algo import run_algorithm

from algorithm.utils.result_analysis import result_metrics_calculation


def index_backtesting(
    start,
    end,
    equity_pct,
    sector_preference,
    region_preference,
    commission_fee,
    initial_capital,
    rebalance_freq,
    model_freq,
):

    blweight_path = join(data_path, "weights")

    info = (
        str(int(equity_pct * 100)) + "-" + sector_preference + "-" + region_preference
    )
    weight_path = join(blweight_path, rebalance_freq + "-" + model_freq, info)
    if exists(weight_path):
        pass
    else:
        makedirs(weight_path)

    bundles_path = join(data_path, "bundles")
    bt_result_path = join(result_path, "backtest", rebalance_freq + "-" + model_freq)
    if exists(bt_result_path):
        pass
    else:
        makedirs(bt_result_path)

    result_path_rst = join(bt_result_path, "Result" + "_" + info + ".csv")
    result_path_return_index = join(bt_result_path, "PV" + "_" + info + ".csv")
    result_path_report = join(bt_result_path, "Performance" + "_" + info + ".csv")
    aqm_benchmark = "HSI"
    aqm_calendar = get_calendar("AQMHK")
    trading_dates = pd.DataFrame(
        aqm_calendar.all_sessions.to_datetime().tolist(), columns=["date"]
    )
    # trading_dates = pd.read_csv(os.path.join(tradingdates_path, 'AQMCN.csv'), parse_dates=['date'], engine='python')
    trading_dates["yyyy"] = trading_dates["date"].map(lambda x: x.year)
    trading_dates["mm"] = trading_dates["date"].map(lambda x: x.month)
    trading_date_list = trading_dates.groupby(["yyyy", "mm"]).date.max()
    trading_dates = trading_date_list.tolist()
    trading_dates = [i.date() for i in trading_dates]
    trading_dates = trading_dates[:-1]

    rebalance_dates_df = pd.read_csv(
        join(config_path, "rebalance_dates.csv"), parse_dates=[0]
    )
    rebalance_dates_df["year"] = rebalance_dates_df["rebalance_dates"].map(
        lambda x: x.year
    )
    rebalance_dates_df["month"] = rebalance_dates_df["rebalance_dates"].map(
        lambda x: x.month
    )
    rebalance_dict = (
        rebalance_dates_df[["year", "month"]]
        .groupby("year")
        .apply(lambda x: x["month"].tolist())
        .to_dict()
    )
    WINDOW_SIZE = 52

    # -------------------#
    # Start back-testing #
    # -------------------#

    start_unaware = datetime.datetime.strptime(start, "%Y-%m-%d")
    end_unaware = datetime.datetime.strptime(end, "%Y-%m-%d")
    start = pytz.utc.localize(start_unaware)
    end = pytz.utc.localize(end_unaware)

    # aqm_tz = 'Asia/Hong Kong'

    # if type == 'holding period return':
    bundle_name = "aqm130_mf_HKUS"
    bundle = BaseBundle(
        "aqm130_mf_HKUS", aqm_calendar, pd.Timestamp("2000-01-04", tz="UTC")
    )
    bundle.register_bundle(summaryfolder=bundles_path)

    funds_list_path = join(bundles_path, "Summary.csv")
    funds_list = pd.read_csv(funds_list_path)
    funds_list = funds_list["Name"].tolist()

    def initialize(context):
        """
        Initializing the parameters for back-testing and the strategy
        """

        context.stocks = funds_list
        context.symbols = [symbol(s) for s in context.stocks]
        context.weighting = 0.0
        context.n = len(context.stocks)
        context.month = 10

        # Re-balance every hour if spread criterion is satisfied
        schedule_function(monitor, date_rules.every_day(), time_rules.market_open())
        schedule_function(rebalance, date_rules.every_day(), time_rules.market_close())
        # Record variables at the end of each day.
        schedule_function(
            record_info, date_rules.every_day(), time_rules.market_close()
        )

        # Considering the transaction cost
        context.set_commission(commission.PerDollar(cost=commission_fee))
        context.set_slippage(
            slippage.VolumeShareSlippage(price_impact=0, volume_limit=1e5)
        )

    def handle_data(context, data):
        pass

    def monitor(context, data):
        # current_date = get_datetime()
        # print("[ %s ] Portfolio Value = %s " % (current_date, str(context.portfolio.portfolio_value)))
        # print("[ %s ] Leverage = %s " % (current_date, str(context.account.leverage)))
        pass
        # 使用data.history()获取历史数据
        # if datetime.datetime(current_date.year, current_date.month, current_date.day) == datetime.datetime(2014, 6, 5):
        # 	price_df = data.history(context.symbols, ['price'], 10, '1d')['price']
        # 	print(price_df[context.reb_stocks].iloc[-1].sort_values().to_string())
        # 	print(price_df.to_string())
        # 	print('hello')

        # 可以添加自己的策略逻辑和必要的计算
        # your_condition = True
        #
        # if your_condition:
        # 	pass
        # else:
        # 	pass

    def rebalance(context, data):
        """
        This function is the transaction module
        """
        current_date = get_datetime()

        if (
            (current_date.date() in trading_dates
            and current_date.date() != end_unaware.date()
            and current_date.month in rebalance_dict[current_date.year])
        ):
            if current_date.month != 12:
                weight_date = datetime.datetime(
                    current_date.year, current_date.month + 1, 1
                ) + datetime.timedelta(-1)
            if current_date.month == 12:
                weight_date = datetime.datetime(current_date.year, 12, 31)
            file_name = join(weight_path, str(weight_date.date()) + ".csv")
            rebalance_weight = pd.read_csv(file_name)
            rebalance_weight = rebalance_weight[rebalance_weight["Weight"] > 0]

            if (
                current_date.month == start_unaware.month
                and current_date.year == start_unaware.year
            ):
                context.last_weight = rebalance_weight
                context.last_weight = context.last_weight.rename(
                    columns={"Weight": "last_weight"}
                )
            else:
                rebalance_weight = pd.merge(
                    rebalance_weight,
                    context.last_weight,
                    left_on="Funds",
                    right_on="Funds",
                    how="outer",
                )
                rebalance_weight = rebalance_weight.fillna(0)
                # rebalance_weight['drift'] = abs(rebalance_weight['Weight'] - rebalance_weight['last_weight'])
                # rebalance_weight['Weight'] = np.where(rebalance_weight['drift'] >= 0.005, rebalance_weight['Weight'], rebalance_weight['last_weight'])
                # rebalance_weight['Weight'] = rebalance_weight['Weight'] / rebalance_weight['Weight'].sum()
                rebalance_weight = rebalance_weight.loc[:, ["Funds", "Weight"]]
                context.last_weight = rebalance_weight.loc[:, ["Funds", "Weight"]]
                context.last_weight = context.last_weight.rename(
                    columns={"Weight": "last_weight"}
                )
                context.last_weight = context.last_weight[
                    context.last_weight["last_weight"] > 0
                ]

            stocks = rebalance_weight["Funds"].tolist()
            weights = rebalance_weight["Weight"].tolist()
            symbols = [symbol(s) for s in stocks]
            n_symbol = rebalance_weight.shape[0]

            context.reb_stocks = symbols
            context.month = current_date.month

            # price_df = data.history(symbols, ['price'], 1, '1d')['price']

            # 可以添加自己的交易逻辑和日志
            # for i in range(n_symbol):
            # 	if pd.isna(price_df[symbols[i]].iloc[-1]):
            # 		print(i)
            # 		pass
            # 	else:
            # 		print(i)
            # 		order_target_percent(symbols[i], weights[i])
            for i in range(n_symbol):
                order_target_percent(symbols[i], weights[i])

    def record_info(context, data):
        """
        This function is called at the end of each day and record variables.
        """

        # 记录内容都会输出在csv中
        record(leverage=context.account.leverage, positions=context.portfolio.positions)

    # def analyze(context=None, results=None):
    # 	"""
    # 	Plot the portfolio and benchmark performance
    # 	"""
    #
    # 	# 可视化感兴趣的内容
    # 	ax = plt.subplot(111)
    # 	results.algorithm_period_return.plot()
    # 	results.benchmark_period_return.plot()
    # 	plt.grid(True)
    # 	plt.title('Back-testing result')
    # 	plt.ylabel('Cumulative Return')
    #
    # 	# Shrink current axis's height by 10% on the bottom
    # 	box = ax.get_position()
    # 	ax.set_position([
    # 		box.x0,
    # 		box.y0 + box.height * 0.1,
    # 		box.width,
    # 		box.height * 0.9
    # 	])
    # 	ax.legend(
    # 		loc='upper center',
    # 		bbox_to_anchor=(0.5, -0.2),
    # 		fancybox=True,
    # 		shadow=True,
    # 		ncol=5
    # 	)
    # 	plt.show()

    results = run_algorithm(
        start=start,
        end=end,
        initialize=initialize,
        capital_base=initial_capital,
        handle_data=handle_data,
        bundle=bundle_name,
        trading_calendar=get_calendar("AQMHK"),
        aqm_params={"bm_symbol": aqm_benchmark},
    )

    # ------------------------------- #
    # Display the back-testing result #
    # ------------------------------- #

    invest_period = (end - start).days / 365.0
    annual_return = (
        math.pow(results.algorithm_period_return.ix[-1] + 1, 1 / invest_period) - 1
    )

    financial_indicator = [
        ["Portfolio annualized return", annual_return],
        ["Portfolio cumulative return", results.algorithm_period_return.ix[-1]],
        ["Sharpe ratio", results.sharpe.ix[-1]],
        ["Max Draw Down", results.max_drawdown.ix[-1]],
        ["Beta", results.beta.ix[-1]],
        ["Alpha", results.alpha.ix[-1]],
        ["Volatility", results.algo_volatility.ix[-1]],
    ]
    print(
        tabulate(financial_indicator, headers=["Indicator", "Value"], tablefmt="orgtbl")
    )

    return_index = results.copy()[["algorithm_period_return"]] + 1

    # return_SHSZ300 = results[["benchmark_period_return"]] + 1
    return_index["Daily_Return"] = return_index["algorithm_period_return"].pct_change()
    return_index["portfolio_value"] = results.copy()["portfolio_value"]
    # 详细的回测数据输出在下表中
    results.to_csv(result_path_rst)
    # return_SHSZ300.to_csv(result_path_SHSZ300_return_index)
    return_index.to_csv(result_path_return_index)

    pv = results["portfolio_value"].to_frame()
    pv = pv / pv.iloc[0]
    report = result_metrics_calculation(pv)
    report.to_csv(result_path_report)
    # analyze(results=results)

    # metrics_df = pd.DataFrame()
    # metrics_df['Portfolio annualized return'] = [annual_return]
    # metrics_df['Portfolio cumulative return'] = [results.algorithm_period_return.ix[-1]]
    # metrics_df['Sharpe ratio'] = [results.sharpe.ix[-1]]
    # metrics_df['Max Draw Down'] = [results.max_drawdown.ix[-1]]
    # metrics_df['Beta'] = [results.beta.ix[-1]]
    # metrics_df['Alpha'] = [results.alpha.ix[-1]]
    # metrics_df['Volatility'] = [results.algo_volatility.ix[-1]]
    #
    return return_index
    # , return_SHSZ300, metrics_df


if __name__ == "__main__":
    equity_pct_list = [0.07, 0.17, 0.27, 0.45, 0.65]

    for equity_pct in equity_pct_list:
        return1 = index_backtesting(
            start="2019-03-29",
            end="2020-11-30",
            equity_pct=equity_pct,
            sector_preference="A1",
            region_preference="B1",
            commission_fee=0,
            initial_capital=1000000,
            rebalance_freq="6M",
            model_freq="12M",
        )
