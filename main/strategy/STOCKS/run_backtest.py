import collections.abc
import json
import os
import sys
from datetime import datetime

import pandas as pd
from quantcycle.app.result_exporter.result_reader import ResultReader
from quantcycle.engine.backtest_engine import BacktestEngine
from quantcycle.utils.strategy_pool_generator import strategy_pool_generator

sys.path.append(".")

START_DATE = (2017, 1, 1)
# END_DATE = (2018, 2, 1)
END_DATE = (datetime.today().year,
            datetime.today().month,
            datetime.today().day)


def update_dict(old, new):
    '''
        https://stackoverflow.com/questions/3232943/update-value-of-a-nested-dictionary-of-varying-depth
    '''
    for k, v in new.items():
        if isinstance(v, collections.abc.Mapping):
            old[k] = update_dict(old.get(k, {}), v)
        else:
            old[k] = v
    return old


def generate_strategy_pool(strategy_name):
    stock_list = pd.read_csv('strategy/stocks/stock_universe/aqm_cn_stock.csv',
                             index_col=0).values[:, -1].tolist()
    pool_setting = {
        "symbol": {"STOCKS": stock_list},
        "strategy_module":
            "strategy.stocks.algorithm.external_signal_strategy",
        "strategy_name": strategy_name,
        "params": {}
    }
    return strategy_pool_generator(pool_setting, save=False)


def generate_portfolio_strategy_pool(strategy_name_list):
    stock_list = pd.read_csv('strategy/stocks/stock_universe/aqm_cn_stock.csv',
                             index_col=0).values[:, -1].tolist()
    symbols = {strategy_name: [0] for strategy_name in strategy_name_list}
    symbols["STOCKS"] = stock_list
    pool_setting = {
        "symbol": symbols,
        "strategy_module":
            "strategy.stocks.algorithm.portfolio_strategy",
        "strategy_name": "PortfolioStrategy",
        "params": {}
    }
    return strategy_pool_generator(pool_setting, save=False)


def run_once(config, strategy_pool_df, extract_csv=True):
    ts0 = datetime.now()
    backtest_engine = BacktestEngine()
    backtest_engine.load_config(config, strategy_pool_df)
    ts = datetime.now()
    backtest_engine.prepare()
    te = datetime.now()
    print("回测准备与导入数据用时", te - ts)
    ts = datetime.now()
    backtest_engine.start_backtest()
    te = datetime.now()
    print("回测+编译用时", te - ts)
    ts = datetime.now()
    backtest_engine.export_results()
    te = datetime.now()
    print("保存回测结果和策略状态用时", te - ts)
    te0 = datetime.now()
    print("回测总用时", te0 - ts0)
    if extract_csv:
        ResultReader(
            os.path.join(config["result_output"]["save_dir"],
                         config["result_output"]["save_name"])
        ).to_csv(config["result_output"]["save_dir"])


def run(strategy_name):
    config = json.load(open('strategy/stocks/configs/external_signal_template.json'))
    new_config = {
        "start_year": START_DATE[0],
        "start_month": START_DATE[1],
        "start_day": START_DATE[2],
        "end_year": END_DATE[0],
        "end_month": END_DATE[1],
        "end_day": END_DATE[2],
        "result_output": {
            "save_dir": f"strategy/stocks/results/{strategy_name}",
            "status_dir": f"strategy/stocks/results/{strategy_name}"
        },
        "engine": {
            "engine_name": strategy_name
        },
        "ref_data": {
            "signal": {
                "Symbol": [strategy_name],
                "StartDate": {
                    "Year": START_DATE[0],
                    "Month": START_DATE[1],
                    "Day": START_DATE[2]
                },
                "EndDate": {
                    "Year": END_DATE[0],
                    "Month": END_DATE[1],
                    "Day": END_DATE[2],
                }
            },
            "filter": {
                "StartDate": {
                    "Year": START_DATE[0],
                    "Month": START_DATE[1],
                    "Day": START_DATE[2]
                },
                "EndDate": {
                    "Year": END_DATE[0],
                    "Month": END_DATE[1],
                    "Day": END_DATE[2],
                }
            }
        }
    }
    config = update_dict(config, new_config)

    strategy_pool_df = generate_strategy_pool(strategy_name)

    run_once(config=config,
             strategy_pool_df=strategy_pool_df,
             extract_csv=True
             )


def run_portfolio(strategy_name_list):
    config = json.load(open('strategy/stocks/configs/portfolio.json'))
    secondary_data = {}
    for strategy_name in strategy_name_list:
        one_secondary_data = {
            "DataCenter": "ResultReader",
            "DataCenterArgs": {
                "DataPath": f"strategy/stocks/results/{strategy_name}/result"
            },
            "Fields": ["position"],
            "Frequency": "DAILY"
        }
        secondary_data[strategy_name] = one_secondary_data
    new_config = {
        "start_year": START_DATE[0],
        "start_month": START_DATE[1],
        "start_day": START_DATE[2],
        "end_year": END_DATE[0],
        "end_month": END_DATE[1],
        "end_day": END_DATE[2],
        "result_output": {
            "save_dir": "strategy/stocks/results/portfolio",
            "status_dir": "strategy/stocks/results/portfolio"
        },
        "engine": {
            "engine_name": "portfolio"
        },
        "secondary_data": secondary_data
    }
    config = update_dict(config, new_config)

    strategy_pool_df = generate_portfolio_strategy_pool(strategy_name_list)

    run_once(config=config,
             strategy_pool_df=strategy_pool_df,
             extract_csv=True
             )


if __name__ == '__main__':
    run("NORTHBOUND_HOLD")
    run("QUALITY_AND_REVERSION")

    run_portfolio(["NORTHBOUND_HOLD",
                   "QUALITY_AND_REVERSION"])
