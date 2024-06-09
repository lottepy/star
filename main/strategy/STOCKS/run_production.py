import collections.abc
import json
import uuid
from datetime import datetime

import pandas as pd
import requests
from quantcycle.utils.strategy_pool_generator import strategy_pool_generator

START_DATE = (2017, 1, 1)
# END_DATE = (2018, 2, 1)
END_DATE = (datetime.today().year, 
            datetime.today().month,
            datetime.today().day -1 )


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
    stock_list = pd.read_csv('strategy/uat_stocks/stock_universe/aqm_cn_stock.csv',
                             index_col=0).values[:, -1].tolist()[:100]
    pool_setting = {
        "symbol": {"STOCKS": stock_list},
        "strategy_module":
            "strategy.stocks.algorithm.external_signal_strategy",
        "strategy_name": strategy_name,
        "params": {}
    }
    return strategy_pool_generator(pool_setting, save=False)


def generate_portfolio_strategy_pool(strategy_name_list):
    stock_list = pd.read_csv('strategy/uat_stocks/stock_universe/aqm_cn_stock.csv',
                             index_col=0).values[:, -1].tolist()[:100]
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


def strategy_subscription(config, strategy_pool_df, last_level):
    req_url = "http://172.29.39.140:1234/strategy_subscription"

    if last_level:
        account_config = {
            "engine": {
                "Order_Router_name": "PortfolioTaskEngineOrderRouter"
            },
            "PortfolioTaskEngineOrderRouter": {
                "ACCOUNT": "369",
                "task_ippath": "http://172.31.86.37:15567/oms/api/task-engine/",
                "subtask_ippath": "http://172.31.86.37:15561/oms/api/execution-engine/",
                "brokerType": "IB"
            }
        }
        config = update_dict(config, account_config)

    request_id = uuid.uuid1().hex
    params = {'config_json': config, "strategy_df": strategy_pool_df.to_json()}
    res = requests.post(url=req_url,
                        headers={'x-request-id': request_id},
                        json=params)
    print(res.text)


def receive_result(strategy):
    req_url = "http://172.29.39.140:1234/result"

    request_id = uuid.uuid1().hex
    params = {"engine_name": strategy, "id_list": [0], "fields": [
        "pnl", "position"], "phase": "order_feedback"}
    res = requests.get(url=req_url, headers={
                       'x-request-id': request_id}, json=params)
    print(res.text)
    df = pd.read_json(json.loads(res.text)['data']['0'][0])
    print(df)
    df = pd.read_json(json.loads(res.text)['data']['0'][1])
    print(df)


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
                "DataCenterArgs": {
                    "dir": "/mnt/smb/alioss/1_StockStrategy/data/signal"
                },
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
                "DataCenterArgs": {
                    "dir": "/mnt/smb/alioss/1_StockStrategy/data/filter"
                },
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

    strategy_subscription(config=config,
                          strategy_pool_df=strategy_pool_df,
                          last_level=False)


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

    strategy_subscription(config=config,
                          strategy_pool_df=strategy_pool_df,
                          last_level=True)


if __name__ == '__main__':
    run("NORTHBOUND_HOLD")
    run("QUALITY_AND_REVERSION")

    run_portfolio(["NORTHBOUND_HOLD",
                   "QUALITY_AND_REVERSION"])
