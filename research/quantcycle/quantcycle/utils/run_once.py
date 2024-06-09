from quantcycle.engine.backtest_engine import BacktestEngine
from datetime import datetime
import json

def run_once(json_path, strategy_pool_df=None):
    print()
    ts0 = datetime.now()
    backtest_engine = BacktestEngine()
    if strategy_pool_df is None:
        backtest_engine.load_config(json.load(open(json_path)))
    else:
        backtest_engine.load_config(json.load(open(json_path)), strategy_pool_df)

    ts = datetime.now()
    backtest_engine.prepare()
    te = datetime.now()
    print("回测准备与导入数据用时", te-ts)
    ts = datetime.now()
    backtest_engine.start_backtest()
    te = datetime.now()
    print("回测+编译用时", te-ts)
    ts = datetime.now()
    backtest_engine.export_results()
    te = datetime.now()
    print("保存回测结果和策略状态用时", te-ts)
    te0 = datetime.now()
    print("回测总用时", te0-ts0)
