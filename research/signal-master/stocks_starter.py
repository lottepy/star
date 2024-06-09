import json
import os
from datetime import datetime

import pandas as pd
from quantcycle.app.result_exporter.result_reader import ResultReader
from quantcycle.engine.backtest_engine import BacktestEngine

from utils.parser import parse


def run(config_path, strategy_pool_path, extract_csv=False,
        rmv_pkl=True, signal_to_csv=True):
    config = json.load(open(config_path))
    if rmv_pkl:
        try:
            os.remove(
                os.path.join(config['result_output']['status_dir'],
                             config['result_output']['status_name']+'.pkl')
            )
        except FileNotFoundError:
            pass
    strategy_pool_df = pd.read_csv(strategy_pool_path)
    ts0 = datetime.now()
    backtest_engine = BacktestEngine()
    backtest_engine.load_config(config, strategy_pool_df)
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
    if extract_csv:
        ResultReader(
            os.path.join(config["result_output"]["save_dir"],
                         config["result_output"]["save_name"])
        ).to_csv(config["result_output"]["save_dir"])
    if signal_to_csv:
        parse(config["result_output"]["save_dir"])


if __name__ == '__main__':
    # run("src/configs/stocks/KD.json",
    #     "src/pools/stocks/KD.csv")
    # run("src/configs/stocks/CCI.json",
    #     "src/pools/stocks/CCI.csv")
    # run("src/configs/stocks/MACD_P.json",
    #     "src/pools/stocks/MACD_P.csv")
    # run("src/configs/stocks/RSI.json",
    #     "src/pools/stocks/RSI.csv")
    # run("src/configs/stocks/DonchianChannel.json",
    #     "src/pools/stocks/DonchianChannel.csv")
    # run("src/configs/stocks/MA.json",
    #     "src/pools/stocks/MA.csv")
    # run("src/configs/stocks/EMA.json",
    #     "src/pools/stocks/EMA.csv")
    run("src/configs/stocks/BollingerBand.json",
        "src/pools/stocks/BollingerBand.csv")
    run("src/configs/stocks/Momentum.json",
        "src/pools/stocks/Momentum.csv")
