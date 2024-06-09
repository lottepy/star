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
    # run("src/configs/KD.json",
    #     "src/pools/KD.csv")
    # run("src/configs/CCI.json",
    #     "src/pools/CCI.csv")
    # run("src/configs/MACD_P.json",
    #     "src/pools/MACD_P.csv")
    # run("src/configs/RSI.json",
    #     "src/pools/RSI.csv")
    # run("src/configs/DonchianChannel.json",
    #     "src/pools/DonchianChannel.csv")
    # run("src/configs/MA.json",
    #     "src/pools/MA.csv")
    # run("src/configs/EMA.json",
    #     "src/pools/EMA.csv")
    # run("src/configs/BollingerBand.json",
    #     "src/pools/BollingerBand.csv")
    # run("src/configs/Reversion.json",
    #     "src/pools/Reversion.csv")
    # run("src/configs/Momentum.json",
    #     "src/pools/Momentum.csv")

    # DB
    # run("src/configs/DB/Momentum.json",
    #     "src/pools/DB/Momentum.csv")
    # run("src/configs/DB/Carry.json",
    #     "src/pools/DB/Carry.csv")
    # run("src/configs/DB/CFTCMomentum.json",
    #     "src/pools/DB/CFTCMomentum.csv")
    # run("src/configs/DB/CFTCReversal.json",
    #     "src/pools/DB/CFTCReversal.csv")
    run("src/configs/DB/MomentumSpillOver.json",
        "src/pools/DB/MomentumSpillOver.csv")
