from quantcycle.utils.strategy_pool_generator import strategy_pool_generator
import pandas as pd
STOCK_LIST = pd.read_csv('src/pools/stocks/aqm_cn_stock_1364.csv',
                         index_col=0).values[:, -1].tolist()


def KD():
    pool_setting = {
        "symbol": {"STOCKS": STOCK_LIST},
        "strategy_module": "src.technicals",
        "strategy_name": "signalKD",
        "save_path": "src/pools/stocks/KD.csv",
        "params": {
            "length1": [10.0, 15.0, 20.0, 40.0],
            "length2": [3.0],
            "length3": [3.0],
        }
    }
    strategy_pool_generator(pool_setting)


def CCI():
    pool_setting = {
        "symbol": {"STOCKS": STOCK_LIST},
        "strategy_module": "src.technicals",
        "strategy_name": "signalCCI",
        "save_path": "src/pools/stocks/CCI.csv",
        "params": {
            "length": [10.0, 15.0, 20.0, 40.0]
        }
    }
    strategy_pool_generator(pool_setting)


def MACD_P():
    pool_setting = {
        "symbol": {"STOCKS": STOCK_LIST},
        "strategy_module": "src.technicals",
        "strategy_name": "signalMACD",
        "save_path": "src/pools/stocks/MACD_P.csv",
        "params": {
            "length1": [5.0, 10.0],
            "length2": [20.0, 30.0],
            "length3": [9.0],
        }
    }
    strategy_pool_generator(pool_setting)


def RSI():
    pool_setting = {
        "symbol": {"STOCKS": STOCK_LIST},
        "strategy_module": "src.technicals",
        "strategy_name": "signalRSI",
        "save_path": "src/pools/stocks/RSI.csv",
        "params": {
            "length": [10.0, 15.0, 20.0, 40.0]
        }
    }
    strategy_pool_generator(pool_setting)


def DonchianChannel():
    pool_setting = {
        "symbol": {"STOCKS": STOCK_LIST},
        "strategy_module": "src.technicals",
        "strategy_name": "signalDonchianChannel",
        "save_path": "src/pools/stocks/DonchianChannel.csv",
        "params": {
            "len_MA": [5.0, 10.0, 15.0, 20.0, 40.0, 60.0, 80.0, 100.0]
        }
    }
    strategy_pool_generator(pool_setting)


def MA():
    pool_setting = {
        "symbol": {"STOCKS": STOCK_LIST},
        "strategy_module": "src.technicals",
        "strategy_name": "signalMA",
        "save_path": "src/pools/stocks/MA.csv",
        "params": {
            "len_MA": [5.0, 10.0, 15.0, 20.0, 30.0, 40.0, 60.0, 80.0, 100.0]
        }
    }
    strategy_pool_generator(pool_setting)


def EMA():
    pool_setting = {
        "symbol": {"STOCKS": STOCK_LIST},
        "strategy_module": "src.technicals",
        "strategy_name": "signalEMA",
        "save_path": "src/pools/stocks/EMA.csv",
        "params": {
            "len_MA": [5.0, 10.0, 15.0, 20.0, 30.0, 40.0, 60.0, 80.0, 100.0]
        }
    }
    strategy_pool_generator(pool_setting)


def BollingerBand():
    pool_setting = {
        "symbol": {"STOCKS": STOCK_LIST},
        "strategy_module": "src.technicals",
        "strategy_name": "signalBollingerBand",
        "save_path": "src/pools/stocks/BollingerBand.csv",
        "params": {
            "length": [10, 15, 20, 40],
            "width": [0.0, 0.5, 1.0, 1.5, 2.0],
        }
    }
    strategy_pool_generator(pool_setting)


def Momentum():
    pool_setting = {
        "symbol": {"STOCKS": STOCK_LIST},
        "strategy_module": "src.momentum",
        "strategy_name": "signalMomentum",
        "save_path": "src/pools/stocks/Momentum.csv",
        "params": {
            "rank_period": [75.0, 85.0, 95.0, 105.0, 115.0, 125.0],
            "mom_window": [20.0, 30.0, 40.0, 50.0, 60.0, 70.0],
        }
    }
    strategy_pool_generator(pool_setting)


if __name__ == '__main__':
    KD()
    CCI()
    MACD_P()
    RSI()
    DonchianChannel()
    MA()
    EMA()
    BollingerBand()
    Momentum()
