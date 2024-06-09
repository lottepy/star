from quantcycle.utils.strategy_pool_generator import strategy_pool_generator


def KD():
    pool_setting = {
        "symbol": {"FX": ['EURUSD', 'USDJPY', 'NZDJPY', 'NOKSEK', 'GBPJPY',
                          'CADJPY', 'CADCHF', 'AUDNZD', 'AUDJPY', 'AUDCAD',
                          'USDSEK', 'USDNOK', 'EURCHF', 'USDCHF', 'USDCAD',
                          'NZDUSD', 'GBPUSD', 'AUDUSD', 'USDSGD', 'GBPCHF',
                          'USDINR', 'USDIDR', 'USDTWD', 'USDKRW']},
        "strategy_module": "src.technicals",
        "strategy_name": "signalKD",
        "save_path": "src/pools/KD.csv",
        "params": {
            "length1": [10.0, 15.0, 20.0, 40.0],
            "length2": [3.0],
            "length3": [3.0],
        }
    }
    strategy_pool_generator(pool_setting)


def CCI():
    pool_setting = {
        "symbol": {"FX": ['EURUSD', 'USDJPY', 'NZDJPY', 'NOKSEK', 'GBPJPY',
                          'CADJPY', 'CADCHF', 'AUDNZD', 'AUDJPY', 'AUDCAD',
                          'USDSEK', 'USDNOK', 'EURCHF', 'USDCHF', 'USDCAD',
                          'NZDUSD', 'GBPUSD', 'AUDUSD', 'USDSGD', 'USDTHB',
                          'GBPCAD', 'EURCAD', 'EURJPY', 'EURGBP', 'GBPCHF',
                          'USDINR', 'USDIDR', 'USDTWD', 'USDKRW']},
        "strategy_module": "src.technicals",
        "strategy_name": "signalCCI",
        "save_path": "src/pools/CCI.csv",
        "params": {
            "length": [10.0, 15.0, 20.0, 40.0]
        }
    }
    strategy_pool_generator(pool_setting)


def MACD_P():
    pool_setting = {
        "symbol": {"FX": ['EURUSD', 'USDJPY', 'NZDJPY', 'NOKSEK', 'GBPJPY',
                          'CADJPY', 'CADCHF', 'AUDNZD', 'AUDJPY', 'AUDCAD',
                          'USDSEK', 'USDNOK', 'EURCHF', 'USDCHF', 'USDCAD',
                          'NZDUSD', 'GBPUSD', 'AUDUSD', 'USDSGD', 'USDTHB',
                          'GBPCAD', 'EURCAD', 'EURJPY', 'EURGBP', 'GBPCHF',
                          'USDINR', 'USDIDR', 'USDTWD', 'USDKRW']},
        "strategy_module": "src.technicals",
        "strategy_name": "signalMACD",
        "save_path": "src/pools/MACD_P.csv",
        "params": {
            "length1": [5.0, 10.0],
            "length2": [20.0, 30.0],
            "length3": [9.0],
        }
    }
    strategy_pool_generator(pool_setting)


def RSI():
    pool_setting = {
        "symbol": {"FX": ['EURUSD', 'USDJPY', 'NZDJPY', 'NOKSEK', 'GBPJPY',
                          'CADJPY', 'CADCHF', 'AUDNZD', 'AUDJPY', 'AUDCAD',
                          'USDSEK', 'USDNOK', 'EURCHF', 'USDCHF', 'USDCAD',
                          'NZDUSD', 'GBPUSD', 'AUDUSD', 'USDSGD', 'USDTHB',
                          'GBPCAD', 'EURCAD', 'EURJPY', 'EURGBP', 'GBPCHF',
                          'USDINR', 'USDIDR', 'USDTWD', 'USDKRW']},
        "strategy_module": "src.technicals",
        "strategy_name": "signalRSI",
        "save_path": "src/pools/RSI.csv",
        "params": {
            "length": [10.0, 15.0, 20.0, 40.0]
        }
    }
    strategy_pool_generator(pool_setting)


def DonchianChannel():
    pool_setting = {
        "symbol": {"FX": ['EURUSD', 'USDJPY', 'NZDJPY', 'NOKSEK', 'GBPJPY',
                          'CADJPY', 'CADCHF', 'AUDNZD', 'AUDJPY', 'AUDCAD',
                          'USDSEK', 'USDNOK', 'EURCHF', 'USDCHF', 'USDCAD',
                          'NZDUSD', 'GBPUSD', 'AUDUSD', 'USDSGD', 'USDTHB',
                          'GBPCAD', 'EURCAD', 'EURJPY', 'EURGBP', 'GBPCHF',
                          'USDINR', 'USDIDR', 'USDTWD', 'USDKRW']},
        "strategy_module": "src.technicals",
        "strategy_name": "signalDonchianChannel",
        "save_path": "src/pools/DonchianChannel.csv",
        "params": {
            "len_MA": [5.0, 10.0, 15.0, 20.0, 40.0, 60.0, 80.0, 100.0]
        }
    }
    strategy_pool_generator(pool_setting)


def MA():
    pool_setting = {
        "symbol": {"FX": ['EURUSD', 'USDJPY', 'NZDJPY', 'NOKSEK', 'GBPJPY',
                          'CADJPY', 'CADCHF', 'AUDNZD', 'AUDJPY', 'AUDCAD',
                          'USDSEK', 'USDNOK', 'EURCHF', 'USDCHF', 'USDCAD',
                          'NZDUSD', 'GBPUSD', 'AUDUSD', 'USDSGD', 'USDTHB',
                          'GBPCAD', 'EURCAD', 'EURJPY', 'EURGBP', 'GBPCHF',
                          'USDINR', 'USDIDR', 'USDTWD', 'USDKRW']},
        "strategy_module": "src.technicals",
        "strategy_name": "signalMA",
        "save_path": "src/pools/MA.csv",
        "params": {
            "len_MA": [5.0, 10.0, 15.0, 20.0, 30.0, 40.0, 60.0, 80.0, 100.0]
        }
    }
    strategy_pool_generator(pool_setting)


def EMA():
    pool_setting = {
        "symbol": {"FX": ['EURUSD', 'USDJPY', 'NZDJPY', 'NOKSEK', 'GBPJPY',
                          'CADJPY', 'CADCHF', 'AUDNZD', 'AUDJPY', 'AUDCAD',
                          'USDSEK', 'USDNOK', 'EURCHF', 'USDCHF', 'USDCAD',
                          'NZDUSD', 'GBPUSD', 'AUDUSD', 'USDSGD', 'USDTHB',
                          'GBPCAD', 'EURCAD', 'EURJPY', 'EURGBP', 'GBPCHF',
                          'USDINR', 'USDIDR', 'USDTWD', 'USDKRW']},
        "strategy_module": "src.technicals",
        "strategy_name": "signalEMA",
        "save_path": "src/pools/EMA.csv",
        "params": {
            "len_MA": [5.0, 10.0, 15.0, 20.0, 30.0, 40.0, 60.0, 80.0, 100.0]
        }
    }
    strategy_pool_generator(pool_setting)


def BollingerBand():
    pool_setting = {
        "symbol": {"FX": ['EURUSD', 'USDJPY', 'NZDJPY', 'NOKSEK', 'GBPJPY',
                          'CADJPY', 'CADCHF', 'AUDNZD', 'AUDJPY', 'AUDCAD',
                          'USDSEK', 'USDNOK', 'EURCHF', 'USDCHF', 'USDCAD',
                          'NZDUSD', 'GBPUSD', 'AUDUSD', 'USDSGD', 'USDTHB',
                          'GBPCAD', 'EURCAD', 'EURJPY', 'EURGBP', 'GBPCHF',
                          'USDINR', 'USDIDR', 'USDTWD', 'USDKRW']},
        "strategy_module": "src.technicals",
        "strategy_name": "signalBollingerBand",
        "save_path": "src/pools/BollingerBand.csv",
        "params": {
            "length": [10, 15, 20, 40],
            "width": [0.0, 0.5, 1.0, 1.5, 2.0],
        }
    }
    strategy_pool_generator(pool_setting)


def Reversion():
    pool_setting = {
        "symbol": {"FX": ['AUDUSD', 'EURUSD', 'GBPUSD', 'NZDUSD', 'USDCAD',
                          'USDCHF', 'USDIDR', 'USDINR', 'USDJPY', 'USDKRW',
                          'USDNOK', 'USDSEK', 'USDSGD', 'USDTHB', 'USDTWD']},
        "strategy_module": "src.reversion",
        "strategy_name": "signalReversion",
        "save_path": "src/pools/Reversion.csv",
        "params": {
            "alpha": [0.05, 0.1, 0.4],
            "W": [10.0, 20.0, 30.0],
        }
    }
    strategy_pool_generator(pool_setting)


def Momentum():
    pool_setting = {
        "symbol": {"FX": ['EURUSD', 'USDJPY', 'NZDJPY', 'NOKSEK', 'GBPJPY',
                          'CADJPY', 'CADCHF', 'AUDNZD', 'AUDJPY', 'AUDCAD',
                          'USDSEK', 'USDNOK', 'EURCHF', 'USDCHF', 'USDCAD',
                          'NZDUSD', 'GBPUSD', 'AUDUSD', 'USDSGD', 'USDTHB',
                          'GBPCAD', 'EURCAD', 'EURJPY', 'EURGBP', 'GBPCHF',
                          'USDINR', 'USDIDR', 'USDTWD', 'USDKRW']},
        "strategy_module": "src.momentum",
        "strategy_name": "signalMomentum",
        "save_path": "src/pools/Momentum.csv",
        "params": {
            "rank_period": [75.0, 85.0, 95.0, 105.0, 115.0, 125.0],
            "mom_window": [20.0, 30.0, 40.0, 50.0, 60.0, 70.0],
        }
    }
    strategy_pool_generator(pool_setting)


def DBMomentum():
    pool_setting = {
        "symbol": {"FX": ['AUDUSD', 'EURUSD', 'GBPUSD', 'NZDUSD', 'USDCAD',
                          'USDCHF', 'USDIDR', 'USDINR', 'USDJPY', 'USDKRW',
                          'USDNOK', 'USDSEK', 'USDSGD', 'USDTHB', 'USDTWD']},
        "strategy_module": "src.dbresearch",
        "strategy_name": "signalMomentum",
        "save_path": "src/pools/DB/Momentum.csv",
        "params": {
        }
    }
    strategy_pool_generator(pool_setting)


def DBCarry():
    pool_setting = {
        "symbol": {"FX": ['AUDUSD', 'EURUSD', 'GBPUSD', 'NZDUSD', 'USDCAD',
                          'USDCHF', 'USDJPY', 'USDNOK', 'USDSEK', 'USDSGD',
                          'USDTHB']},
        "strategy_module": "src.dbresearch",
        "strategy_name": "signalCarry",
        "save_path": "src/pools/DB/Carry.csv",
        "params": {
            "smooth_length": [5.0, 10.0, 20.0, 40.0, 120.0]
        }
    }
    strategy_pool_generator(pool_setting)


def DBCFTCMomentum():
    pool_setting = {
        "symbol": {"FX": ['AUDUSD', 'EURUSD', 'GBPUSD', 'NZDUSD', 'USDCAD',
                          'USDCHF', 'USDJPY']},
        "strategy_module": "src.dbresearch",
        "strategy_name": "signalCFTCMomentum",
        "save_path": "src/pools/DB/CFTCMomentum.csv",
        "params": {
        }
    }
    strategy_pool_generator(pool_setting)


def DBCFTCReversal():
    pool_setting = {
        "symbol": {"FX": ['AUDUSD', 'EURUSD', 'GBPUSD', 'NZDUSD', 'USDCAD',
                          'USDCHF', 'USDJPY']},
        "strategy_module": "src.dbresearch",
        "strategy_name": "signalCFTCReversal",
        "save_path": "src/pools/DB/CFTCReversal.csv",
        "params": {
        }
    }
    strategy_pool_generator(pool_setting)


def DBMomentumSpillOver():
    pool_setting = {
        "symbol": {"FX": ['AUDUSD', 'EURUSD', 'GBPUSD', 'NZDUSD', 'USDCAD',
                          'USDCHF', 'USDJPY', 'USDNOK', 'USDSEK', 'USDSGD',
                          'USDTHB']},
        "strategy_module": "src.dbresearch",
        "strategy_name": "signalMomentumSpillOver",
        "save_path": "src/pools/DB/MomentumSpillOver.csv",
        "params": {
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
    Reversion()
    Momentum()
    DBMomentum()
    DBCarry()
    DBCFTCMomentum()
    DBCFTCReversal()
    DBMomentumSpillOver()
