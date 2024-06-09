from enum import Enum

MAX_SELL_AMOUNT = 100000000

GET_SYMBOL_TYPE_BATCH = 50

RISK_RETRY_NO = 2
PMS_RETRY_NO = 2

class InstrumentType(Enum):
    FX = 1
    HK_STOCK = 2
    CN_STOCK = 3
    US_STOCK = 4
    FUTURES = 5

class OrderStatus(Enum):
    FILLED = 1
    PARTLY_FILLED = 2
    REJECTED = 3

class PmsStatus(Enum):
    LEGAL = 1
    ILLEGAL = 2



class OrderFeedback(Enum):
    transaction = 0
    current_data = 1
    current_fx_data = 2
    commission_fee = 3
    order_status = 4
    timestamps = 5

class DataType(Enum):
    current_data = "current_data"
    current_fx_data = "current_fx_data"
    current_rate_data = "current_rate_data"

class Metrics(Enum):
    holding_period_return = 0
    sharpe_ratio = 1
    MDD = 2
    ADD = 3
    rate_of_return = 4
    number_of_trades = 5
    return_from_longs = 6
    return_from_shorts = 7
    ratio_of_longs = 8
    hit_ratio = 9
    profit_rate = 10
    return_from_hits = 11
    return_from_misses = 12
    return_from_profit_days = 13
    return_from_loss_days = 14

class Rank_metrics(Enum):
    Expected_Return = 0
    Rate_of_Return = 1

class Fake:
    def __init__(self):
        pass
