from enum import Enum

MAX_SELL_AMOUNT = 100000000

GET_SYMBOL_TYPE_BATCH = 50

RISK_RETRY_NO = 1
PMS_RETRY_NO = 2

REQUEST_TIMEOUT = 600

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

class PmsRejectReason(Enum):
    STOP_TRADE = "ST"
    STOP_BUY = "SB"
    STOP_SELL = "SS"
    LARGE_THAN_SELL_LIMIT = "LS"
    NAN_ORDER = "NA"
    NO_CASH = "NC"

class TradeStatus(Enum):
    NORMAL = 1
    STOP = 2
    STOPBUY = 3
    STOPSELL = 4



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
    current_tradable_data = "current_tradable_data"
    secondary_data = "secondary_data"
    ref_data = "ref_data"
    signal_remark = "signal_remark"
    reload_current_data = "reload_current_data"
    reload_order_feedback = "reload_order_feedback"
    fire_order = "fire_order"

class Time(Enum):
    """ time array的各列名称 """
    TIMESTAMP = 0
    END_TIMESTAMP = 1
    WEEKDAY = 2
    YEAR = 3
    MONTH = 4
    DAY = 5
    HOUR = 6
    MINUTE = 7
    SECOND = 8

class CaptureType(Enum):
    current_data = 1
    current_fx_data = 2
    current_rate_data = 3


class Symbol(Enum):
    """Symbol tuple 的各列名称"""
    MNEMONIC = 0
    SYMBOL = 1
    DM_SYMBOL = 2
    FREQUENCY = 3


class Frequency(Enum):
    """Frequcy 对应表"""
    DAILY = 0
    HOURLY = 1
    MINUTE = 2

class TerminationType(Enum):
    END = "END"
    END_load_data = "END_load_data"
    END_load_current_data = "END_load_current_data"

class PmsTaskType(Enum):
    order_feedback = "order_feedback"
    reset_trade_status = "reset_trade_status"
    update_spot = "update_spot"
    update_dividends = "update_dividends"
    sync_cash = "sync_cash"
    sync_holding = "sync_holding"
    sync_current_data = "sync_current_data"
    sync_current_fx_data = "sync_current_fx_data"

class OrderRouterType(Enum):
    TestOrderRouter = "TestOrderRouter"
    CsvOrderRouter = "CsvOrderRouter"
    PortfolioTaskEngineOrderRouter = "PortfolioTaskEngineOrderRouter"