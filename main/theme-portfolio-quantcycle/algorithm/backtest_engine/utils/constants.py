from enum import Enum  # numba支持emum的数据类型


class Data(Enum):
    """ data array的各列名称 """
    OPEN = 0
    HIGH = 1
    LOW = 2
    CLOSE = 3
    VOLUME = 4

class Rate(Enum):
    """ rate array的各列名称 """
    BID = 0
    ASK = 1
    LAST = 2


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


class TimeFreq(Enum):
    MINLY = 60
    HOURLY = 3600
    DAILY = 3600 * 24

class InstrumentType(Enum):
    FX = 1
    HK_STOCK = 2
    CN_STOCK = 3
    US_STOCK = 4
    FUTURE = 5

class StockDataType(Enum):
    UNADJUSTED = 0
    BACKWARD_ADJUSTED = 1
    FORWARD_ADJUSTED = 2

SAVE_DATA_TIME = 3
WINDOW_TIME = 3
REF_WINDOW_TIME = 3
GET_SYMBOL_TYPE_BATCH = 50
DATA_LOADER_DAILY_DATA_BATCH = 50
