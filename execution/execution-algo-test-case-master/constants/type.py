from enum import Enum

class SubTaskTypeEnum(str, Enum):
    AQUMON = 'AQUMON'
    PORTFOLIO = 'PORTFOLIO'
    BOCI = 'BOCI'

class BrokerTypeEnum(str, Enum):
    IB = 'IB'
    AYERS = 'AYERS'
    BOCI = 'BOCI'

class RegionEnum(str, Enum):
    CN = 'CN'
    HK = 'HK'
    US = 'US'
    SG = 'SG'

class CurrencyEnum(str, Enum):
    CNH = 'CNH'
    USD = 'USD'
    HKD = 'HKD'