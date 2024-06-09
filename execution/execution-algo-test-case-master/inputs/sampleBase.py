from constants.type import *
import numpy as np

class Sample:
    symbol: np.ndarray = None
    lotSize: np.ndarray = None
    askPrice: np.ndarray = None
    currentPosition: np.ndarray = np.zeros_like(symbol)
    filledQuantity: np.ndarray = np.zeros_like(symbol)
    filledAvgPrice: np.ndarray = np.zeros_like(symbol)
    filledAmount: np.ndarray = filledQuantity * filledAvgPrice
    longVolumeHistory: np.ndarray = np.zeros_like(symbol)
    shortVolumeToday: np.ndarray = np.zeros_like(symbol)
    filledQuantity: np.ndarray = np.zeros_like(symbol)
    mvTradingHalt = np.float('0')
    tradingHaltDrift = np.float('0')
    oddLotTrade: bool = False
    oddLotFreezedInOddUnits: np.ndarray = np.zeros_like(symbol)

class AqumonSample(Sample):
    pass

class PortfolioSample(Sample):
    targetPV: np.float = None

class BOCISample(Sample):
    targetWeight: np.ndarray = None
    region: RegionEnum = None
    currency: CurrencyEnum = None
    brokerType: BrokerTypeEnum = BrokerTypeEnum.BOCI
    priceLowerBound: np.ndarray = None
    priceUpperBound: np.ndarray = None
    quantityLowerBound: np.ndarray = None
    quantityUpperBound: np.ndarray = None
    lastPrice: np.ndarray = None