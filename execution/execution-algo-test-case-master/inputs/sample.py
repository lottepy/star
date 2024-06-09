import numpy as np
from constants.type import *
from inputs.sampleBase import *
from inputs.sampleAqumon import *

class SamplePortfolio3(PortfolioSample):
    symbol = np.asarray(['0005.HK', '0700.HK', '3333.HK', '0981.HK', '0388.HK'])
    targetWeight = np.fromstring('0.1 0.1 0.25 0.25 0.3', dtype=np.float, sep=' ')
    region = RegionEnum.HK
    currency = CurrencyEnum.HKD
    brokerType = BrokerTypeEnum.IB
    targetPV = 1000000
    mvTradingHalt = np.float('0')
    tradingHaltDrift = np.float('0')
    currentPosition = np.fromstring('0 0 0 0 0', dtype=int, sep=' ')
    filledQuantity = np.fromstring('0 0 0 0 0', dtype=int, sep=' ')
    filledAvgPrice = np.fromstring('0 0 0 0 0', dtype=int, sep=' ')
    filledAmount = filledQuantity * filledAvgPrice
    longVolumeHistory = np.fromstring('0 0 0 0 0', dtype=int, sep=' ')
    shortVolumeToday = np.fromstring('0 0 0 0 0', dtype=int, sep=' ')
    lotSize = np.fromstring('400 100 1000 500 100', dtype=int, sep=' ')
    askPrice = np.fromstring('40.275 417.1 13.12 15.18 246.3', dtype=np.float, sep=' ')

class BOCISample1(BOCISample):
    symbol = ['AGG', 'JPST']
    lotSize = np.fromstring('1 1', dtype=np.int, sep=' ')
    askPrice = np.fromstring('118 50.78', dtype=np.float, sep=' ')
    deltaAmount = np.float('10000')
    availableCashAmount = np.float('500')
    targetWeight = np.fromstring('0.5 0.5', dtype=np.float, sep=' ')
    region = RegionEnum.US
    currency = CurrencyEnum.USD
    lastPrice = np.fromstring('117 50.78', dtype=np.float, sep=' ')
    currentPosition = np.fromstring('0 0', dtype=int, sep=' ')
    priceLowerBound: np.ndarray = -np.Infinity * np.ones_like(askPrice)
    priceUpperBound: np.ndarray = np.Infinity * np.ones_like(askPrice)
    quantityLowerBound: np.ndarray = -np.Infinity * np.ones_like(askPrice) 
    quantityUpperBound: np.ndarray = np.Infinity * np.ones_like(askPrice)

class BOCISample2(BOCISample):
    currentPosition = np.fromstring('11 9 11 7 10', dtype=int, sep=' ')
    lastClose = np.fromstring('200 200 200 200 200', dtype=np.float, sep=' ')
    symbol = ['A', 'B', 'C', 'D', 'E']
    lotSize = np.fromstring('1 1 1 1 1', dtype=np.int, sep=' ')
    askPrice = lastClose * (1 + np.random.rand(5) * 0.1 - 0.05)
    deltaAmount = np.float('0')
    availableCashAmount = np.float('0')
    targetWeight = np.fromstring('0.2 0.2 0.2 0.2 0.2', dtype=np.float, sep=' ')
    region = RegionEnum.US
    currency = CurrencyEnum.USD
    lastPrice = askPrice
    priceLowerBound = lastClose * 0.9
    priceUpperBound = lastClose * 1.1
    quantityLowerBound = (np.floor(np.sum(currentPosition * lastClose) * targetWeight / priceUpperBound)).astype(int) - currentPosition
    quantityUpperBound = (np.floor(np.sum(currentPosition * lastClose) * targetWeight / priceLowerBound)).astype(int) - currentPosition
    filledQuantity = np.zeros_like(lotSize)

class BOCISample3(BOCISample2):
    quantityLowerBound = [0 if abs(l) <= abs(h) and np.sign(l) != np.sign(h) else l for (l, h) in zip(BOCISample2.quantityLowerBound, BOCISample2.quantityUpperBound)]
    quantityUpperBound = [0 if abs(l) > abs(h) and np.sign(l) != np.sign(h) else h for (l, h) in zip(BOCISample2.quantityLowerBound, BOCISample2.quantityUpperBound)]

class BOCISample4(BOCISample):
    currentPosition = np.fromstring('11 9 11 7 10', dtype=int, sep=' ')
    lastClose = np.fromstring('200 200 200 200 200', dtype=np.float, sep=' ')
    symbol = ['A', 'B', 'C', 'D', 'E']
    lotSize = np.fromstring('1 1 1 1 1', dtype=np.int, sep=' ')
    askPrice = lastClose * (1 + np.random.rand(5) * 0.2 - 0.1)
    deltaAmount = np.float('0')
    availableCashAmount = np.float('0')
    targetWeight = np.fromstring('0.2 0.2 0.2 0.2 0.2', dtype=np.float, sep=' ')
    region = RegionEnum.US
    currency = CurrencyEnum.USD
    lastPrice = askPrice
    priceLowerBound = lastClose * 0.9
    priceUpperBound = lastClose * 1.1
    quantityLowerBound = (np.floor(np.sum(currentPosition * lastClose) * targetWeight / priceUpperBound)).astype(int) - currentPosition
    quantityUpperBound = (np.floor(np.sum(currentPosition * lastClose) * targetWeight / priceLowerBound)).astype(int) - currentPosition
    filledQuantity = np.zeros_like(lotSize)

class BOCISample5(BOCISample4):
    quantityLowerBound = [0 if abs(l) <= abs(h) and np.sign(l) != np.sign(h) else l for (l, h) in zip(BOCISample4.quantityLowerBound, BOCISample4.quantityUpperBound)]
    quantityUpperBound = [0 if abs(l) > abs(h) and np.sign(l) != np.sign(h) else h for (l, h) in zip(BOCISample4.quantityLowerBound, BOCISample4.quantityUpperBound)]

class BOCISample6(BOCISample):
    currentPosition = np.fromstring('10 10 10 10 10', dtype=int, sep=' ')
    lastClose = np.fromstring('190 210 180 220 200', dtype=np.float, sep=' ')
    symbol = ['A', 'B', 'C', 'D', 'E']
    lotSize = np.fromstring('1 1 1 1 1', dtype=np.int, sep=' ')
    askPrice = np.fromstring('209 191 180 220 200', dtype=np.float, sep=' ')
    deltaAmount = np.float('0')
    availableCashAmount = np.float('0')
    targetWeight = np.fromstring('0.2 0.2 0.2 0.2 0.2', dtype=np.float, sep=' ')
    region = RegionEnum.US
    currency = CurrencyEnum.USD
    lastPrice = askPrice
    priceLowerBound = lastClose * 0.9
    priceUpperBound = lastClose * 1.1
    quantityLowerBound = (np.floor(np.sum(currentPosition * lastClose) * targetWeight / priceUpperBound)).astype(int) - currentPosition
    quantityUpperBound = (np.floor(np.sum(currentPosition * lastClose) * targetWeight / priceLowerBound)).astype(int) - currentPosition
    filledQuantity = np.zeros_like(lotSize)

class BOCISample7(BOCISample6):
    quantityLowerBound = [0 if abs(l) <= abs(h) and np.sign(l) != np.sign(h) else l for (l, h) in zip(BOCISample6.quantityLowerBound, BOCISample6.quantityUpperBound)]
    quantityUpperBound = [0 if abs(l) > abs(h) and np.sign(l) != np.sign(h) else h for (l, h) in zip(BOCISample6.quantityLowerBound, BOCISample6.quantityUpperBound)]

class BOCISample8(BOCISample):
    currentPosition = np.fromstring('1000 1000', dtype=int, sep=' ')
    lastClose = np.fromstring('106 964', dtype=np.float, sep=' ')
    symbol = ['A', 'B']
    lotSize = np.fromstring('1 1', dtype=np.int, sep=' ')
    askPrice = np.fromstring('116 858.6', dtype=np.float, sep=' ')
    deltaAmount = np.float('0')
    availableCashAmount = np.float('0')
    targetWeight = np.fromstring('0.1 0.9', dtype=np.float, sep=' ')
    region = RegionEnum.US
    currency = CurrencyEnum.USD
    lastPrice = askPrice
    priceLowerBound = lastClose * 0.9
    priceUpperBound = lastClose * 1.1
    quantityLowerBound = (np.floor(np.sum(currentPosition * lastClose) * targetWeight / priceUpperBound)).astype(int) - currentPosition
    quantityUpperBound = (np.floor(np.sum(currentPosition * lastClose) * targetWeight / priceLowerBound)).astype(int) - currentPosition
    filledQuantity = np.zeros_like(lotSize)

class BOCISample9(BOCISample8):
    quantityLowerBound = [0 if abs(l) <= abs(h) and np.sign(l) != np.sign(h) else l for (l, h) in zip(BOCISample8.quantityLowerBound, BOCISample8.quantityUpperBound)]
    quantityUpperBound = [0 if abs(l) > abs(h) and np.sign(l) != np.sign(h) else h for (l, h) in zip(BOCISample8.quantityLowerBound, BOCISample8.quantityUpperBound)]