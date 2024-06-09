from inputs.sampleBase import *

def parseTestCaseString(string):
    contents = string.split(";")
    s = AqumonSample()
    s.symbol = np.asarray(contents[0].split(','))
    s.targetWeight = np.fromstring(contents[1], dtype=np.float, sep=',')
    s.region = RegionEnum[contents[2][:2]]
    s.currency = CurrencyEnum[contents[2]]
    s.brokerType = BrokerTypeEnum[contents[3]]
    s.deltaAmount = np.float(contents[5])
    s.availableCashAmount = np.float(contents[6])
    s.mvTradingHalt = np.float('0')
    s.tradingHaltDrift = np.float('0')
    s.currentPosition = np.fromstring(contents[9], dtype=np.float, sep=',')
    s.filledQuantity = np.fromstring(contents[10], dtype=int, sep=',')
    s.filledAvgPrice = np.fromstring(contents[11], dtype=np.float, sep=',')
    s.filledAmount = np.fromstring(contents[12], dtype=np.float, sep=',')
    s.longVolumeHistory = np.fromstring(contents[13], dtype=int, sep=',')
    s.shortVolumeToday = np.fromstring(contents[14], dtype=int, sep=',')
    s.lotSize = np.fromstring(contents[15], dtype=int, sep=',')
    s.askPrice = np.fromstring(contents[16], dtype=np.float, sep=',')
    return s

class SampleHKETF1(AqumonSample):
    symbol = np.asarray(['3169.HK', '2813.HK', '3141.HK'])
    targetWeight = np.fromstring('0.50 0.095 0.40', dtype=np.float, sep=' ')
    region = RegionEnum.HK
    currency = CurrencyEnum.HKD
    brokerType = BrokerTypeEnum.AYERS
    deltaAmount = np.float('-50000')
    availableCashAmount = np.float('500')
    mvTradingHalt = np.float('0')
    tradingHaltDrift = np.float('0')
    currentPosition = np.fromstring('3200 50 1200', dtype=int, sep=' ')
    filledQuantity = np.fromstring('0 0 0', dtype=int, sep=' ')
    filledAvgPrice = np.fromstring('11 130 19', dtype=int, sep=' ')
    filledAmount = filledQuantity * filledAvgPrice
    longVolumeHistory = np.fromstring('0 0 0', dtype=int, sep=' ')
    shortVolumeToday = np.fromstring('0 0 0', dtype=int, sep=' ')
    lotSize = np.fromstring('100 10 200', dtype=int, sep=' ')
    askPrice = np.fromstring('10 120 20', dtype=np.float, sep=' ')

class SampleHKETF2(AqumonSample):
    symbol = np.asarray(['3169.HK', '2813.HK', '3141.HK'])
    targetWeight = np.fromstring('0.05 0.45 0.40', dtype=np.float, sep=' ')
    region = RegionEnum.HK
    currency = CurrencyEnum.HKD
    brokerType = BrokerTypeEnum.AYERS
    deltaAmount = np.float('0')
    availableCashAmount = np.float('500')
    mvTradingHalt = np.float('0')
    tradingHaltDrift = np.float('0')
    currentPosition = np.fromstring('3000 50 1200', dtype=int, sep=' ')
    filledQuantity = np.fromstring('0 0 0', dtype=int, sep=' ')
    filledAvgPrice = np.fromstring('11 130 19', dtype=int, sep=' ')
    filledAmount = filledQuantity * filledAvgPrice
    longVolumeHistory = np.fromstring('0 0 0', dtype=int, sep=' ')
    shortVolumeToday = np.fromstring('0 0 0', dtype=int, sep=' ')
    lotSize = np.fromstring('100 10 200', dtype=int, sep=' ')
    askPrice = np.fromstring('10 120 20', dtype=np.float, sep=' ')

class SampleAqumon3(AqumonSample):
    symbol = np.asarray(['3169.HK', '2813.HK', '3141.HK'])
    targetWeight = np.fromstring('0.3 0.2 0.40', dtype=np.float, sep=' ')
    region = RegionEnum.HK
    currency = CurrencyEnum.HKD
    brokerType = BrokerTypeEnum.AYERS
    deltaAmount = np.float('20000')
    availableCashAmount = np.float('500')
    mvTradingHalt = np.float('0')
    tradingHaltDrift = np.float('0')
    currentPosition = np.fromstring('3200 50 1200', dtype=int, sep=' ')
    filledQuantity = np.fromstring('0 0 0', dtype=int, sep=' ')
    filledAvgPrice = np.fromstring('0 0 0', dtype=int, sep=' ')
    filledAmount = filledQuantity * filledAvgPrice
    longVolumeHistory = np.fromstring('0 0 0', dtype=int, sep=' ')
    shortVolumeToday = np.fromstring('0 0 0', dtype=int, sep=' ')
    lotSize = np.fromstring('100 10 200', dtype=int, sep=' ')
    askPrice = np.fromstring('12 121 21', dtype=np.float, sep=' ')

class SampleAqumon4(AqumonSample):
    symbol = np.asarray(['AAA'])
    targetWeight = np.fromstring('1', dtype=np.float, sep=' ')
    region = RegionEnum.HK
    currency = CurrencyEnum.HKD
    brokerType = BrokerTypeEnum.AYERS
    deltaAmount = np.float('40770.74706241417')
    availableCashAmount = np.float('0')
    mvTradingHalt = np.float('0')
    tradingHaltDrift = np.float('0')
    currentPosition = np.fromstring('89', dtype=int, sep=' ')
    filledQuantity = np.fromstring('-8', dtype=int, sep=' ')
    filledAvgPrice = np.fromstring('106.23851439214978', dtype=np.float, sep=' ')
    filledAmount = filledQuantity * filledAvgPrice
    longVolumeHistory = np.fromstring('97', dtype=int, sep=' ')
    shortVolumeToday = np.fromstring('8', dtype=int, sep=' ')
    lotSize = np.fromstring('36', dtype=int, sep=' ')
    askPrice = np.fromstring('106.15990727619216', dtype=np.float, sep=' ')

class SampleAqumon5(AqumonSample):
    symbol = np.asarray(['AAA','b','c','d','e','f'])
    targetWeight = np.fromstring('0.037688559393668795 0.17142119339375841 0.04789948748095217 0.1484787190811854 0.45799934113890006 0.1365126995115353', dtype=np.float, sep=' ')
    region = RegionEnum.HK
    currency = CurrencyEnum.HKD
    brokerType = BrokerTypeEnum.AYERS
    deltaAmount = np.float('45762.60341739366')
    availableCashAmount = np.float('0')
    mvTradingHalt = np.float('0')
    tradingHaltDrift = np.float('0')
    currentPosition = np.fromstring('0 0 0 0 0 0', dtype=int, sep=' ')
    filledQuantity = np.fromstring('0 0 0 0 0 0', dtype=int, sep=' ')
    filledAvgPrice = np.fromstring('0 0 0 0 0 0', dtype=int, sep=' ')
    filledAmount = filledQuantity * filledAvgPrice
    longVolumeHistory = np.fromstring('0 0 0 0 0 0', dtype=int, sep=' ')
    shortVolumeToday = np.fromstring('0 0 0 0 0 0', dtype=int, sep=' ')
    lotSize = np.fromstring('58 99 32 39 76 80', dtype=int, sep=' ')
    askPrice = np.fromstring('97.27329938926417 45.87067226560404 118.72399382629237 99.972218653243 73.57889297703338 40.5283299190239', dtype=np.float, sep=' ')

class SampleAqumon6(AqumonSample):
    symbol = np.asarray(["1201001287","1201001397","1201001884","1201003253","1201006116","1201008001","1201009196","1201017423","1201020862"])
    targetWeight = np.fromstring('0.173257 0.235815 0.093337 0.062432 0.133407 0.125699 0.113129 0.038 0.024925', dtype=np.float, sep=' ')
    region = RegionEnum.HK
    currency = CurrencyEnum.HKD
    brokerType = BrokerTypeEnum.AYERS
    deltaAmount = np.float('0')
    availableCashAmount = np.float('642.46')
    mvTradingHalt = np.float('0')
    tradingHaltDrift = np.float('0')
    currentPosition = np.fromstring('1000 800 70 600 2 200 400 0 200', dtype=int, sep=' ')
    filledQuantity = np.zeros(9)
    filledAvgPrice = np.zeros(9)
    filledAmount = np.fromstring('0 0 0 0 0 0 0 0 0', dtype=int, sep=' ')
    longVolumeHistory = currentPosition
    shortVolumeToday = np.zeros(9)
    lotSize = np.fromstring('200 200 10 200 1 100 200 100 200', dtype=int, sep=' ')
    askPrice = np.fromstring('16.55 26.06 124.8 12.96 7962.05 70.8 37.36 43.94 11.89', dtype=np.float, sep=' ')

class SampleAqumon7(AqumonSample):
    symbol = np.asarray(["1201001287","1201001397","1201001884","1201003253","1201006116","1201008001","1201009196","1201017423","1201020862"])
    targetWeight = np.fromstring('0.173257 0.235815 0.093337 0.062432 0.133407 0.125699 0.113129 0.038 0.024925', dtype=np.float, sep=' ')
    region = RegionEnum.HK
    currency = CurrencyEnum.HKD
    brokerType = BrokerTypeEnum.AYERS
    deltaAmount = np.float('0')
    availableCashAmount = np.float('642.46')
    mvTradingHalt = np.float('0')
    tradingHaltDrift = np.float('0')
    currentPosition = np.fromstring('1000 800 70 600 2 200 400 0 200', dtype=int, sep=' ')
    filledQuantity = np.zeros(9)
    filledAvgPrice = np.zeros(9)
    filledAmount = np.fromstring('0 0 0 0 0 0 0 0 0', dtype=int, sep=' ')
    longVolumeHistory = currentPosition
    shortVolumeToday = np.zeros(9)
    lotSize = np.fromstring('200 200 10 200 1 100 200 100 200', dtype=int, sep=' ')
    askPrice = np.fromstring('16.3 26.28 125.05 13.14 7963.75 69.56 35.52 44.12 10.95', dtype=np.float, sep=' ')

class SampleAqumon8(AqumonSample):
    symbol = np.asarray(["1201001287","1201001397","1201001884","1201008001","1201009196","1201017423"])
    targetWeight = np.fromstring('0.259937 0.244647 0.140063 0.13966 0.125694 0.09', dtype=np.float, sep=' ')
    region = RegionEnum.HK
    currency = CurrencyEnum.HKD
    brokerType = BrokerTypeEnum.AYERS
    deltaAmount = np.float('50000')
    availableCashAmount = np.float('0')
    mvTradingHalt = np.float('0')
    tradingHaltDrift = np.float('0')
    currentPosition = np.fromstring('0 0 0 0 0 0', dtype=int, sep=' ')
    filledQuantity = np.zeros(6)
    filledAvgPrice = np.zeros(6)
    filledAmount = np.fromstring('0 0 0 0 0 0', dtype=int, sep=' ')
    longVolumeHistory = currentPosition
    shortVolumeToday = np.zeros(6)
    lotSize = np.fromstring('200 200 10 100 200 100', dtype=int, sep=' ')
    askPrice = np.fromstring('16.42 26.9 125.6 72.42 38.66 44.62', dtype=np.float, sep=' ')

class SampleAqumon9(AqumonSample):
    symbol = np.asarray(["1105000777","1105000992","1105001093","1105001810","1105002382","1105009618"])
    targetWeight = np.fromstring('0.0869 0.1464 0.1586 0.189682 0.1697 0.248718', dtype=np.float, sep=' ')
    region = RegionEnum.HK
    currency = CurrencyEnum.HKD
    brokerType = BrokerTypeEnum.AYERS
    deltaAmount = np.float('100000')
    availableCashAmount = np.float('0')
    mvTradingHalt = np.float('0')
    tradingHaltDrift = np.float('0')
    currentPosition = np.fromstring('500 1000 2000 0 0 0', dtype=int, sep=' ')
    filledQuantity = np.fromstring('500 1000 2000 0 0 0', dtype=int, sep=' ')
    filledAvgPrice = np.fromstring('19.5 9.77 8.5 30.1 220 388', dtype=np.float, sep=' ')
    filledAmount = np.fromstring('500 1000 2000 0 0 0', dtype=int, sep=' ') * np.fromstring('19.5 9.77 8.5 30.1 220 388', dtype=np.float, sep=' ')
    longVolumeHistory = np.fromstring('500 1000 2000 0 0 0', dtype=int, sep=' ')
    shortVolumeToday = np.zeros(6)
    lotSize = np.fromstring('500 2000 2000 200 100 50', dtype=int, sep=' ')
    askPrice = np.fromstring('19.5 9.77 8.5 30.1 220 388', dtype=np.float, sep=' ')
    oddLotTrade = True
    oddLotFreezedInOddUnits = lotSize

class SampleAqumon10(AqumonSample):
    symbol = np.asarray(["1105000777","1105000992","1105001093","1105001810","1105002382","1105009618"])
    targetWeight = np.fromstring('0.0925 0.1980 0.1654 0.1391 0.214 0.191', dtype=np.float, sep=' ')
    region = RegionEnum.HK
    currency = CurrencyEnum.HKD
    brokerType = BrokerTypeEnum.AYERS
    deltaAmount = np.float('100500')
    availableCashAmount = np.float('0')
    mvTradingHalt = np.float('0')
    tradingHaltDrift = np.float('0')
    currentPosition = np.zeros(6)
    filledQuantity = np.zeros(6)
    filledAvgPrice = np.zeros(6)
    filledAmount = np.zeros(6)
    longVolumeHistory = np.zeros(6)
    shortVolumeToday = np.zeros(6)
    lotSize = np.fromstring('500 2000 2000 200 100 50', dtype=int, sep=' ')
    askPrice = np.fromstring('19.5 9.77 8.5 30.1 220 388', dtype=np.float, sep=' ')
    oddLotTrade = False
    oddLotFreezedInOddUnits = lotSize