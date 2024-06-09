from core.targetWeightToDeltaPosition import TargetWeightToDeltaPositionEquityCalculator, TradingInfo
from inputs.sample import Sample
from constants.type import *
from utils.printing import *
import numpy as np

class SubTask:
    DRIFT_THRESHOLD = None
    SUBTASK_TYPE = None
    sample: Sample

    def __init__(self, sample: Sample):
        self.sample = sample
        self.nSecurities = len(self.sample.symbol)
        self._checkInheritanceAttrs()
    
    def _checkInheritanceAttrs(self):
        requiredAttrs = ['SUBTASK_TYPE', 'DRIFT_THRESHOLD']
        for attr in requiredAttrs:
            if not attr in self.__class__.__dict__.keys():
                PANIC(f"You have to set the value for \"{attr}\" when writing a new SubTask")

    def calcPV(self) -> np.float:
        raise NotImplementedError

    def calcCommission(self, estTurnover: np.float) -> np.float:
        raise NotImplementedError
    
    def calc(self) -> np.ndarray:
        sample = self.sample
        print("symbol = {}".format(sample.symbol))
        tradingInfo = TradingInfo(filledQuantity=sample.filledQuantity, currentPosition=sample.currentPosition, longVolumeHistory=sample.longVolumeHistory, shortVolumeToday=sample.shortVolumeToday)
        targetPV = self.calcPV()
        print("targetPV = {}".format(targetPV))
        print("【开始做预估】")
        estCalculator = TargetWeightToDeltaPositionEquityCalculator(targetPV, 0, 0, self.DRIFT_THRESHOLD, sample.mvTradingHalt or 0, sample.tradingHaltDrift, sample.symbol, sample.targetWeight, sample.lotSize, sample.askPrice, tradingInfo, sample.brokerType, self.SUBTASK_TYPE, sample.region, sample.currentPosition, 1, 1, -1, lambda _: 0, sample.oddLotTrade, sample.oddLotFreezedInOddUnits)
        estTurnover = estCalculator.calcEstTurnover(estCalculator.calc(), sample.askPrice)
        print("estTurnover = {}".format(estTurnover))
        print("【开始正式计算】")
        estCommissionAndTax = self.calcCommission(estTurnover)
        print("estCommissionAndTax = {}".format(estCommissionAndTax))
        cashBuffer = estCalculator.calcCashBuffer(estTurnover)
        print("cashBuffer = {}".format(cashBuffer))
        self.calculator = TargetWeightToDeltaPositionEquityCalculator(targetPV, estCommissionAndTax, cashBuffer, self.DRIFT_THRESHOLD, sample.mvTradingHalt or 0, sample.tradingHaltDrift, sample.symbol, sample.targetWeight, sample.lotSize, sample.askPrice, tradingInfo, sample.brokerType, self.SUBTASK_TYPE, sample.region, sample.currentPosition, 1, 1, -1, self.calcCommission, sample.oddLotTrade, sample.oddLotFreezedInOddUnits)
        deltaPosition = self.calculator.calc()
        print("【计算结果】")
        for i in range(self.nSecurities):
            print("{}:\tcurrent: {}\tdelta: {}".format(sample.symbol[i], sample.currentPosition[i], deltaPosition[i]))
        if 'calcDrift' in self.__class__.__dict__.keys():
            drift = self.calcDrift(deltaPosition)
            print(f"drift:\t{drift*100}%")
        return deltaPosition