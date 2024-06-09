from typing import Callable
import numpy as np
from constants.type import *
from utils.printing import *

class TradingInfo:
    filledQuantity: np.ndarray
    currentPosition: np.ndarray
    longVolumeHistory: np.ndarray
    shortVolumeToday: np.ndarray
    def __init__(self, **attrs):
        self.__dict__.update(**attrs)
    def __getattr__(self, attr):
        return self.__dict__.get(attr, None)

class TargetWeightToDeltaPositionEquityCalculator:
    def __init__(
        self,
        targetPV: np.float,
        estCommissionAndTax: np.float,
        cashBuffer: np.float,
        driftThreshold: np.float,
        mvTradingHalt: np.float,
        tradingHaltDrift: np.float,

        symbol: np.ndarray,
        targetWeight: np.ndarray,
        lotSize: np.ndarray,
        askPrice: np.ndarray,
        tradingInfo: TradingInfo,

        brokerType: BrokerTypeEnum,
        subTaskType: SubTaskTypeEnum,
        region: RegionEnum,
        currency: CurrencyEnum,

        currentBatchIndex: int,
        totalNumOfBatches: int,
        turnoverLimit: np.float,
        
        calcCommission: Callable = lambda _: 0, # 计算 commission 的公式，因为各个任务的公式都不一样，所以需要单独传进来
        oddLotTrade: bool = False, # 是否采用碎股下单模式
        oddLotFreezedInOddUnits: np.ndarray = np.asarray([], dtype=np.bool_), # 从 trading engine 请求回来的冻结住的股票的数目，单位是“1 碎股单位”
    ):
        """Convert a target_weight to delta position for equities.

        Args:
            targetPV (np.float): [description]
            estCommissionAndTax (np.float): [description]
            cashBuffer (np.float): 如果 < 0，则 cashBuffer 让逻辑自动计算；如果 >= 0，则为指定 cashBuffer
            symbol (np.ndarray): [description]
            targetWeight (np.ndarray): [description]
            lotSize (np.ndarray): [description]
            askPrice (np.ndarray): [description]
        """
        self.targetPV = targetPV
        self.estCommissionAndTax = estCommissionAndTax
        self.cashBuffer = cashBuffer
        self.driftThreshold = driftThreshold
        self.mvTradingHalt = mvTradingHalt
        self.tradingHaltDrift = tradingHaltDrift

        self.symbol = symbol
        self.targetWeight = targetWeight
        self.lotSize = lotSize
        self.askPrice = askPrice
        self.filledQuantity = tradingInfo.filledQuantity
        self.currentPosition = tradingInfo.currentPosition
        self.longVolumeHistory = tradingInfo.longVolumeHistory
        self.shortVolumeToday = tradingInfo.shortVolumeToday

        self.brokerType = brokerType
        self.subTaskType = subTaskType
        self.region = region
        self.currency = currency

        self.currentBatchIndex = currentBatchIndex
        self.totalNumOfBatches = totalNumOfBatches
        self.turnoverLimit = turnoverLimit
        self.calcCommission = calcCommission
        self.oddLotTrade = oddLotTrade
        self.oddLotFreezedInOddUnits = oddLotFreezedInOddUnits
        self.oddLotUnit = 0.01 if region == RegionEnum.US else 1 # 一个碎股单位是多少股
    
    def calc(self):
        useSpecifiedCashBuffer = self.cashBuffer >= 0
        useSpecifiedEstCommissionAndTax = self.estCommissionAndTax >= 0
        self.cashBuffer = self.cashBuffer if useSpecifiedCashBuffer else self.calcCashBuffer()
        print("[CORE] cashBuffer = {}".format(self.cashBuffer))
        self.targetWeightPortfolioCash = self.calcTargetWeightPortfolioCash()
        print("[CORE] targetWeightPortfolioCash = {}".format(self.targetWeightPortfolioCash))
        self.pv = self.calcPV()
        print("[CORE] pv = {}".format(self.pv))
        targetPositionFractional = self.calcTargetPositionFractional()
        print("[CORE] targetPositionFractional = {}".format(targetPositionFractional))
        targetPosition = self.integerOptimization(targetPositionFractional)
        print("[CORE] targetPosition after integerOptimization = {}".format(targetPosition))
        estTurnover = self.calcEstTurnover(self.handleReverseOrder(targetPosition, 0) - self.currentPosition, self.askPrice)
        self.estCommissionAndTax = self.estCommissionAndTax if useSpecifiedEstCommissionAndTax else self.calcCommission(estTurnover)
        cashBufferForReverseOrderHandling = self.cashBuffer if useSpecifiedCashBuffer else self.calcCashBuffer(estTurnover)
        print("[CORE] cashBufferForReverseOrderHandling = {}".format(cashBufferForReverseOrderHandling))
        targetPosition = self.handleReverseOrder(targetPosition, cashBufferForReverseOrderHandling)
        print("[CORE] targetPosition after handleReverseOrder = {}".format(targetPosition))
        deltaPosition = self.calcDeltaPosition(targetPosition)
        print("[CORE] deltaPosition = {}".format(deltaPosition))
        
        return deltaPosition

    @staticmethod
    def calcEstTurnover(deltaPosition: np.ndarray, price: np.ndarray) -> np.float:
        return np.abs(np.sum(deltaPosition * price))

    @staticmethod
    def calcDrift(
        subTaskType: SubTaskTypeEnum, brokerType: BrokerTypeEnum,
        position: np.ndarray, price: np.ndarray, targetWeight: np.ndarray,
        portfolioCash: np.float, targetWeightPortfolioCash: np.float, mv: np.float, extraDrift: np.float=0
    ) -> np.float:
        # if not (subTaskType == SubTaskTypeEnum.PORTFOLIO or subTaskType == SubTaskTypeEnum.AQUMON and brokerType == BrokerTypeEnum.IB or subTaskType == SubTaskTypeEnum.BOCI):
        if subTaskType == SubTaskTypeEnum.AQUMON and brokerType == BrokerTypeEnum.AYERS:
            drift = 1 / 2 * np.sum(np.abs(position * price / mv - targetWeight))
            drift += np.abs(portfolioCash / mv - targetWeightPortfolioCash)
        else:
            drift = 1 / 2 * np.sum(np.abs(position * price / np.sum(position * price) - targetWeight)) # np.sum(position * price) or mv - portfolioCash
        return drift + extraDrift

    @staticmethod
    def calcCashBuffer(estTurnover=None) -> np.float:
        estTurnover = estTurnover or 0
        return estTurnover * 0.5 / 100

    def calcTargetWeightPortfolioCash(self) -> np.float:
        if self.subTaskType == SubTaskTypeEnum.PORTFOLIO or self.subTaskType == SubTaskTypeEnum.AQUMON and self.brokerType == BrokerTypeEnum.IB:
            return 0
        if self.subTaskType == SubTaskTypeEnum.AQUMON and self.brokerType == BrokerTypeEnum.AYERS:
            return 1 - np.sum(self.targetWeight)
        return 0

    def calcPV(self) -> np.float:
        return self.targetPV - self.estCommissionAndTax - self.cashBuffer

    def calcTargetPositionFractional(self) -> np.ndarray:
        return self.pv * self.targetWeight / self.askPrice

    def integerOptimization(self, targetPositionFractional: np.ndarray, frozen: np.ndarray=None) -> np.ndarray:
        """如果 frozen[i] = 1，则不对这一项做优化
        """
        frozen = np.zeros_like(targetPositionFractional) if frozen is None else frozen
        targetPositionIntegerPart = np.asarray([targetPositionFractional[i] // self.lotSize[i] * self.lotSize[i] if frozen[i] == 0 else targetPositionFractional[i] for i in range(len(frozen))])
        price = self.askPrice
        cashAFIO = self.pv - np.sum(self.askPrice * targetPositionIntegerPart)
        progress = np.zeros_like(self.targetWeight)
        optimizationUnit = np.ones_like(self.lotSize) * self.oddLotUnit if self.oddLotTrade else self.lotSize
        oddLotUnitUsed = np.zeros_like(self.oddLotFreezedInOddUnits)

        while np.any(progress != np.ones_like(progress)):
            decreaseInDrift = np.zeros_like(self.targetWeight)
            for i in range(len(progress)):
                if frozen[i] == 1 or progress[i] == 1:
                    continue
                decreaseInDrift[i] = np.sum(
                    np.absolute( targetPositionIntegerPart[i] * price[i] / self.pv - self.targetWeight[i] )
                ) - np.sum(
                    np.absolute( (targetPositionIntegerPart[i] + optimizationUnit[i]) * price[i] / self.pv - self.targetWeight[i] )
                )
                if self.subTaskType == SubTaskTypeEnum.PORTFOLIO or self.subTaskType == SubTaskTypeEnum.AQUMON and self.brokerType == BrokerTypeEnum.IB:
                    pass
                elif self.subTaskType == SubTaskTypeEnum.AQUMON and self.brokerType == BrokerTypeEnum.AYERS:
                    decreaseInDrift[i] += np.abs(cashAFIO / self.pv - self.targetWeightPortfolioCash) - np.abs((cashAFIO - price[i] * optimizationUnit[i]) / self.pv - self.targetWeightPortfolioCash)
            if np.all(decreaseInDrift < 0):
                progress = np.ones_like(progress)
            else:
                decreaseInDriftRankIndex = np.argsort(decreaseInDrift)[::-1]
                for index in decreaseInDriftRankIndex:
                    if frozen[index] == 0 and progress[index] != 1 and cashAFIO > price[index] * optimizationUnit[index] and decreaseInDrift[index] > 0:
                        if self.oddLotTrade:
                            oddLotUnitUsed[index] += self.oddLotUnit
                            if oddLotUnitUsed[index] == self.oddLotFreezedInOddUnits[index]:
                                progress[index] = 1
                        else:
                            progress[index] = 1
                        targetPositionIntegerPart[index] += optimizationUnit[index]
                        cashAFIO -= price[index] * optimizationUnit[index]
                        break
                else:
                    progress = np.ones_like(progress)

        return targetPositionIntegerPart
    
    def nonReverseOrdersForTargetPosition(self, targetPosition: np.ndarray) -> np.ndarray:
        a = self.filledQuantity
        b = targetPosition - self.currentPosition
        return np.logical_or(np.logical_or(np.sign(a) == np.sign(b), a == 0), b == 0)
        # return np.sign(a) == np.sign(b) or a == 0 or b == 0
        # return np.sign(self.filledQuantity) == np.sign(targetPosition - self.currentPosition)

    def handleReverseOrder(self, targetPosition: np.ndarray, cashBuffer: np.float) -> np.ndarray:
        nonReverse = self.nonReverseOrdersForTargetPosition(targetPosition)
        if np.all(nonReverse):
            return targetPosition
        cashNeededFroThisBatch = np.sum((targetPosition - self.currentPosition) * self.askPrice * nonReverse)
        cashAvailableForThisBatch = self.targetPV - self.estCommissionAndTax - cashBuffer - np.sum(self.currentPosition * self.askPrice)

        if cashNeededFroThisBatch <= cashAvailableForThisBatch:
            newTargetPosition = self.currentPosition.copy() # 只下不会出现反向交易的
            newTargetPosition[nonReverse] = targetPosition[nonReverse]
            portfolioCash = self.pv - np.sum(newTargetPosition * self.askPrice)
            mv = self.pv + self.mvTradingHalt
            drift = self.calcDrift(self.subTaskType, self.brokerType, newTargetPosition, self.askPrice, self.targetWeight, portfolioCash, self.targetWeightPortfolioCash, mv, self.tradingHaltDrift)
            if drift <= self.driftThreshold:
                print("\t[HANDLE REVERSE] drift 超过限制，将解除所有限制，将按原始的带有反向交易的 target position 下单")
                return newTargetPosition
            else:
                return targetPosition
        else:
            print("\t[HANDLE REVERSE] 资金不足！将使用另一个公式再次优化")
            newTargetPosition = self.currentPosition.copy()
            newTargetPosition[nonReverse] = self.calcTargetPositionInHanlingReverseOrder(nonReverse)[nonReverse]
            portfolioCash = self.pv - np.sum(newTargetPosition * self.askPrice)
            mv = self.pv + self.mvTradingHalt
            drift = self.calcDrift(self.subTaskType, self.brokerType, newTargetPosition, self.askPrice, self.targetWeight, portfolioCash, self.targetWeightPortfolioCash, mv, self.tradingHaltDrift)
            if drift <= self.driftThreshold and np.all(self.nonReverseOrdersForTargetPosition(newTargetPosition)):
                print("\t[HANDLE REVERSE] 进行优化后，drift 超过限制，将解除所有限制，将按原始的带有反向交易的 target position 下单")
                return newTargetPosition
            else:
                return targetPosition

    def calcTargetPositionInHanlingReverseOrder(self, nonReverse: np.ndarray) -> np.ndarray:
        m = np.sum(nonReverse)
        targetPosition = self.currentPosition.copy()
        for i in range(len(targetPosition)):
            if not nonReverse[i]:
                continue
            sumOfMVFOfReverseSymbolsDevidedByPV = np.sum(self.currentPosition * self.askPrice * (~nonReverse) / self.pv)
            sumOfTargetWeightForNonReverseSymbolsExcludingItSelf = np.sum(self.targetWeight * nonReverse) - self.targetWeight[i]
            targetPosition[i] = self.pv / self.askPrice[i] * (
                m / (m+1) * self.targetWeight[i] +
                1 / (m+1) * (
                    (1 - self.targetWeightPortfolioCash) -
                    sumOfMVFOfReverseSymbolsDevidedByPV -
                    sumOfTargetWeightForNonReverseSymbolsExcludingItSelf
                )
            )
        if np.sum(targetPosition * self.askPrice) > self.pv:
            for i in range(len(targetPosition)):
                if not nonReverse[i]:
                    continue
                sumOfMVFOfReverseSymbolsDevidedByPrice = np.sum(self.currentPosition * self.askPrice * (~nonReverse) / self.askPrice[i])
                sumOfTargetWeightForNonReverseSymbols = np.sum(self.targetWeight * nonReverse)
                targetPosition[i] = self.pv / self.askPrice[i] * self.targetWeight[i] + \
                    1 / m * (
                        self.pv / self.askPrice[i] -
                        sumOfMVFOfReverseSymbolsDevidedByPrice -
                        self.pv / self.askPrice[i] * sumOfTargetWeightForNonReverseSymbols
                    )
        targetPosition = self.integerOptimization(targetPosition, 1 - nonReverse)
        return targetPosition

    def calcDeltaPosition(self, targetPosition: np.ndarray):
        quantity = targetPosition - self.currentPosition
        if self.subTaskType == SubTaskTypeEnum.PORTFOLIO or self.subTaskType == SubTaskTypeEnum.AQUMON and self.brokerType == BrokerTypeEnum.IB or self.subTaskType == SubTaskTypeEnum.AQUMON and self.brokerType == BrokerTypeEnum.AYERS:
            quantity = (quantity / (self.totalNumOfBatches - self.currentBatchIndex + 1)).astype(int)
        elif self.subTaskType == SubTaskTypeEnum.BOCI:
            if self.turnoverLimit > 0:
                quantity = (quantity * self.turnoverLimit / self.calcEstTurnover(quantity, self.askPrice)).astype(int)

        if self.region == RegionEnum.CN and self.currency == CurrencyEnum.CNH:
            quantity[quantity > 0] = (np.round(quantity[quantity > 0] / self.lotSize)).astype(int) * self.lotSize
            quantity[quantity < 0] = np.min(quantity[quantity < 0], (self.longVolumeHistory - self.shortVolumeToday)[quantity < 0])
        
        return quantity