from core.targetWeightToDeltaPosition import TargetWeightToDeltaPositionEquityCalculator, TradingInfo
from inputs.sample import BOCISample
from utils.printing import *
from constants.type import *
from subtasks.subtask import SubTask
import numpy as np
import copy

# https://confluence-algo.aqumon.com/pages/viewpage.action?pageId=43876723
class BOCISubTask(SubTask):
    DRIFT_THRESHOLD = np.float('0.07')
    SUBTASK_TYPE = SubTaskTypeEnum.BOCI
    PERSHING_PRICE_RANGE = np.float('0.03')
    sample: BOCISample

    def __init__(self, sample: BOCISample):
        INFO(f"BOCISubTask: {CLASS_STRING(sample)}")
        samplePriceTruncated = self.truncatePrice(sample)
        super().__init__(samplePriceTruncated)

    def calc(self) -> np.ndarray:
        deltaPosition = super().calc()
        deltaPosition = self.truncateQuantity(deltaPosition)
        drift = self.calcDrift(deltaPosition)
        if drift <= self.DRIFT_THRESHOLD:
            INFO(f"truncate 之后 drift 是 {drift}，没超 threshold {self.DRIFT_THRESHOLD}，将按照 truncate 之后的值下单。")
            return deltaPosition
        else:
            INFO(f"truncate 之后 drift 是 {drift}，超了 threshold {self.DRIFT_THRESHOLD}，需要尝试调整 ETF 的 position，来降低 drift，如果无法降低，将放弃任务")
            return self.lastDitchAttempOnDrift(deltaPosition)
    
    def calcPV(self) -> np.float:
        sample = self.sample
        return sample.deltaAmount + np.sum(sample.currentPosition * sample.askPrice) + sample.availableCashAmount - np.sum(sample.filledAmount)
    
    def calcCommission(self, estTurnover: np.float) -> np.float:
        return 0
    
    def truncatePrice(self, sample: BOCISample) -> BOCISample:
        if np.any(sample.lastPrice * (1 - self.PERSHING_PRICE_RANGE) > sample.priceUpperBound) or np.any(sample.lastPrice * (1 + self.PERSHING_PRICE_RANGE) < sample.priceLowerBound):
            PANIC("Price cannot be truncated.")
        
        samplePriceTruncated = copy.deepcopy(sample)
        samplePriceTruncated.askPrice = np.clip(samplePriceTruncated.askPrice, samplePriceTruncated.priceLowerBound, samplePriceTruncated.priceUpperBound)
        samplePriceTruncated.askPrice = np.clip(samplePriceTruncated.askPrice, samplePriceTruncated.lastPrice * (1 - self.PERSHING_PRICE_RANGE), samplePriceTruncated.lastPrice * (1 + self.PERSHING_PRICE_RANGE))
        return samplePriceTruncated
    
    def calcDrift(self, deltaPosition: np.ndarray) -> np.float:
        return TargetWeightToDeltaPositionEquityCalculator.calcDrift(SubTaskTypeEnum.BOCI, BrokerTypeEnum.BOCI, self.sample.currentPosition + deltaPosition, self.sample.askPrice, self.sample.targetWeight, 0, 0, np.sum((self.sample.currentPosition + deltaPosition) * self.sample.askPrice))

    def truncateQuantity(self, deltaPosition: np.ndarray) -> np.ndarray:
        return np.clip(deltaPosition, self.sample.quantityLowerBound, self.sample.quantityUpperBound)

    def lastDitchAttempOnDrift(self, deltaPosition: np.ndarray) -> np.ndarray:
        calculator = self.calculator
        targetPosition = calculator.currentPosition + deltaPosition
        remainingAttempts = self.nSecurities * max(5, np.sqrt(calculator.targetPV / 20000))
        drift = self.calcDrift(targetPosition - calculator.currentPosition)
        price = calculator.askPrice
        cashAvailable = calculator.pv - np.sum(price * targetPosition)
        while remainingAttempts > 0 and drift > self.DRIFT_THRESHOLD:
            decreaseInDrift = np.zeros(self.nSecurities)
            operation = np.zeros(self.nSecurities) # +ev = BUY; -ev = SELL
            for i in range(self.nSecurities):
                decreaseInDrift_i_buy = np.sum(
                    np.absolute( targetPosition[i] * price[i] / calculator.pv - calculator.targetWeight[i] )
                ) - np.sum(
                    np.absolute( (targetPosition[i] + calculator.lotSize[i]) * price[i] / calculator.pv - calculator.targetWeight[i] )
                )
                decreaseInDrift_i_sell = np.sum(
                    np.absolute( targetPosition[i] * price[i] / calculator.pv - calculator.targetWeight[i] )
                ) - np.sum(
                    np.absolute( (targetPosition[i] - calculator.lotSize[i]) * price[i] / calculator.pv - calculator.targetWeight[i] )
                )
                operation[i] = calculator.lotSize[i] if decreaseInDrift_i_buy > decreaseInDrift_i_sell else -calculator.lotSize[i]
                decreaseInDrift[i] = max(decreaseInDrift_i_buy, decreaseInDrift_i_sell)
            
            decreaseInDriftRankIndex = np.argsort(decreaseInDrift)[::-1]
            foundAny = False
            for index in decreaseInDriftRankIndex:
                if operation[index] > 0 and cashAvailable < price[index] * operation[index] * calculator.lotSize[index] or decreaseInDrift[index] <= 0:
                    continue
                if operation[index] > 0 and deltaPosition[index] + operation[index] > self.sample.quantityUpperBound[i] or operation[index] < 0 and deltaPosition[index] - operation[index] < self.sample.quantityLowerBound[i]:
                    continue
                targetPosition[index] += calculator.lotSize[index] * operation[index]
                cashAvailable -= price[index] * calculator.lotSize[index] * operation[index]
                remainingAttempts -= 1
                foundAny = True
                drift = self.calcDrift(targetPosition - calculator.currentPosition)
                if drift <= self.DRIFT_THRESHOLD:
                    INFO(f"Drift 降到了 {drift}")
                    return targetPosition - calculator.currentPosition
                break
            
            if not foundAny:
                print(targetPosition - calculator.currentPosition)
                PANIC(f"Cannot find more ETF to optimize and current drift = {drift}, threshold={self.DRIFT_THRESHOLD}")
        
        totalMV = np.sum(targetPosition * price)
        panic_msg = f"Stop optimizations after several attempts, current drift = {drift}, threshold={self.DRIFT_THRESHOLD}]\n" + \
            f"Conditions before exit are:\n"
        for i in range(len(calculator.symbol)):
            panic_msg += f"{calculator.symbol[i]}:\ttargetPosition: {targetPosition[i]}\tdrift: {abs(targetPosition[i] * price[i] / totalMV - calculator.targetWeight[i]) / 2}\n"
        PANIC(panic_msg)
        
        