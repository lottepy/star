from typing import Callable
import numpy as np
from enum import Enum
from utils.printing import *

class EstDirection(str, Enum):
    BUY = 'BUY'
    SELL = 'SELL'
    HOLD = 'HOLD'

class LimitUpLimitDownTradingHaltHandler:
    """
    - Limit Up: 涨停
    - Limit Down: 跌停
    - Trading Halt: 停牌（包括 MWCB（Market-Wide Circuit Breakers，指数熔断）、八号风球等全市场停牌，公司事件等单个股票停牌）
    """
    def __init__(
        self,
        backupRecords: dict,
        backupSymbols: list,
        symbols: np.ndarray,
        targetWeight: np.ndarray,
        currentPosition: np.ndarray,
        askPrice: np.ndarray,
        calcTargetPV: Callable,
        limitUps: np.ndarray,
        limitDowns: np.ndarray,
        tradingHalts: np.ndarray
    ):
        self.canNotSell = set()
        self.canNotBuy = set()
        self.backupRecords = backupRecords
        self.backupSymbols = backupSymbols
        self.symbols = symbols
        self.n = len(symbols)
        self.targetWeight = targetWeight
        self.originalTargetWeight = targetWeight
        self.currentPosition = currentPosition
        self.currentWeight = np.zeros_like(symbols)
        self.askPrice = askPrice
        self.targetPV = calcTargetPV()
        self.wannaBuy = np.zeros_like(symbols)
        self.wannaSell = np.zeros_like(symbols)
        self.MVHalted = 0
        self.limitUps = limitUps
        self.limitDowns = limitDowns
        self.tradingHalts = tradingHalts
        self.haltedIndex = []

    def handle(self):
        self.currentWeight = self.calcCurrentWeight()
        for i in range(self.n):
            if self.targetWeight[i] > 0:
                wannaBuyOrWannaSell = self.determineWhetherWannaBuyOrWannaSell(self.currentWeight[i], self.targetWeight[i])
                symbol = self.symbols[i]
                if symbol in self.backupRecords.keys():
                    backupSymbol = self.backupRecords[symbol]
                    j = np.where(self.symbols == backupSymbol)[0][0]
                    mainSymbolTargetWeight = self.targetWeight[i] + 0 # deep copy?
                    backupSymbolTargetWeight = mainSymbolTargetWeight - self.currentPosition[i] * self.askPrice[i] / self.targetPV
                    
                    wannaBuyOrWannaSellForBackup = self.determineWhetherWannaBuyOrWannaSell(self.currentWeight[j], backupSymbolTargetWeight)
                    self.canNotBuy.add(symbol)
                    self.accumulateMVHalted(i)
                    self.targetWeight[i] = 0
                    if wannaBuyOrWannaSellForBackup == EstDirection.BUY and self.limitUpOrTradingHalt(j):
                        self.canNotBuy.add(backupSymbol)
                        self.accumulateMVHalted(j)
                    elif wannaBuyOrWannaSellForBackup == EstDirection.SELL and self.limitDownOrTradingHalt(j):
                        self.canNotSell.add(backupSymbol)
                        self.accumulateMVHalted(j)
                    else:
                        self.targetWeight[j] = backupSymbolTargetWeight
                else:
                    if wannaBuyOrWannaSell == EstDirection.BUY and self.limitUpOrTradingHalt(i):
                        self.handleLimitUpOrTradingHalt(i)
                    elif wannaBuyOrWannaSell == EstDirection.SELL and self.limitDownOrTradingHalt(i):
                        self.handleLimitDownOrTradingHalt(i)
                    else:
                        pass
        for i in range(self.n):
            if self.currentPosition[i] > 0 and np.isclose(self.originalTargetWeight[i], 0, atol=1e-4):
                if self.limitDownOrTradingHalt(i):
                    self.handleLimitDownOrTradingHalt(i)    

        if not np.isclose(np.sum(self.targetWeight), 0, 1e-4):
            self.targetWeight /= np.sum(self.targetWeight)

    def calcCurrentWeight(self) -> np.ndarray:
        return self.askPrice * self.currentPosition / self.targetPV
    
    def determineWhetherWannaBuyOrWannaSell(self, currentWeight, targetWeight) -> EstDirection:
        """
        判断一个股票是想买入卖出还是不动。
        
        判断逻辑是：
        - 如果 targetWeight - currentWeight > 0.1%，则认为想买入
        - 如果 targetWeight - currentWeight < -0.1%，则认为想卖出
        - 否则认为不动
        
        返回：
        - EstDirection.BUY   买入
        - EstDirection.HOLD  不动
        - EstDirection.SELL  卖出
        """
        if targetWeight - currentWeight > np.float("0.001"):
            return EstDirection.BUY
        elif targetWeight - currentWeight < -np.float("0.001"):
            return EstDirection.SELL
        else:
            return EstDirection.HOLD

    def limitUp(self, i: int) -> bool:
        return self.limitUps[i]
    
    def limitDown(self, i: int) -> bool:
        return self.limitDowns[i]
    
    def tradingHalt(self, i: int) -> bool:
        return self.tradingHalts[i]
    
    def limitUpOrTradingHalt(self, i: int) -> bool:
        return self.limitUp(i) or self.tradingHalt(i)
    
    def limitDownOrTradingHalt(self, i: int) -> bool:
        return self.limitDown(i) or self.tradingHalt(i)
    
    def accumulateMVHalted(self, i: int) -> None:
        if not i in self.haltedIndex:
            self.haltedIndex.append(i)
            INFO(f"{self.symbols[i]} will be included in MV_停牌")
            self.MVHalted += self.currentPosition[i] * self.askPrice[i]

    def handleLimitUpOrTradingHalt(self, i: int):
        self.canNotBuy.add(self.symbols[i])
        mainSymbolTargetWeight = self.targetWeight[i] + 0 # deep copy?
        backupSymbolTargetWeight = mainSymbolTargetWeight - self.currentPosition[i] * self.askPrice[i] / self.targetPV
        for backupSymbol in self.backupSymbols:
            j = np.where(self.symbols == backupSymbol)[0][0]
            if (backupSymbol not in self.backupRecords.values()) and (not self.limitUpOrTradingHalt(j)) and (self.determineWhetherWannaBuyOrWannaSell(self.currentWeight[j], self.targetWeight[i]) == EstDirection.BUY):
                self.targetWeight[j] = backupSymbolTargetWeight
                self.accumulateMVHalted(j)
                self.backupRecords[self.symbols[i]] = self.symbols[j]
                break
        else:
            WARN(f"Cannot find a backup symbol for {self.symbols[i]}(index: {i}), exiting...")
            raise Exception(f"Cannot find a backup symbol for {self.symbols[i]}(index: {i}), exiting...")
        # self.targetWeight /= np.sum(self.targetWeight)
        self.targetWeight[i] = 0
    
    def handleLimitDownOrTradingHalt(self, i: int):
        self.canNotSell.add(self.symbols[i])
        self.targetWeight[i] = 0
        self.accumulateMVHalted(i)
        # self.targetWeight /= np.sum(self.targetWeight)
