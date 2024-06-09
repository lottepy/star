from core.targetWeightToDeltaPosition import TargetWeightToDeltaPositionEquityCalculator, TradingInfo
from inputs.sample import AqumonSample
from constants.type import *
from subtasks.subtask import SubTask
import numpy as np

class AqumonSubTask(SubTask):
    DRIFT_THRESHOLD = np.float('0.08')
    SUBTASK_TYPE = SubTaskTypeEnum.AQUMON
    
    def __init__(self, sample: AqumonSample):
        super().__init__(sample)
    
    def calcPV(self):
        sample = self.sample
        # x/y/z/a for debug purpose
        x = sample.deltaAmount
        y = np.sum(sample.currentPosition * sample.askPrice)
        z = sample.availableCashAmount
        a = np.sum(sample.filledAmount)
        return sample.deltaAmount + np.sum(sample.currentPosition * sample.askPrice) + sample.availableCashAmount - np.sum(sample.filledAmount)

    def calcCommission(self, estTurnover: np.float):
        etfCategory = self.nSecurities
        commission = 0
        if self.sample.region == RegionEnum.HK:
            commission += max(3, 0.0003 * estTurnover)
            commission += 15 * etfCategory
            commission += min(max(5.5, 0.00005 * estTurnover), 100)
            commission += 0.5 * etfCategory
            commission += max(1, 0.001 * estTurnover)
            commission += max(0.01, 0.00005 * estTurnover)
            commission += max(0.01, 0.000027 * estTurnover)
        elif self.sample.region == RegionEnum.US:
            commission += 0
        return commission