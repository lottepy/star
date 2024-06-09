from inputs.sample import PortfolioSample
from constants.type import *
from subtasks.subtask import SubTask
import numpy as np

class PortfolioSubTask(SubTask):
    DRIFT_THRESHOLD = np.float('0.05')
    SUBTASK_TYPE = SubTaskTypeEnum.PORTFOLIO
    
    def __init__(self, sample: PortfolioSample):
        super().__init__(sample)
    
    def calcPV(self):
        return self.sample.targetPV

    def calcCommission(self, estTurnover: np.float):
        return 0