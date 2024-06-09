from enum import Enum


DATA_MASTER_LOAD_BATCH = 50

class LabelField(Enum):
    """Key Label Field 的各列名称"""
    MAIN = 0
    CCPFXRATE = 1
    INT = 2
    SPLIT = 3