# -*- coding:utf-8 -*-

import numpy as np
import pandas as pd

def recordParser(record):
    holdings = pd.DataFrame(data = record.holding_list,index = record.time_list,columns=record.universe)
    portfolios = pd.DataFrame(data = record.portfolio_list,index = record.time_list,columns=['portfolio','cash','position_value'])
    return portfolios
