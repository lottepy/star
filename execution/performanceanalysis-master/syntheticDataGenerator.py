# -*- coding:utf-8 -*-

import numpy as np
import pandas as pd

class ExampleRecord():
    def __init__(self):
        self.time_list = np.array([])
        self.universe = []
        
        self.holding_list = np.array([])
        self.pv_list = np.array([])
        self.portfolio_list = np.array([])

        self.parameters = []
    
def generateARecord():
    data = pd.read_csv('./data/synthetic_data/data_example1.csv',index_col=0, parse_dates=True)
    recordExample = ExampleRecord()    
    recordExample.time_list = np.array(data.index)
    recordExample.universe = ['000001.SZ','000002.SZ','000004.SZ']
    recordExample.parameters = []
    recordExample.holding_list = data.iloc[:,3:6].to_numpy()
    recordExample.pv_list = data.iloc[:,9:12].to_numpy()
    recordExample.portfolio_list = data.iloc[:,0:3].to_numpy()
    return recordExample
    
if __name__ == "__main__":
    recordExample = generateARecord()