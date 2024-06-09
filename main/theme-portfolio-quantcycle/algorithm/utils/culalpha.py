import pandas as pd
import numpy as np
import os
import math
from algorithm import addpath
from constants import *
import datetime
import statsmodels.api as sm
import matplotlib.pyplot as plt

portfolio_name='US_Safety'

cumreturn_path=os.path.join(addpath.result_path, portfolio_name,'cumreturn_all.csv')
reference_path=os.path.join(addpath.data_path,'us_data','reference','market_index_cumreturn.csv')

cum_return=pd.read_csv(cumreturn_path)
reference=pd.read_csv(reference_path).iloc[:,1]

for i in range(len(cum_return.columns)-1):
    print(portfolio_name+' columns： '+str(i)+'all period')
    x = sm.add_constant(reference)
    y=cum_return.iloc[:,i+1]
    model = sm.OLS(y, x).fit()
    print(model.summary())






