import pickle
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from constants import *
from algorithm import addpath
from algorithm.portfolio_formation.investment_universe import investment_universe

input_path = os.path.join(addpath.data_path, "cn_data")
pkl_file = open(os.path.join(input_path, 'factor.pkl'), 'rb')
factors_all = pickle.load(pkl_file)
pkl_file.close()

symbol_list=pd.read_csv(r'D:\AQUMON\ThemeProject\aqumon-theme-portfolio\config\CN_Quality\symbol_list.csv')['symbol'].tolist()
industry_list=pd.read_csv(r'D:\AQUMON\ThemeProject\aqumon-theme-portfolio\config\CN_Quality\industry_list.csv')['industry'].tolist()

symbol_industry = pd.read_csv(
    r'D:\AQUMON\ThemeProject\aqumon-theme-portfolio\config\CN_Quality\symbol_list_industry.csv')
symbol_industry.set_index('symbol', inplace=True)
date1=datetime(2018,7,31)
date2=datetime(2020,7,31)
result=pd.DataFrame(index=industry_list,columns=['MARKET_CAP','ROE','ROA','CFOA','LEV','EARNVAR','BTM','STP','CFTP','ETP',\
                                                 'RealizedVol_3M','REVS10_path','Momentum_2_15'])
date_list=[date1,date2]
for date in date_list:
    factors =pd.DataFrame()
    for industry in industry_list:
        symbol_list=symbol_industry.index[symbol_industry['industry'] == industry]
        for symbol in symbol_list:
            tmp = factors_all[symbol][factors_all[symbol].index == date]
            factors = pd.concat([factors, tmp], axis=0)
        factors=factors[result.columns].dropna()
        marcap_sum = factors['MARKET_CAP'].sum()
        result.loc[industry, 'MARKET_CAP']=marcap_sum
        result.loc[industry, 'ROE'] = sum(factors['ROE'] * factors['MARKET_CAP'])/marcap_sum
        result.loc[industry, 'ROA'] = sum(factors['ROA'] * factors['MARKET_CAP'])/marcap_sum
        result.loc[industry, 'CFOA'] = sum(factors['CFOA'] * factors['MARKET_CAP'])/marcap_sum
        result.loc[industry, 'LEV'] = sum(factors['LEV'] * factors['MARKET_CAP'])/marcap_sum

        result.loc[industry, 'EARNVAR'] = sum(factors['EARNVAR'] * factors['MARKET_CAP'])/marcap_sum
        result.loc[industry, 'BTM'] = sum(factors['BTM'] * factors['MARKET_CAP'])/marcap_sum
        result.loc[industry, 'STP'] = sum(factors['STP'] * factors['MARKET_CAP'])/marcap_sum
        result.loc[industry, 'CFTP'] = sum(factors['CFTP'] * factors['MARKET_CAP'])/marcap_sum
        result.loc[industry, 'ETP'] = sum(factors['ETP'] * factors['MARKET_CAP'])/marcap_sum
        result.loc[industry, 'RealizedVol_3M'] = sum(factors['RealizedVol_3M'] * factors['MARKET_CAP'])/marcap_sum
        result.loc[industry, 'REVS10_path'] = sum(factors['REVS10_path'] * factors['MARKET_CAP'])/marcap_sum
        result.loc[industry, 'Momentum_2_15'] = sum(factors['Momentum_2_15'] * factors['MARKET_CAP'])/marcap_sum
    result.to_csv(r'D:\AQUMON\ThemeProject\aqumon-theme-portfolio\config\CN_Quality'+'\\'+str(date.year)+'.csv')







