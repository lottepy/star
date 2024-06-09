import pandas as pd
import numpy as np
from algorithm import addpath
import datetime
import os

bond_list_path=os.path.join(addpath.config_path,'2013-2020国债筛选.xlsx')
bond_list=pd.read_excel(bond_list_path,sheet_name='combined')
bond_list=bond_list[bond_list['总入选次数']>=1]['Ticker']
bond_portfolio_data_path=os.path.join(addpath.data_path,'bond','portfolio')
if os.path.exists(bond_portfolio_data_path):
    pass
else:
    os.makedirs(bond_portfolio_data_path)

formation_date_path=os.path.join(addpath.config_path,'formation_date_bond.csv')
formation_dates=pd.read_csv(formation_date_path,parse_dates=[0])['formation_date']
formation_dates = formation_dates[formation_dates < datetime.datetime.today()]
bond_list_path=os.path.join(addpath.config_path,'2013-2020国债筛选.xlsx')
bond_list=pd.read_excel(bond_list_path,sheet_name='combined')
for date in formation_dates:
    portfolio=bond_list[bond_list[str(date.year)+'入选']==1][['Ticker']]
    portfolio=portfolio.drop_duplicates()
    portfolio['weight']=1/len(portfolio)
    portfolio.to_csv(os.path.join(bond_portfolio_data_path,date.strftime(format('%Y-%m-%d'))+'.csv'),index=False)



