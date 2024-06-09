import pandas as pd
import numpy
import os
import warnings
warnings.filterwarnings('ignore')
'''
#设置路径
read_path = r'D:\AQUMON\ThemeProject\data\CHN\monthly_data'
save_path = r'D:\AQUMON\ThemeProject\aqumon-theme-portfolio-master\data\financials'
os.chdir(read_path)
csv_name_list = os.listdir()

sample=pd.read_csv(r'D:\AQUMON\ThemeProject\data\HK\hk_data\financials\1 HK Equity.csv')
sample.loc[:, 'date'] = pd.to_datetime(sample.iloc[:,0], format='%Y/%m/%d', errors='ignore')
sample.index=sample.loc[:,'date']
sample=sample.drop(columns=sample.columns[0])
sample=sample.drop(columns=['date'])

for csv_name in csv_name_list:
    print(csv_name)
    datain=pd.read_csv(read_path+'\\'+csv_name)
    datain.loc[:, 'date'] = pd.to_datetime(datain.iloc[:, 0], format='%Y/%m/%d', errors='ignore')
    datain.index=datain.loc[:,'date']

    dataout = pd.DataFrame(index=datain.index,columns=sample.columns)
    dataout.loc[:, 'BS_TOT_ASSET'] = datain.loc[:, 'tot_assets']
    dataout.loc[:, 'BS_TOT_LIAB2'] = datain.loc[:, 'tot_liab']
    dataout.loc[:, 'TOT_EQUITY'] = datain.loc[:, 'tot_equity']
    dataout.loc[:, 'SALES_REV_TURN'] = datain.loc[:, 'qfa_tot_oper_rev']+datain.loc[:, 'qfa_tot_oper_rev'].shift(3)
    dataout.loc[:, 'EBIT'] = datain.loc[:, 'qfa_opprofit']+datain.loc[:, 'qfa_opprofit'].shift(3)
    dataout.loc[:, 'NET_INCOME'] = datain.loc[:, 'qfa_net_profit_is']+datain.loc[:, 'qfa_net_profit_is'].shift(3)
    dataout.loc[:, 'CF_CASH_FROM_OPER'] = datain.loc[:, 'qfa_net_cash_flows_oper_act']+datain.loc[:, 'qfa_net_cash_flows_oper_act'].shift(3)
    dataout.loc[:, 'IS_INT_EXPENSE'] = datain.loc[:, 'lt_borrow']+datain.loc[:, 'lt_borrow'].shift(3)
    dataout=dataout.resample('2Q').last()
    dataout.to_csv(save_path+'\\'+csv_name,index=True)


'''

symbol_list=pd.read_csv(r'D:\AQUMON\ThemeProject\aqumon-theme-portfolio-master\config\CN_Quality\symbol_list_raw.csv')
data=pd.DataFrame()
for symbol in symbol_list.loc[:,'symbol']:
    filename = r'D:\AQUMON\ThemeProject\aqumon-theme-portfolio-master\data\cn_data\trading'+'\\'+symbol+'.csv'
    if os.path.exists(filename):
        symbol=list([symbol])
        symbol = pd.DataFrame(symbol)
        data=pd.concat([data,symbol],axis=0)
data.columns = list(['symbol'])
data.to_csv(r'D:\AQUMON\ThemeProject\aqumon-theme-portfolio-master\config\CN_Quality\symbol_list.csv',index=False)



