import pandas as pd
import numpy
import os
import warnings
warnings.filterwarnings('ignore')

#设置路径
read_path = r'D:\AQUMON\ThemeProject\data\CHN\A trading_20200730\daily_data'
save_path = r'D:\AQUMON\ThemeProject\aqumon-theme-portfolio-master\data\trading'
os.chdir(read_path)
csv_name_list = os.listdir()

sample=pd.read_csv(r'D:\AQUMON\ThemeProject\data\HK\hk_data\trading\1 HK Equity.csv')
sample.loc[:, 'date'] = pd.to_datetime(sample.iloc[:,0], format='%Y/%m/%d', errors='ignore')
#sample.index=sample.loc[:,'date']
#sample=sample.drop(columns=['date'])
for csv_name in csv_name_list:
    print(csv_name)
    datain=pd.read_csv(read_path+'\\'+csv_name)
    datain.loc[:, 'date'] = pd.to_datetime(datain.iloc[:, 0], format='%Y-%m-%d', errors='ignore')
    dataout = pd.DataFrame(columns=sample.columns)
    dataout.loc[:, 'date'] = datain.loc[:, 'date']
    dataout.loc[:, 'PX_LAST'] = datain.loc[:, 'close']
    dataout.loc[:, 'PX_VOLUME'] = datain.loc[:, 'volume']
    dataout.loc[:, 'EQY_SH_OUT'] = datain.loc[:, 'total_shares']
    dataout.loc[:, 'DAY_TO_DAY_TOT_RETURN_GROSS_DVDS'] = datain.loc[:, 'adj_close'] / datain.loc[:, 'adj_close'].shift() - 1
    dataout.to_csv(save_path+'\\'+csv_name,index=False)






