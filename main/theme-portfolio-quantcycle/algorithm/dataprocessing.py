# import pandas as pd
# import numpy
# import os
# import warnings
# warnings.filterwarnings('ignore')
#
# #设置路径
# read_path = r'/Users/liyang/Desktop/AQUMON/aqumon-theme-portfolio/data/cn_data/daily_data'
# save_path = r'/Users/liyang/Desktop/AQUMON/aqumon-theme-portfolio/data/cn_data/trading'
# os.chdir(read_path)
# csv_name_list = os.listdir()
#
# sample=pd.read_csv(r'D:\AQUMON\ThemeProject\aqumon-theme-portfolio\data\hk_data\trading\1 HK Equity.csv')
# sample.loc[:, 'date'] = pd.to_datetime(sample.iloc[:,0], format='%Y/%m/%d', errors='ignore')
# sample=pd.DataFrame(columns=['date','PX_LAST','PX_VOLUME','EQY_SH_OUT','DAY_TO_DAY_TOT_RETURN_GROSS_DVDS',\
#                              'PX_LAST_RAW'])
#
# sample.index=sample.loc[:,'date']
# sample=sample.drop(columns=['date'])
# for csv_name in csv_name_list:
#     print(csv_name)
#     datain = pd.read_csv(read_path + '//' + csv_name, parse_dates=[0], index_col=0)
#     dataout = pd.DataFrame(index=datain.index)
#     dataout.loc[:, 'date'] = datain.index
#     dataout.loc[:, 'PX_LAST'] = datain.loc[:, 'adj_close']
#     dataout.loc[:, 'PX_VOLUME'] = datain.loc[:, 'volume']
#     dataout.loc[:, 'EQY_SH_OUT'] = datain.loc[:, 'total_shares']
#     dataout.loc[:, 'DAY_TO_DAY_TOT_RETURN_GROSS_DVDS'] = datain.loc[:, 'adj_close'] / datain.loc[:, 'adj_close'].shift() - 1
#     dataout.loc[:, 'PX_LAST_RAW'] = datain.loc[:, 'close']
#     dataout.loc[:, 'MARKET_CAP'] = datain.loc[:, 'mkt_cap_ard']
#     dataout.loc[:, 'TURNOVER'] = datain.loc[:, 'turn']
#     dataout.to_csv(save_path+'//'+csv_name,index=False)
#
#
#
# #downloan trading data from datamater to update local data
# import pandas as pd
# from choice_client import c
#
# symbol_list=pd.read_csv(r'D:\AQUMON\ThemeProject\aqumon-theme-portfolio-master\config\CN_Quality\symbol_list.csv')
# data=pd.DataFrame()
#
# for symbol in symbol_list.loc[:,'symbol']:
#     print(symbol)
#     data = c.csd(symbol, "CLOSE,VOLUME,TOTALSHARE,FRONTTAFACTOR", "2020-07-30", "2020-09-01",
#                  "BaseDate=2020-09-02,period=1,adjustflag=1,curtype=1,order=1,market=CNSESH")
#     data.index=pd.to_datetime(data.index, format='%Y/%m/%d', errors='ignore')
#     data['adjclose']=data.iloc[:,0]*data.iloc[:,3]
#
#     dataold=pd.read_csv(r'D:\AQUMON\ThemeProject\aqumon-theme-portfolio\data\cn_data\trading'+'\\'+symbol+'.csv')
#     dataold.loc[:, 'date'] = pd.to_datetime(dataold.iloc[:,0], format='%Y-%m-%d', errors='ignore')
#
#     dataout=pd.DataFrame(index=data.index,columns=dataold.columns)
#     dataout.loc[:, 'date'] = dataout.index
#     dataout.loc[:, 'PX_LAST'] = data.iloc[:,0]
#     dataout.loc[:, 'PX_VOLUME'] = data.iloc[:,1]
#     dataout.loc[:, 'EQY_SH_OUT'] = data.iloc[:,2]
#     dataout.loc[:, 'DAY_TO_DAY_TOT_RETURN_GROSS_DVDS'] = data.iloc[:,4] / data.iloc[:,4].shift() - 1
#     dataout=dataout.drop(index=dataout.index[0])
#     dataout=pd.concat([dataold,dataout],axis=0)
#
#     dataout.to_csv(r'D:\AQUMON\ThemeProject\aqumon-theme-portfolio\data\cn_data\trading'+'\\'+symbol+'.csv',index=False)
# #
# import datetime
# import pandas as pd
# mktindex=pd.read_csv(r'D:\AQUMON\ThemeProject\aqumon-theme-portfolio\data\cn_data\reference\market_index.csv')
# mktindex.loc[:, 'date'] = pd.to_datetime(mktindex.iloc[:,0], format='%Y-%m-%d', errors='ignore')
# mktindex.set_index('date',inplace=True)
# mktindex=mktindex.resample('1D').ffill()
# mktindex=mktindex.loc[datetime.datetime(year=2012,month=12,day=31):]
# mktindex.to_csv(r'D:\AQUMON\ThemeProject\aqumon-theme-portfolio\results\CN_caixin\marketindex_20121231.csv')


# #choice上导出基金优选白名单申赎状态
# from choice_client import c
# import pandas as pd
# import datetime
# from retry import retry
#
# @retry()
# def datasearch(symbol,field,parastr):
#     data=c.css(symbol,field,parastr)
#     return data
#
# fund_list=pd.read_csv(r'D:\AQUMON\whitelist\whitelist.csv')
# fund_list=fund_list['wind_ticker']
# start=datetime.datetime(2012,1,1)
# end = datetime.datetime(2020,9,1)
# date_list=pd.date_range(start=start,end=end,freq='M')
# PURCHSTATUS=pd.DataFrame(index=date_list,columns=list(fund_list))
# REDEMSTATUS=pd.DataFrame(index=date_list,columns=list(fund_list))
# for symbol in fund_list:
#     temp_p=pd.DataFrame(columns=list([symbol]),index=date_list)
#     temp_r = pd.DataFrame(columns=list([symbol]),index=date_list)
#     for date in date_list:
#         print(date)
#         data=datasearch(symbol,"PURCHSTATUS,REDEMSTATUS","TradeDate="+date.strftime("%Y-%m-%d"))
#         temp_p.loc[date,symbol]=data.values[0][0]
#         temp_r.loc[date,symbol]=data.values[0][1]
#     PURCHSTATUS.loc[:,symbol]=temp_p
#     REDEMSTATUS.loc[:,symbol]=temp_r
# PURCHSTATUS.to_csv(r'D:\AQUMON\whitelist\PURCHSTATUS.csv',encoding='utf-8_sig')
# REDEMSTATUS.to_csv(r'D:\AQUMON\whitelist\REDEMSTATUS.csv',encoding='utf-8_sig')
#
#

# import pandas as pd
# import os
# read_path1 = r'/Users/lizhirui/PycharmProjects/aqumon-theme-portfolio-quantcycle/data/us_data/trading'
# read_path2 = r'/Users/lizhirui/PycharmProjects/aqumon-theme-portfolio-quantcycle/data/12_31_data/us_data/trading'
# save_path = read_path1
# os.chdir(read_path1)
# csv_name_list1 = os.listdir()
# os.chdir(read_path2)
# csv_name_list2 = os.listdir()
# for csv_name in csv_name_list2:
#     print(csv_name)
#     if csv_name in csv_name_list1:
#         old_data=pd.read_csv(os.path.join(read_path1,csv_name),parse_dates=[0],index_col=0)
#         new_data=pd.read_csv(os.path.join(read_path2,csv_name),parse_dates=[0],index_col=0)
#         if len(old_data)>0:
#             new_data=new_data[new_data.index>old_data.index[-1]]
#             old_data = pd.concat([old_data, new_data], axis=0)
#             old_data.to_csv(os.path.join(read_path1, csv_name))
#         else:
#             new_data.to_csv(os.path.join(read_path1, csv_name))
#             print(csv_name, "old_data file is empty")
#
#     else:
#         new_data.to_csv(os.path.join(read_path1,csv_name))
#         print(csv_name,"not found in old file list")

import pandas as pd
import os
import requests
import json
from datamaster import dm_client


underlying_type='HK_STOCK'
if underlying_type == "US_STOCK":
    r = requests.get(
        'https://algo-internal.aqumon.com/datamaster/exchange_calendar/?start_date=' + '2000-01-01' + '&end_date=' + '2022-12-31' + '&ex=' + 'NYSE')
elif underlying_type == "HK_STOCK":
    r = requests.get(
        'https://algo-internal.aqumon.com/datamaster/exchange_calendar/?start_date=' + '2000-01-01' + '&end_date=' + '2022-12-31' + '&ex=' + 'SEHK')
elif underlying_type == "CN_STOCK":
    r = requests.get(
        'https://algo-internal.aqumon.com/datamaster/exchange_calendar/?start_date=' + '2000-01-01' + '&end_date=' + '2022-12-31' + '&ex=' + 'SSE')

user_info = r.content.decode()
user_dict = json.loads(user_info)

calendar = pd.DataFrame(user_dict)
calendar.sort_values(by='data', inplace=True)
calendar.index = pd.to_datetime(calendar['data'], format='%Y-%m-%d')

read_path1 = r'/Users/lizhirui/PycharmProjects/aqumon-theme-portfolio-quantcycle/data/cn_data/trading'
save_path = r'/Users/lizhirui/PycharmProjects/aqumon-theme-portfolio-quantcycle/data/12_31_data/cn_data/trading'
os.chdir(read_path1)
csv_name_list1 = os.listdir()
csv_name_list2=pd.read_csv(r'/Users/lizhirui/PycharmProjects/aqumon-theme-portfolio-quantcycle/data/cn_data/symbol_list.csv')['symbol'].tolist()
dm_client.start()
start_date = '1999-12-31'
nodata_in_dm_list=[]
for csv_name in csv_name_list2:
    try:
        symbol=csv_name
        print(symbol)
        tmp = pd.DataFrame(
            columns=['date', 'PX_OPEN_RAW', 'PX_HIGH_RAW', 'PX_LOW_RAW', 'PX_LAST_RAW', 'PX_VOLUME_RAW',
                     'DAY_TO_DAY_TOT_RETURN_GROSS_DVDS', 'EQY_SH_OUT_RAW', 'MARKET_CAP',
                     'DAY_TO_DAY_TOT_RETURN_GROSS_DVDS_PRODUCT', 'adjust_factor', 'PX_LAST'])
        data = dm_client.historical(symbols=symbol, start_date=start_date,
                                    fields=['adjust_factor', 'choice_mktcap', 'volume', 'close',
                                            'open', 'high', 'low', 'bbg_daily_total_return',
                                            'choice_total_share'])
        data = pd.DataFrame(data['values'][symbol], columns=data['fields'])
        data['date'] = pd.to_datetime(data['date'])
        if len(data['adjust_factor'].dropna()) == 0:
            data['adjust_factor'] = 1
        else:
            data['adjust_factor'] = data['adjust_factor'].ffill().bfill()

        tmp['date'] = data['date']
        tmp['PX_OPEN_RAW'] = data['open']
        tmp['PX_HIGH_RAW'] = data['high']
        tmp['PX_LOW_RAW'] = data['low']
        tmp['PX_LAST_RAW'] = data['close']
        tmp['PX_VOLUME_RAW'] = data['volume']
        tmp['MARKET_CAP'] = data['choice_mktcap']
        tmp['adjust_factor'] = data['adjust_factor']
        tmp['DAY_TO_DAY_TOT_RETURN_GROSS_DVDS'] = data['bbg_daily_total_return']
        tmp['PX_LAST'] = tmp['PX_LAST_RAW'] * tmp['adjust_factor']
        tmp['PX_VOLUME'] = tmp['PX_VOLUME_RAW'] / tmp['adjust_factor']
        tmp['EQY_SH_OUT'] = data['choice_total_share']
        tmp['PX_LAST'] = tmp['PX_LAST'].ffill()
        tmp.set_index('date', inplace=True)

        tmp = tmp.loc[calendar.loc[tmp.index[0]:tmp.index[-1], :].index, :]
        tmp = tmp.dropna(subset=['PX_VOLUME'])
        # data_new=pd.concat([data_old,tmp],axis=0)
        tmp.to_csv(os.path.join(save_path, symbol + '.csv'))
        # print(tmp)
    except:
        print(csv_name,"has no data in dm")
        nodata_in_dm_list.append(symbol)
        # data_old=pd.read_csv(os.path.join(read_path1,csv_name+'.csv'),index_col=0,parse_dates=[0])
        # data_old.to_csv(os.path.join(save_path,csv_name))

print(nodata_in_dm_list)





