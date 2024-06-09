# import pandas as pd
# import numpy as np
# import os
# from datetime import datetime, timedelta
# import pickle
# from algorithm import addpath
# from constants import *
# from algorithm.utils.portfolio_config_loader import portfolio_config_loader
# import pandas as pd
# import statsmodels.api as sm
# import matplotlib.pyplot as plt
# import warnings
# warnings.filterwarnings("ignore")
#
# portfolio_name = "US_Trading_factortest"
# symbol_list, symbol_industry, start, end, cash, commission, lookback_days, rebalance_freq, base_ccy, underlying_type, \
# number_assets, weighting_approach, market_cap_threshold, upper_bound, lower_bound = portfolio_config_loader(
#     portfolio_name)
# input_path = os.path.join(addpath.data_path, portfolio_name)
# cum_return = pd.read_csv(os.path.join(addpath.result_path, portfolio_name, 'cumulative_return.csv'))
# cum_return['date'] = pd.to_datetime(cum_return['datetime'], format='%Y/%m/%d')
# cum_return.set_index('date', inplace=True)
# cum_return = cum_return.drop(columns=['datetime'])
#
# if underlying_type == "US_STOCK":
#     input_path = os.path.join(addpath.data_path, "us_data")
# elif underlying_type == "HK_STOCK":
#     input_path = os.path.join(addpath.data_path, "hk_data")
# elif underlying_type == "CN_STOCK":
#     input_path = os.path.join(addpath.data_path, "cn_data")
# reference = pd.read_csv(os.path.join(input_path, 'reference', 'market_index.csv'))
# reference['date'] = pd.to_datetime(reference['date'], format='%Y/%m/%d')
# reference.set_index('date', inplace=True)
# reference = reference.loc[cum_return.index[0]:cum_return.index[-1], :]
# reference = reference.resample('1D').ffill()
# reference = reference / reference.iloc[0, 0] - 1
# data = pd.concat([cum_return, reference], axis=1)
#
#
# print(data)
#
# x=data.iloc[:,1]
# y=data.iloc[:,0]
# x = sm.add_constant(x) # 若模型中有截距，必须有这一步
# model = sm.OLS(y, x).fit() # 构建最小二乘模型并拟合
# print(model.summary()) # 输出回归结果
#

# #提取每个股票的行业信息
# import pandas as pd
# import numpy
# import os
# import warnings
# warnings.filterwarnings('ignore')
#
# #设置路径
# read_path = r'/Users/liyang/Desktop/AQUMON/aqumon-theme-portfolio/data/us_data/Quarterly Financial csv'
# os.chdir(read_path)
# csv_name_list = os.listdir()
# # csv_name_list.remove('desktop.ini')
# symbol_list=pd.DataFrame(index=csv_name_list,columns=['symbol','gicsector','gicgroup','gicind','gicsubind','standard_ind'])
#
# for i in range(len(csv_name_list)):
#     csv_name=csv_name_list[i]
#     print(csv_name[:-4])
#     symbol_list['symbol'][i]=csv_name[:-4]
#     datain=pd.read_csv(read_path+'//'+csv_name)
#     gsector=datain['gsector'].dropna().drop_duplicates()
#     if len(gsector.values)>0:
#         symbol_list['gicsector'][i] =gsector.values[0]
#
#     ggroup=datain['ggroup'].dropna().drop_duplicates()
#     if len(ggroup.values) > 0:
#         symbol_list['gicgroup'][i] =ggroup.values[0]
#
#     gind=datain['gind'].dropna().drop_duplicates()
#     if len(gind.values) > 0:
#         symbol_list['gicind'][i] =gind.values[0]
#
#     gsubind=datain['gsubind'].dropna().drop_duplicates()
#     if len(gsubind.values) > 0:
#         symbol_list['gicsubind'][i] =gsubind.values[0]
#
#     sic=datain['sic'].dropna().drop_duplicates()
#     if len(sic.values) > 0:
#         symbol_list['standard_ind'][i] =sic.values[0]
#
#     symbol_list.set_index('symbol',inplace=True)
#     symbol_list=symbol_list.dropna()
#     symbol_list.to_csv(r'/Users/liyang/Desktop/AQUMON/aqumon-theme-portfolio/data/us_data/symbol_industry.csv')
#

# #给产品计算aqumon近一年收益
# import pandas as pd
# import os
# from algorithm.utils.result_analysis import result_metrics_calculation
#
# pv_path= '/Users/liyang/Desktop/AQUMON/smart-global-2b/data/aqumon/20201221'
#
# strategy_list=['sg','sg_basic','sg_bond','shk','shk_basic','shk_bond']
# for strategy in strategy_list:
#     print(strategy)
#     data=pd.read_csv(os.path.join(pv_path,'pv_'+strategy+'.csv'),parse_dates=[0],index_col=0)
#     report=result_metrics_calculation(data)
#     report.to_csv(os.path.join(pv_path,'report_'+strategy+'.csv'))


#拼接个股介绍数据
import pandas as pd
import os
from constants import *

all_symbol=pd.read_csv('/Users/lizhirui/Desktop/all_symbol2.csv',index_col=0,encoding='utf-8_sig')
data_path=r'/Users/lizhirui/PycharmProjects/aqumon-theme-portfolio-quantcycle/data/strategy_temps'
backup_list=r'/Users/lizhirui/Desktop/AQUMON/smart-equity-2b/data/smart_equity/20210104/backup_list.csv'
all_symbol=all_symbol[['公司名称','简介']]
all_symbol=all_symbol.dropna(subset=['公司名称'])
all_symbol.index=[ele[6:] for ele in all_symbol.index]
sector_mapping=pd.read_excel('/Users/lizhirui/Desktop/AQUMON/smart-equity-2b/data/smart_equity/sector_mapping.xltx',index_col=0)
strategy_list=['CN_Quality','CN_Value','HK_Hstech_B','HK_Hstech_S',
                           'US_Profit_B','US_Profit_S','US_Safety_B','US_Safety_S',
                           'US_Tech_B','US_Tech_S']
# strategy_list=['CN_Quality','CN_Value','HK_Hstech_B','HK_Hstech_S',]

writer = pd.ExcelWriter('/Users/lizhirui/Desktop/RAW_DATA.xlsx')
# for strategy in strategy_list:
#     all_symbol = pd.read_excel('/Users/lizhirui/Desktop/stock_info.xlsx',sheet_name=strategy[:2], index_col = 0)
#     print(strategy)
#     pv_path = os.path.join(data_path,strategy,"portfolios","2020-12-31.csv")
#     data=pd.read_csv(pv_path,index_col=0)
#     criteria = PARAMETER_ALGO[strategy]['criteria']
#     criteria=[ele+'rank_pct in industry' for ele in criteria]
#     criteria.extend(['industry','rank','weight','MARKET_CAP','ind_num'])
#     if strategy in ['HK_Hstech_B','HK_Hstech_S']:
#         data=data[['industry','rank','weight','MARKET_CAP']]
#     else:
#         data=data[criteria]
#     # data=data[['weight']]
#     # if strategy[:2]=='CN':
#     #     data.index=['CN_10_'+ele[:-3] for ele in data.index]
#     # if strategy[:2]=='HK':
#     #     data.index=['HK_10_'+ele[:-10] for ele in data.index]
#     # if strategy[:2]=='US':
#     #     data.index=['US_10_'+ele for ele in data.index]
#
#     if strategy in ['CN_Quality','CN_Value',
#                            'US_Profit_B','US_Profit_S','US_Safety_B','US_Safety_S',
#                            'US_Tech_B','US_Tech_S']:
#
#         industry=[str(int(ele)) for ele in data['industry']]
#         industry=pd.DataFrame(sector_mapping.loc[data['industry'].tolist(), "中文"].tolist(),index=data.index,columns=['industry'])
#     data['industry']=industry
#     # data.index=[ele[:-3] for ele in data.index]
#     data_add=all_symbol.loc[data.index,:]
#     data=pd.concat([data_add,data],axis=1)
#     data.to_excel(writer,sheet_name=strategy,encoding='utf-8_sig')
#     print(strategy, 'data has been written')
# writer.save()






backup_df = pd.read_csv(backup_list, index_col=0)
strategy_list=['CN_Quality','CN_Value','HK_Hstech_B','HK_Hstech_S',
                           'US_Profit_B','US_Profit_S','US_Safety_B','US_Safety_S',
                           'US_Tech_B','US_Tech_S']
all=pd.DataFrame()

writer = pd.ExcelWriter('/Users/lizhirui/Desktop/all_symbol.xlsx')
for strategy in strategy_list:
    pv_path = os.path.join(data_path,strategy,"portfolios","2020-12-31.csv")
    weight=pd.read_csv(pv_path,index_col=0).index.tolist()

    backup_list = backup_df.loc[strategy, 'backup_list']
    backup_list = backup_list[:-1].split(',')
    backup_list = [ele[2:-1] for ele in backup_list]

    if strategy[:2] == 'CN':
        backup_list = ['CN_10_' + backup_list[i][:6] for i in range(len(backup_list))]
        weight = ['CN_10_' + weight[i][:6] for i in range(len(weight))]
    if strategy[:2] == 'HK':
        backup_list = ['HK_10_' + backup_list[i][:-10] for i in range(len(backup_list))]
        weight = ['HK_10_' + weight[i][:-10] for i in range(len(weight))]
    if strategy[:2] == 'US':
        backup_list = ['US_10_' + backup_list[i] for i in range(len(backup_list))]
        weight = ['US_10_' + weight[i] for i in range(len(weight))]
    backup_list.extend(weight)
    backup_list=pd.DataFrame(backup_list,columns=['symbol'])
    print(backup_list)
    backup_list.to_excel(writer,sheet_name=strategy,encoding='utf-8_sig',index=False)
    all=pd.concat([all,backup_list],axis=0)
    print(strategy, 'data has been written')
all.to_excel(writer,sheet_name='all',encoding='utf-8_sig',index=False)
print(all)
writer.save()













