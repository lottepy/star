import pandas as pd
from algorithm import addpath
import os

portfolio_name_list = ['CN_Quality','CN_Value','HK_Hstech_B','HK_Hstech_S',
                        'US_Profit_B','US_Profit_S','US_Safety_B','US_Safety_S',
                        'US_Tech_B','US_Tech_S']
# portfolio_name_list = ['HK_Hstech_B']

ind_all=[]
sector_mapping=pd.read_csv('/Users/lizhirui/Desktop/AQUMON/smart-equity-2b/data/smart_equity/sector_mapping.csv',index_col=0)
# sector_mapping.index=[str(ele) for ele in sector_mapping.index]
output_path='/Users/lizhirui/Desktop/AQUMON/smart-equity-2b/data/smart_equity/20210118/'
if os.path.exists(output_path):
    pass
else:
    os.makedirs(output_path)
rebalance_date="2020-12-31.csv"
task="backtesting"
all_symbol_list=[]
backup_list=pd.DataFrame(index=portfolio_name_list,columns=['backup_list'])
backup_list.index.name='portfolio'

for portfolio_name in portfolio_name_list:
    print(portfolio_name)
    portfolio_path = os.path.join(addpath.data_path, "strategy_temps", portfolio_name, "portfolios")
    data=pd.read_csv(os.path.join(portfolio_path,rebalance_date),index_col=0)
    ind_list=data['industry'].tolist()
    ind_list=list(set(ind_list))
    dataout=pd.DataFrame(index=ind_list,columns=['NON_NON_100'])
    for ind in ind_list:
        dataout.loc[ind,'NON_NON_100']=data[data['industry']==ind]['weight'].sum()
    ind_all.extend(ind_list)
    if portfolio_name in ['HK_Hstech_B', 'HK_Hstech_S']:
        pass
    else:
        ind_list = [str(int(ind)) for ind in ind_list]
    ind_list=sector_mapping.loc[ind_list,'变量名'].tolist()
    dataout.index=ind_list
    print(dataout)

    dataout.to_csv(output_path+'distribution_'+portfolio_name+'.csv')


    weightout=pd.DataFrame(index=data.index,columns=['NON_NON_100'])
    weightout['NON_NON_100']=data['weight']
    all_symbol_list.extend(weightout.index.tolist())
    if portfolio_name[:2]=='CN':
        weightout.index=['CN_10_'+ind[:6] for ind in weightout.index]
    if portfolio_name[:2] == 'HK':
        weightout.index = ['HK_10_' + ind[:-10] for ind in weightout.index]
    if portfolio_name[:2] == 'US':
        weightout.index = ['US_10_' + ind for ind in weightout.index]
    print(weightout)
    # weightout.to_csv(output_path+'weight_'+portfolio_name+'.csv')

    data=pd.read_csv(os.path.join(addpath.result_path,task,portfolio_name,"portfolio value.csv"),index_col=0)
    pvout=pd.DataFrame(index=data.index,columns=['NON_NON_100'])
    pvout['NON_NON_100']=data[portfolio_name]
    # pvout.to_csv(output_path+'pv_'+portfolio_name+'.csv')

    backup_list_input=pd.read_csv(os.path.join(portfolio_path,'num_universe.csv'))['backup_list'].tolist()[-1]
    backup_list.loc[portfolio_name,'backup_list']=backup_list_input
ind_all=list(set(ind_all))
print(ind_all)
print(all_symbol_list)
all_symbol_df=pd.DataFrame(all_symbol_list,columns=['symbol'])
# all_symbol_df.to_csv(output_path+)
# backup_list.to_csv(output_path+'backup_list.csv')













