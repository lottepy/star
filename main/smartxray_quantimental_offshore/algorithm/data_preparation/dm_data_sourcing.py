'''
This file is for downloading bundle data and factor data from DM.

'''
from os import makedirs, listdir
from os.path import exists, join
import pandas as pd
from algorithm.addpath import data_path, config_path

from datamaster import dm_client
dm_client.start()


def bundle_download():
    bundles_path = join(data_path, 'bundles')
    # funds_list_df = pd.read_excel(join(config_path, 'white_list_Wing_Lung.xlsx'), sheet_name = 'All', header=1)

    # funds_list = funds_list_df['ms_secid'].dropna().unique().tolist()
    
    pool_list = listdir(r'D:\script\smartxray_quantimental_wlb\data\pool\12M')
    
    pool_list = [pd.read_csv(join(r'D:\script\smartxray_quantimental_wlb\data\pool\12M', ele)) for ele in pool_list]
    funds_list = pd.concat(pool_list)['ms_secid'].unique().tolist()
    
    price_list = ['open', 'high', 'low', 'close']
    savepath = join(bundles_path, 'daily')

    if exists(savepath):
        pass
    else:
        makedirs(savepath)

    summary_list = []
    for fund in funds_list:
        
        funds_dict = dm_client.historical(symbols=fund, start_date='2000-01-04', fields='fund_fqnav')
        funds_df_tmp = pd.DataFrame.from_dict(funds_dict['values'][fund])
        if funds_df_tmp.empty:
            funds_df_tmp = pd.DataFrame(columns=funds_dict['fields'])
        else:
            funds_df_tmp.columns = funds_dict['fields']
            
        # if fund == 'FOGBR05KLY' or fund =='F00000MC3V':
        #     funds_df_tmp['fund_fqnav'] = funds_df_tmp['fund_fqnav']/funds_df_tmp['fund_fqnav'].iloc[0]
            
        bundle_df_tmp = funds_df_tmp.copy()
        for price in price_list:
            bundle_df_tmp[price] = funds_df_tmp['fund_fqnav']
        if not funds_df_tmp.empty:
            bundle_df_tmp['volume'] = 10000
            bundle_df_tmp['dividend'] = 0
            bundle_df_tmp['split'] = 1
            bundle_df_tmp = bundle_df_tmp[['date'] + price_list + ['volume', 'dividend', 'split']]
            bundle_df_tmp.to_csv(join(savepath, fund + '.csv'), index=False)
            print('Successfully download the bundle data from DataMaster for', fund)
            summary_list.append(fund)
        else:
            print('Create empty bundle data for', fund)
    summary = pd.DataFrame(index = summary_list)
    summary.index.name = 'Name'
    summary.to_csv(join(bundles_path, 'Summary.csv'))


def benchmark_download(bbgcode_list, start_date, end_date):
    index_df_list = []
    for index in bbgcode_list:
        index_dict = dm_client.historical(symbols=index, start_date=start_date, end_date = end_date, fields='close')
        index_df_tmp = pd.DataFrame.from_dict(index_dict['values'][index])
        if index_df_tmp.empty:
            index_df_tmp = pd.DataFrame(columns=index_dict['fields'])
        else:
            index_df_tmp.columns = index_dict['fields']
        index_df_tmp = index_df_tmp.set_index('date').rename(columns = {'close': index})
        index_df_list.append(index_df_tmp)

    index_df = pd.concat(index_df_list, axis = 1)
    return index_df

if __name__ == "__main__":
    bundle_download()