from os import makedirs, listdir
from os.path import exists, join, normpath
import pandas as pd
import numpy as np
from datetime import datetime
from algorithm.addpath import config_path, data_path, result_path
# from constant import ms_sector_dict
# this code is to construct dataframe for the convenience of mean-variance optimization.


def concat_ret(output_path):
    if exists(output_path):
        pass
    else:
        makedirs(output_path)

    bundles_path = join(data_path, 'bundles', 'daily')
    retfile_list = listdir(bundles_path)

    try:
        retfile_list.remove('.DS_Store')
    except ValueError:
        print('No .DS_Store file')

    ret_df_list = []

    for file in retfile_list:
        funds_df_tmp = pd.read_csv(join(bundles_path, file), parse_dates=[0], index_col=0)
        funds_df_tmp['daily_return'] = funds_df_tmp['close'].pct_change(fill_method='ffill')
        ret_df_tmp = funds_df_tmp[['daily_return']]

        funds = file[:-4]
        ret_df_tmp.columns = [funds]
        ret_df_list.append(ret_df_tmp)

    ret_df = pd.concat(ret_df_list, axis = 1)
    ret_df.to_csv(join(output_path, 'return.csv'))

    return ret_df

def concat_price(output_path):
    if exists(output_path):
        pass
    else:
        makedirs(output_path)

    bundles_path = join(data_path, 'bundles', 'daily')
    pricefile_list = listdir(bundles_path)

    try:
        pricefile_list.remove('.DS_Store')
    except ValueError:
        print('No .DS_Store file')

    class_list = ['Global', 'China', 'US', 'European DM', 'APAC', 'Emerging Market',
                  'Finance', 'Healthcare', 'Consumption', 'Technology',
                  'Global IG', 'US IG', 'Other IG', 'High Yield']

    funds_set = set()
    for clas in class_list:
        white_list_byclass_df_tmp = pd.read_excel(join(config_path, 'white_list_Wing_Lung.xlsx'), sheet_name =clas, index_col='ms_secid')
        white_list_byclass_df_tmp = white_list_byclass_df_tmp[['Rank in the Category', 'status']]
        white_list_byclass_df_tmp = white_list_byclass_df_tmp.dropna()
        funds_set.update(white_list_byclass_df_tmp.index.tolist())
    funds_list = list(funds_set)
    
    funds_list = listdir(r'D:\script\smartxray_quantimental_wlb\data\bundles\daily')
    funds_list = [ele[:-4] for ele in funds_list]

    price_df_list = []
    for fund in funds_list:
        file = fund + '.csv'
        funds_df_tmp = pd.read_csv(join(bundles_path, file), parse_dates=[0], index_col=0)
        price_df_tmp = funds_df_tmp[['close']]
        funds = file[:-4]
        price_df_tmp.columns = [funds]
        price_df_list.append(price_df_tmp)

    price_df = pd.concat(price_df_list, axis = 1)
    price_df.to_csv(join(output_path, 'price.csv'))


def concat_sector_allocation(output_path):
    if exists(output_path):
        pass
    else:
        makedirs(output_path)

    categorization_path = join(data_path, 'categorization')
    sa_path = join(categorization_path, 'sector_allocation')

    safile_list = listdir(sa_path)
    try:
        safile_list.remove('.DS_Store')
    except ValueError:
        print('No .DS_Store file')

    sa_df_list = []

    for file in safile_list:
        date = datetime.strptime(file[:-4], "%Y-%m-%d")
        sa_df_tmp = pd.read_csv(join(sa_path, file), index_col=0)
        sa_df_tmp['Funds'] =sa_df_tmp.index.map(lambda x: x[:-11])
        sa_df_tmp['date'] = date
        sa_df_tmp = sa_df_tmp.set_index('date')
        sa_df_list.append(sa_df_tmp)

    sa_df = pd.concat(sa_df_list, axis = 0)
    sa_df = sa_df.sort_index()
    # sa_df = sa_df.rename(columns = ms_sector_dict)
    sa_df.to_csv(join(output_path, 'sector_allocation.csv'))


def concat_country_allocation(output_path):
    if exists(output_path):
        pass
    else:
        makedirs(output_path)

    categorization_path = join(data_path, 'categorization')
    ca_path = join(categorization_path, 'country_allocation')

    cafile_list = listdir(ca_path)
    try:
        cafile_list.remove('.DS_Store')
    except ValueError:
        print('No .DS_Store file')

    ca_df_list = []

    for file in cafile_list:
        date = datetime.strptime(file[:-4], "%Y-%m-%d")
        ca_df_tmp = pd.read_csv(join(ca_path, file), index_col=0)
        ca_df_tmp['Funds'] =ca_df_tmp.index.map(lambda x: x[:-11])
        ca_df_tmp['date'] = date
        ca_df_tmp = ca_df_tmp.set_index('date')
        ca_df_list.append(ca_df_tmp)

    ca_df = pd.concat(ca_df_list, axis = 0)
    ca_df = ca_df.sort_index()
    ca_df.to_csv(join(output_path, 'country_allocation.csv'))


def concat_credit_quality(output_path):
    if exists(output_path):
        pass
    else:
        makedirs(output_path)

    categorization_path = join(data_path, 'categorization')
    cq_path = join(categorization_path, 'credit_quality')

    cqfile_list = listdir(cq_path)
    try:
        cqfile_list.remove('.DS_Store')
    except ValueError:
        print('No .DS_Store file')

    cq_df_list = []

    for file in cqfile_list:
        date = datetime.strptime(file[:-4], "%Y-%m-%d")
        cq_df_tmp = pd.read_csv(join(cq_path, file), index_col=0)
        cq_df_tmp['Funds'] =cq_df_tmp.index.map(lambda x: x[:-11])
        cq_df_tmp['date'] = date
        cq_df_tmp = cq_df_tmp.set_index('date')
        cq_df_list.append(cq_df_tmp)

    cq_df = pd.concat(cq_df_list, axis = 0)
    cq_df = cq_df.sort_index()
    cq_df.to_csv(join(output_path, 'credit_quality.csv'))


def concat_index_mc(output_path):
    if exists(output_path):
        pass
    else:
        makedirs(output_path)
    index_path = join(data_path, 'index')
    mc_df = pd.read_csv(join(index_path, 'index cap.csv'), parse_dates = [0], index_col = 0)

    cap_df1 = mc_df[['CRSPTM1 Index']].replace(0, np.nan)
    cap_df2 = mc_df[['FTREEURO Index']].replace(0, np.nan) * 1.2
    cap_df3 = mc_df[['MXAP Index']].replace(0, np.nan)
    cap_df4 = mc_df[['MXEF Index']].replace(0, np.nan) * 0.4

    cap_df = pd.concat([cap_df1, cap_df2, cap_df3, cap_df4], axis = 1)
    # cap_df.columns = ['US', 'European DM', 'APAC', 'Emerging Market']
    cap_df = cap_df.rename(columns = {'CRSPTM1 Index': 'US',
                                      'FTREEURO Index': 'European DM',
                                      'MXAP Index': 'APAC',
                                      'MXEF Index': 'Emerging Market'})
    # output_df = pd.merge(cap_df, mc_df, left_index=True, right_index=True, how = 'outer')
    cap_df.to_csv(join(output_path, 'index_market_cap.csv'))
    


if __name__ == '__main__':
    foropt_path = join(data_path, 'foropt')

    concat_price(output_path = foropt_path)
    # concat_sector_allocation(output_path=foropt_path)
    # concat_country_allocation(output_path = foropt_path)
    # concat_credit_quality(output_path = foropt_path)
    # concat_index_mc(output_path = foropt_path)


