from os import makedirs, listdir
from os.path import exists, join, normpath
import pandas as pd
import numpy as np
from algorithm.addpath import config_path, data_path, result_path
from datetime import datetime
from dateutil.relativedelta import relativedelta
# #
# start_date ='2016-02-29'
# end_date ='2020-04-30'
# model_freq ='12M'
# rescale = 'history_withoutds'

def funds_pool(start_date, end_date, model_freq, rescale, price_df):
    # foropt_path = join(data_path, 'foropt')
    score_path = join(data_path, 'predictions', 'expected_returns')
    pool_path = join(data_path, 'pool')
    savepath = join(pool_path, model_freq)

    if exists(savepath):
        pass
    else:
        makedirs(savepath)

    class_list = ['Global', 'China', 'US', 'European DM', 'APAC', 'Emerging Market',
                  'Finance', 'Healthcare', 'Consumption', 'Technology',
                  'Global IG', 'US IG', 'Other IG', 'High Yield']

    white_list_book_dict = {}
    # selected_funds_his_dict = {}
    for clas in class_list:
        white_list_byclass_df_tmp = pd.read_excel(join(config_path, 'white_list_Wing_Lung.xlsx'), sheet_name =clas, index_col='ms_secid')
        white_list_byclass_df_tmp = white_list_byclass_df_tmp[['Rank in the Category', 'status']]
        white_list_byclass_df_tmp['status'] = white_list_byclass_df_tmp['status'].fillna('selected')
        # white_list_byclass_df_tmp = white_list_byclass_df_tmp.dropna()
        white_list_book_dict[clas] = white_list_byclass_df_tmp
        # selected_funds_his_dict[clas] = np.nan

    # file_list = listdir(join(score_path, model_freq))
    # try:
    #     file_list.remove('.DS_Store')
    # except ValueError:
    #     print('No .DS_Store file')

    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    rebalance_dates_df = pd.read_csv(join(config_path, 'rebalance_dates.csv'), parse_dates=[0])
    dates_list = rebalance_dates_df[np.logical_and(rebalance_dates_df['rebalance_dates']>=start_dt,
                                                   rebalance_dates_df['rebalance_dates']<=end_dt)]['rebalance_dates'].tolist()

    rebalance_dict = {
        0: ['Global', 'US IG', 'China'],
        1: ['US','Finance', 'Healthcare', 'Consumption', 'Technology'],
        2: ['European DM', 'Other IG'],
        3: ['APAC', 'Emerging Market', 'Global IG', 'High Yield']
                        }
    dates_list.sort()

    kept_fund_df_tmp = pd.DataFrame()
    selected_fund_his_df_tmp = pd.DataFrame()
    for date in dates_list:
        if date.strftime('%Y-%m-%d') == '2020-08-31':
            continue
        price_df_tmp = price_df[np.logical_and(price_df.index > date - relativedelta(months = 4),
                                                price_df.index <= date-relativedelta(months = 3))]
        price_df_tmp = price_df_tmp.dropna(axis = 1, how = 'all')
        funds_list_tmp = price_df_tmp.columns.tolist()
        filename = date.strftime('%Y-%m-%d') + '.csv'
        score_ori_df_tmp = pd.read_csv(join(score_path, model_freq, filename), index_col=0)
        score_name = 'score_' + rescale


        if date == dates_list[0]:
            rebalance_list = class_list
            kept_fund_df_tmp = pd.DataFrame()
        else:
            rebalance_num = dates_list.index(date) % len(rebalance_dict)
            rebalance_list = rebalance_dict[rebalance_num]
            kept_fund_df_tmp = selected_fund_his_df_tmp.loc[~selected_fund_his_df_tmp['class'].isin(rebalance_list)]

        selected_fund_list1 = []
        for clas in rebalance_list:
            if clas in ['Global IG', 'Other IG', 'US IG', 'High Yield']:
                score_ori_df_tmp_1 = pd.read_csv(join(score_path, model_freq, date.strftime('%Y-%m-%d'), 'High Risk Bond.csv'),
                                               index_col=0)
                score_ori_df_tmp_2 = pd.read_csv(join(score_path, model_freq, date.strftime('%Y-%m-%d'), 'Low Risk Bond.csv'),
                                                 index_col=0)
                score_ori_df_tmp = pd.concat([score_ori_df_tmp_1, score_ori_df_tmp_2])
            elif clas in ['Finance', 'Healthcare', 'Consumption', 'Technology']:
                score_ori_df_tmp_1 = pd.read_csv(join(score_path, model_freq, date.strftime('%Y-%m-%d'), 'Equity-APAC.csv'),
                                               index_col=0)
                score_ori_df_tmp_2 = pd.read_csv(join(score_path, model_freq, date.strftime('%Y-%m-%d'), 'Equity-China.csv'),
                                                 index_col=0)
                score_ori_df_tmp_3 = pd.read_csv(join(score_path, model_freq, date.strftime('%Y-%m-%d'), 'Equity-EM.csv'),
                                               index_col=0)
                score_ori_df_tmp_4 = pd.read_csv(join(score_path, model_freq, date.strftime('%Y-%m-%d'), 'Equity-EU.csv'),
                                                 index_col=0)
                score_ori_df_tmp_5 = pd.read_csv(join(score_path, model_freq, date.strftime('%Y-%m-%d'), 'Equity-Global.csv'),
                                               index_col=0)
                score_ori_df_tmp_6 = pd.read_csv(join(score_path, model_freq, date.strftime('%Y-%m-%d'), 'Equity-US.csv'),
                                                 index_col=0)
                score_ori_df_tmp = pd.concat([score_ori_df_tmp_1, score_ori_df_tmp_2, score_ori_df_tmp_3, score_ori_df_tmp_4,
                                              score_ori_df_tmp_5, score_ori_df_tmp_6])
            elif clas == 'Global':
                score_ori_df_tmp = pd.read_csv(join(score_path, model_freq, date.strftime('%Y-%m-%d'), 'Equity-Global.csv'),
                                               index_col=0)
            elif clas == 'China':
                score_ori_df_tmp = pd.read_csv(join(score_path, model_freq, date.strftime('%Y-%m-%d'), 'Equity-China.csv'),
                                               index_col=0)
            elif clas == 'US':
                score_ori_df_tmp = pd.read_csv(join(score_path, model_freq, date.strftime('%Y-%m-%d'), 'Equity-US.csv'),
                                               index_col=0)
            elif clas == 'European DM':
                score_ori_df_tmp = pd.read_csv(join(score_path, model_freq, date.strftime('%Y-%m-%d'), 'Equity-EU.csv'),
                                               index_col=0)
            elif clas == 'APAC':
                score_ori_df_tmp = pd.read_csv(join(score_path, model_freq, date.strftime('%Y-%m-%d'), 'Equity-APAC.csv'),
                                               index_col=0)
            elif clas == 'Emerging Market':
                score_ori_df_tmp = pd.read_csv(join(score_path, model_freq, date.strftime('%Y-%m-%d'), 'Equity-EM.csv'),
                                               index_col=0)
            else:
                filename = clas + '.csv'
                score_ori_df_tmp = pd.read_csv(join(score_path, model_freq, date.strftime('%Y-%m-%d'), filename),
                                               index_col=0)
            score_name = 'score'
            white_list_byclass_df_tmp = white_list_book_dict[clas]
            white_list_byclass_df_tmp = white_list_byclass_df_tmp[white_list_byclass_df_tmp.index.isin(funds_list_tmp)]
            if white_list_byclass_df_tmp.empty:
                print(clas, 'is empty  in', date.date())
            else:
                white_list_byclass_df_tmp[score_name] = score_ori_df_tmp.copy().loc[
                                                            white_list_byclass_df_tmp.index, score_name]
                white_list_byclass_df_tmp = white_list_byclass_df_tmp.sort_values(by=score_name, ascending=False)
                if date >= dates_list[-len(rebalance_dict)]:
                    white_list_byclass_df_tmp = white_list_byclass_df_tmp[white_list_byclass_df_tmp['status'] == 'p']
                else:
                    # white_list_byclass_df_tmp = white_list_byclass_df_tmp.sort_values(by = 'Rank in the Category', ascending = True)
                    white_list_byclass_df_tmp = white_list_byclass_df_tmp.copy().iloc[[0], :]
                white_list_byclass_df_tmp['class'] = clas
            selected_fund_list1.append(white_list_byclass_df_tmp)
        selected_fund_df_tmp1 = pd.concat(selected_fund_list1, axis = 0)

        selected_fund_df_tmp = pd.concat([kept_fund_df_tmp, selected_fund_df_tmp1], axis = 0)
        selected_fund_his_df_tmp = selected_fund_df_tmp.copy()
        selected_fund_df_tmp.to_csv(join(savepath, date.strftime('%Y-%m-%d') + '.csv'))
        print('Succeed in creating the funds pool for date ', date.date(), ' with ', len(selected_fund_df_tmp), ' funds')

if __name__ == '__main__':
    foropt_path = join(data_path, 'foropt')
    price_df = pd.read_csv(join(foropt_path, 'price.csv'), parse_dates=[0], index_col=0)
    funds_pool(start_date = '2016-02-29', end_date = '2020-12-31', model_freq = 'hrz_6', price_df = price_df, rescale='history_withoutds')
    # funds_pool(start_date = '2020-07-31', end_date = '2020-08-31', model_freq = 'hrz_6', price_df = price_df)