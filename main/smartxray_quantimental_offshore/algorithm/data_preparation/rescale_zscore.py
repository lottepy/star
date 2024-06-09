from os import makedirs, listdir
from os.path import exists, join, normpath
import pandas as pd
import numpy as np
from algorithm.addpath import config_path, data_path, result_path
import configparser
from datetime import datetime
from dateutil.relativedelta import relativedelta

# start_date = '2016-02-29'
def generate_expected_returns(start_date, return_df):
    # config_file_path = join(config_path, "config.conf")
    # config = configparser.ConfigParser()
    # config.read(config_file_path)

    # start_date = str(config['backtest_info']['start_date'])
    # hrz = str(config['model_info']['prediction_horizon'])
    hrz = '12'
    score_path = join(data_path, 'predictions', 'scores')
    output_path = join(data_path, 'predictions', 'expected_returns', hrz + 'M')
    if exists(output_path):
        pass
    else:
        makedirs(output_path)

    file_list = listdir(join(score_path, hrz + 'M'))
    # 删去.DS_Store文件
    try:
        file_list.remove('.DS_Store')
    except ValueError:
        print('No .DS_Store file')

    dates_list = [datetime.strptime(c[:-4], "%Y-%m-%d") for c in file_list]
    dates_list = [c for c in dates_list if c >= datetime.strptime(start_date, "%Y-%m-%d")]
    dates_list.sort()

    for date in dates_list:
        filename = date.strftime('%Y-%m-%d') + '.csv'
        score_ori_df_tmp = pd.read_csv(join(score_path, hrz + 'M', filename), index_col=0)
        score_ori_df_tmp = score_ori_df_tmp[['score', 'category']]
        funds_list_tmp = score_ori_df_tmp.index.tolist()
        return_df_tmp = return_df[np.logical_and(return_df.index >= date - relativedelta(years = 1),
                                                 return_df.index < date)][funds_list_tmp].transpose()
        ann_ret_df = pd.DataFrame((return_df_tmp + 1).prod(1), columns = ['ann_ret'])
        ann_ret_df['category'] = score_ori_df_tmp['category']

        mean_tmp = ann_ret_df.groupby('category').mean() - 1
        mean_tmp.columns = ['his_mean']
        std_tmp = ann_ret_df.groupby('category').std()
        std_tmp.columns = ['his_std']

        score_df_tmp = pd.merge(score_ori_df_tmp, mean_tmp, left_on = 'category', right_index= True, how = 'left')
        score_df_tmp = pd.merge(score_df_tmp, std_tmp,left_on = 'category', right_index= True, how = 'left')

        score_df_tmp['score_history_withoutds'] = score_df_tmp['score'] * score_df_tmp['his_std'] + score_df_tmp['his_mean']
        score_df_tmp = score_df_tmp[['score', 'category', 'score_history_withoutds']]

        score_df_tmp.to_csv(join(output_path, filename))
        print(filename)


if __name__ == '__main__':
    foropt_path = join(data_path, 'foropt')
    return_df = pd.read_csv(join(foropt_path, 'return.csv'), parse_dates=[0], index_col=0)
    generate_expected_returns(return_df = return_df)