import os, sys
from os.path import dirname, abspath, join
import pandas as pd
import numpy as np
import math
import datetime
from dateutil.relativedelta import relativedelta
from algorithm.addpath import config_path, data_path
from sklearn.decomposition import PCA
import configparser
from constant import new_category_list, new_category_mapping

def pcaCalculator(training_date):
    experiment = 'test2'
    new_dimension = 80
    # category_list = ['Equity_US', 'Index_Global', 'Equity_EM', 'Equity_Global', 'Equity_APAC', 'Equity_DMexUS', 'Bond_Global', 'Balance_Global', 'Bond_US', 'Alternative_Gold_Global', 'Alternative_Futures_Global']
    # category_list = ['Equity_Large']

    config_file_path = join(config_path, "config.conf")
    config = configparser.ConfigParser()
    config.read(config_file_path)
    training_sample_period = int(config['model_info']['training_sample_period_for_xgboost'])
    # training_sampling_freq = int(config['model_info']['training_sampling_freq_for_xgboost'])
    hrz = str(config['model_info']['prediction_horizon'])
    factor_start_date = datetime.datetime.strptime(str(config['model_info']['factor_data_start_date']), "%Y-%m-%d")

    category_path = join(data_path, 'categorization', 'fund_category')
    factors_path = join(data_path, 'factors', 'cs_factor_std')
    offshore_factors = pd.read_csv(join(data_path, 'cs_model', 'Factor Categorization and Single Factor Significance.csv'))['Cross-sectional Factor Name'].tolist()
    offshore_factors = [f + '_zscore' for f in offshore_factors]
    end_date = datetime.datetime.strptime(training_date, "%Y-%m-%d") - relativedelta(months=int(hrz))
    start_date = max(end_date - relativedelta(months=int(training_sample_period) - 1), factor_start_date)
    dates = pd.date_range(start_date, end_date, freq='M').tolist()

    for ctg in new_category_list:
        experiment_path = join(data_path, 'cs_model', 'pca_mapping', experiment, ctg)
        if not os.path.exists(experiment_path):
            os.makedirs(experiment_path)
        model_path = join(experiment_path, 'transition_matrix')
        if not os.path.exists(model_path):
            os.makedirs(model_path)
        explained_var_ls = []
        masterfile_list = []
        for date in dates:
            date = date.strftime(format='%Y-%m-%d')
            category_df = pd.read_csv(join(category_path, '{}.csv'.format(date)), index_col=0)
            factors = pd.read_csv(join(factors_path, "{}.csv".format(date)), index_col=0)
            tmp = pd.merge(category_df, factors, left_index=True, right_index=True)
            masterfile_list.append(tmp)
        masterfile = pd.concat(masterfile_list, axis=0)
        masterfile = masterfile.rename(columns=new_category_mapping)
        masterfile = masterfile[masterfile['category'] == ctg]
        masterfile = masterfile[['category'] + list(offshore_factors)]
        masterfile.replace([np.inf, -np.inf], np.nan, inplace=True)
        masterfile.dropna(how='all', axis=0, inplace=True)
        masterfile.dropna(how='all', axis=1, inplace=True)
        masterfile.fillna(0, inplace=True)
        master = masterfile[masterfile.columns[1:]]

        checked_dimension = min(new_dimension, master.shape[0], master.shape[1])
        if checked_dimension == 0:
            print("No data available at {} {}".format(ctg, training_date))
            continue
        if checked_dimension != new_dimension:
            print('{} {} sample data is not enough: samples={}, features={}'.format(ctg, training_date, master.shape[0], master.shape[1]))

        pca = PCA(n_components=checked_dimension)
        pca.fit_transform(master)
        explained_var = pd.DataFrame(pca.explained_variance_ratio_.cumsum())
        explained_var.index = range(1,checked_dimension+1)
        explained_var.columns = [ctg]
        explained_var_ls.append(explained_var)
        transition = pd.DataFrame(pca.components_.T, columns=range(1,checked_dimension+1), index=master.columns.tolist())
        # print(transition)
        save_path = join(model_path, "dim{}_{}.csv".format(checked_dimension, training_date))
        transition.to_csv(save_path)
        explained_var_df = pd.concat(explained_var_ls, axis=1)
        explained_var_df.to_csv(join(experiment_path, "{}_{}_explained_var.csv".format(training_date, experiment)))


def checkFactorCategorization(training_date):
    category_list = ['Equity_Large', 'Equity_Mid', 'Balance', 'Bond', 'Money', 'QD']
    selection_path = join(data_path, 'cs_model', 'selected')
    factors_path = join(data_path, 'factors', 'cs_factor_std')
    experiment = 'categorized_selection'

    config_file_path = join(config_path, "config.conf")
    config = configparser.ConfigParser()
    config.read(config_file_path)
    training_sample_period = int(config['model_info']['training_sample_period_for_xgboost'])
    training_sampling_freq = int(config['model_info']['training_sampling_freq_for_xgboost'])
    hrz = str(config['model_info']['prediction_horizon'])

    category_path = join(data_path, 'categorization', 'fund_category')
    end_date = datetime.datetime.strptime(training_date, "%Y-%m-%d") - relativedelta(months=int(hrz))
    start_date = end_date - relativedelta(months=int(training_sample_period) - 1)
    dates = pd.date_range(start_date, end_date, freq='M').tolist()

    for ctg in category_list:
        experiment_path = join(data_path, 'cs_model', 'pca_mapping', experiment, ctg)
        if not os.path.exists(experiment_path):
            os.makedirs(experiment_path)
        model_path = join(experiment_path, 'transition_matrix')
        if not os.path.exists(model_path):
            os.makedirs(model_path)
        selected_factors_tmp = pd.read_csv(join(selection_path, training_date, '{}_12.csv'.format(ctg)))
        selected_factors = selected_factors_tmp.loc[selected_factors_tmp['Select']==1, 'Cross-sectional Factor Name'].tolist()
        new_dimension = len(selected_factors)
        masterfile_list = []
        explained_var_ls = []
        for date in dates:
            date = date.strftime(format='%Y-%m-%d')
            category_df = pd.read_csv(join(category_path, '{}.csv'.format(date)), index_col=0)
            factors = pd.read_csv(join(factors_path, "{}.csv".format(date)), index_col=0)
            tmp = pd.merge(category_df, factors, left_index=True, right_index=True)
            masterfile_list.append(tmp)
        masterfile = pd.concat(masterfile_list, axis=0)
        masterfile = masterfile[['category'] + list(selected_factors)]  # differentiate
        masterfile = masterfile[masterfile['category'] == ctg]
        masterfile.replace([np.inf, -np.inf], np.nan, inplace=True)
        masterfile.dropna(how='all', axis=0, inplace=True)
        masterfile.dropna(how='all', axis=1, inplace=True)
        masterfile.fillna(0, inplace=True)
        master = masterfile[masterfile.columns[1:]] # exclude column 'category'

        checked_dimension = min(new_dimension, master.shape[0], master.shape[1])
        if checked_dimension == 0:
            print("No data available at {} {}".format(ctg, training_date))
            continue
        if checked_dimension != new_dimension:
            print('{} {} sample data is not enough: samples={}, features={}'.format(ctg, training_date, master.shape[0],
                                                                                    master.shape[1]))
        pca = PCA(n_components=checked_dimension)
        fit_res = pca.fit_transform(master)
        explained_var = pd.DataFrame(pca.explained_variance_ratio_.cumsum())
        explained_var.index = range(1, checked_dimension + 1)
        explained_var.columns = [ctg]
        explained_var_ls.append(explained_var)
        transition = pd.DataFrame(pca.components_.T, columns=range(1, checked_dimension + 1),
                                  index=master.columns.tolist())
        save_path = join(model_path, "dim{}_{}.csv".format(checked_dimension, training_date))
        transition.to_csv(save_path)
        explained_var_df = pd.concat(explained_var_ls, axis=1)
        explained_var_df.to_csv(join(experiment_path, "{}_{}_explained_var.csv".format(training_date, experiment)))


if __name__ == '__main__':
    start_year = 2013
    now_date = datetime.datetime.now().date()
    if now_date >= datetime.datetime(datetime.datetime.now().year, 5, 31).date():
        end_year = datetime.datetime.now().year
    else:
        end_year = datetime.datetime.now().year - 1

    dates = [datetime.datetime(i, 5, 31) for i in range(start_year, end_year + 1)]
    for date in dates:
        pcaCalculator(training_date=date.strftime(format='%Y-%m-%d'))
        # checkFactorCategorization(training_date=date.strftime(format='%Y-%m-%d'))
    pcaCalculator(training_date='2020-04-30')