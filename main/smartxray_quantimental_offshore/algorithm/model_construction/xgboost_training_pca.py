import numpy as np
import pandas as pd
import datetime
import xgboost as xgb
from os import makedirs
from os.path import exists, join, normpath
from algorithm.addpath import config_path, data_path
import configparser
import shap
from dateutil.relativedelta import relativedelta


def tuning_xgboost(X_train, y_train, X_test, y_test):
    dtrain = xgb.DMatrix(X_train, label=y_train)
    dtest = xgb.DMatrix(X_test, label=y_test)
    num_boost_round = 1000

    params = {
        # Parameters that we are going to tune.
        'max_depth': 6,
        'eta': 0.3,
        'min_child_weight': 1,
        'subsample': 1,
        'colsample_bytree': 1,
        # Other parameters
        'objective': 'reg:squarederror',
        'eval_metric': "mae",
        'tree_method': 'gpu_hist',  # Use GPU accelerated algorithm
        'gpu_id': 0
    }

    # Tuning max_depth and min_child_weight
    # First round
    gridsearch_params = [
        (max_depth, min_child_weight)
        for max_depth in [2, 5, 9, 12, 15]
        for min_child_weight in range(5, 25, 5)
    ]

    min_mae = float("Inf")
    best_params = None
    for max_depth, min_child_weight in gridsearch_params:
        print("CV with max_depth={}, min_child_weight={}".format(
            max_depth,
            min_child_weight))
        # Update our parameters
        params['max_depth'] = max_depth
        params['min_child_weight'] = min_child_weight
        # Run CV
        cv_results = xgb.cv(
            params,
            dtrain,
            num_boost_round=num_boost_round,
            seed=15,
            nfold=2,
            metrics=['mae'],
            early_stopping_rounds=10
        )
        # Update best MAE
        mean_mae = cv_results['test-mae-mean'].min()
        boost_rounds = cv_results['test-mae-mean'].idxmin()
        print("\tMAE {} for {} rounds".format(mean_mae, boost_rounds))
        if mean_mae < min_mae:
            min_mae = mean_mae
            best_params = (max_depth, min_child_weight)
    print("First round best params: max_depth is {}, min_child_weight is {}, MAE: {}".format(best_params[0], best_params[1], min_mae))

    # Second round
    gridsearch_params = [
        (max_depth, min_child_weight)
        for max_depth in range(max(best_params[0] - 2, 2), best_params[0] + 2)
        for min_child_weight in range(max(best_params[1] - 2, 5), best_params[1] + 2)
    ]

    min_mae = float("Inf")
    best_params = None
    for max_depth, min_child_weight in gridsearch_params:
        print("CV with max_depth={}, min_child_weight={}".format(
            max_depth,
            min_child_weight))
        # Update our parameters
        params['max_depth'] = max_depth
        params['min_child_weight'] = min_child_weight
        # Run CV
        cv_results = xgb.cv(
            params,
            dtrain,
            num_boost_round=num_boost_round,
            seed=15,
            nfold=2,
            metrics=['mae'],
            early_stopping_rounds=10
        )
        # Update best MAE
        mean_mae = cv_results['test-mae-mean'].min()
        boost_rounds = cv_results['test-mae-mean'].idxmin()
        print("\tMAE {} for {} rounds".format(mean_mae, boost_rounds))
        if mean_mae < min_mae:
            min_mae = mean_mae
            best_params = (max_depth, min_child_weight)
    print("Second round best params: max_depth is {}, min_child_weight is {}, MAE: {}".format(best_params[0], best_params[1], min_mae))

    params['max_depth'] = best_params[0]
    params['min_child_weight'] = best_params[1]

    # Tune subsample and colsample
    gridsearch_params = [
        (subsample, colsample)
        for subsample in [i / 10. for i in range(7, 11)]
        for colsample in [i / 10. for i in range(7, 11)]
    ]
    min_mae = float("Inf")
    best_params = None
    # We start by the largest values and go down to the smallest
    for subsample, colsample in reversed(gridsearch_params):
        print("CV with subsample={}, colsample={}".format(
            subsample,
            colsample))
        # We update our parameters
        params['subsample'] = subsample
        params['colsample_bytree'] = colsample
        # Run CV
        cv_results = xgb.cv(
            params,
            dtrain,
            num_boost_round=num_boost_round,
            seed=15,
            nfold=2,
            metrics=['mae'],
            early_stopping_rounds=10
        )
        # Update best score
        mean_mae = cv_results['test-mae-mean'].min()
        boost_rounds = cv_results['test-mae-mean'].idxmin()
        print("\tMAE {} for {} rounds".format(mean_mae, boost_rounds))
        if mean_mae < min_mae:
            min_mae = mean_mae
            best_params = (subsample, colsample)
    print("Best params: {}, {}, MAE: {}".format(best_params[0], best_params[1], min_mae))

    params['subsample'] = best_params[0]
    params['colsample_bytree'] = best_params[1]

    # Tune eta and num_boost_round
    params['eta'] = 0.01
    num_boost_round = 5000
    model = xgb.train(
        params,
        dtrain,
        num_boost_round=num_boost_round,
        evals=[(dtest, "Test")],
        early_stopping_rounds=10,
        verbose_eval=50
    )
    num_boost_round = model.best_iteration + 1
    model.__del__()
    best_model = xgb.train(
        params,
        dtrain,
        num_boost_round=num_boost_round,
        evals=[(dtest, "Test")],
        verbose_eval=50,
    )

    print("Best params: ", params)
    return best_model, params


def rolling_window_xgboost_model(training_date):
    model_path = join(data_path, 'cs_model')
    return_path = join(data_path, 'returns')
    factor_path = join(data_path, 'factors', 'cs_factor_std')
    category_path = join(data_path, 'categorization', 'fund_category')
    category_list = ['Equity_US', 'Index_Global', 'Equity_EM', 'Equity_Global', 'Equity_APAC', 'Equity_DMexUS', 'Bond_Global', 'Balance_Global', 'Bond_US', 'Alternative_Gold_Global', 'Alternative_Futures_Global']

    blacklist = pd.read_csv(join(data_path, 'blacklist.csv'), index_col=0).index.tolist()

    config_file_path = join(config_path, "config.conf")
    config = configparser.ConfigParser()
    config.read(config_file_path)
    hrz = str(config['model_info']['prediction_horizon'])
    training_sample_period = int(config['model_info']['training_sample_period_for_xgboost'])
    training_sampling_freq = int(config['model_info']['training_sampling_freq_for_xgboost'])

    if training_sampling_freq == 1:
        test_idx = [0, training_sample_period - 1, training_sample_period - 7, training_sample_period - 13]
        training_idx = [i for i in range(training_sample_period) if i not in test_idx]
    else:
        training_periods = int((training_sample_period - 2) / training_sampling_freq)
        max_available_training_idx = training_sample_period - 2 - training_periods * training_sampling_freq
        if max_available_training_idx == 2:
            test_idx_tmp = 1
        else:
            test_idx_tmp = 2
        training_idx = [max_available_training_idx + training_period * training_sampling_freq for training_period in range(training_periods + 1)]
        test_idx = [test_idx_tmp + training_period * training_sampling_freq for training_period in range(training_periods - 2, training_periods)] + [0, training_sample_period - 1]

    end_date = datetime.datetime.strptime(training_date, "%Y-%m-%d") - relativedelta(months=int(hrz))
    start_date = end_date - relativedelta(months=int(training_sample_period) - 1)
    dates = pd.date_range(start_date, end_date, freq='M').tolist()
    ret_tmp = pd.read_csv(join(return_path, '{}M_return.csv'.format(hrz)), parse_dates=[0], index_col='date')
    ret_tmp = ret_tmp.drop(columns=blacklist)

    draft_path = join(model_path, 'pca_mapping', 'test2')
    save_path1 = join(model_path, 'model_file', training_date)
    if exists(save_path1):
        pass
    else:
        makedirs(save_path1)
    save_path2 = join(model_path, 'shap', training_date)
    if exists(save_path2):
        pass
    else:
        makedirs(save_path2)
    save_path3 = join(model_path, 'importance', training_date)
    if exists(save_path3):
        pass
    else:
        makedirs(save_path3)
    save_path4 = join(model_path, 'hyper_parameters', training_date)
    if exists(save_path4):
        pass
    else:
        makedirs(save_path4)

    parameter_dict = {}
    for idx in category_list:
        transition_matrix = pd.read_csv(join(draft_path, idx, 'transition_matrix', "dim80_" + training_date + ".csv"), index_col=0)
        list_factors = transition_matrix.index.tolist()
        pca_factor_list = transition_matrix.columns
        # Prepare training sample and test sample
        training_sample_list = []
        test_sample_list = []

        for i in training_idx + test_idx:
            date = dates[i]
            ret = pd.DataFrame(ret_tmp.loc[date, :])
            ret = ret.dropna()
            ret.columns = ['fwd_return']

            factors_all = pd.read_csv(join(factor_path, date.strftime("%Y-%m-%d") + '.csv'), index_col=0)
            missing_factors = [fac for fac in list_factors if fac not in factors_all.columns]
            for fac in missing_factors:
                factors_all[fac] = 0
            factors_all = factors_all.loc[:, list_factors]
            factors_all = factors_all.fillna(0)
            pca_factors = np.dot(factors_all, transition_matrix)
            pca_factors = pd.DataFrame(pca_factors, index=factors_all.index, columns=pca_factor_list)

            category_df = pd.read_csv(join(category_path, date.strftime("%Y-%m-%d") + '.csv'), index_col=0)

            masterfile_tmp = pd.concat([ret, category_df, pca_factors], axis=1)
            masterfile_tmp = masterfile_tmp.dropna(subset=['fwd_return'])
            masterfile_tmp = masterfile_tmp[masterfile_tmp['category'] == idx]
            masterfile_tmp['fwd_returns_stdzed'] = (masterfile_tmp['fwd_return'] - masterfile_tmp[
                'fwd_return'].mean()) / masterfile_tmp['fwd_return'].std()
            masterfile_tmp['date'] = date

            if i in test_idx:
                test_sample_list.append(masterfile_tmp)
            else:
                training_sample_list.append(masterfile_tmp)

        training_sample = pd.concat(training_sample_list)
        test_sample = pd.concat(test_sample_list)

        training_sample = training_sample.replace([-np.inf, np.inf], np.nan)
        training_sample = training_sample.fillna(0)
        training_sample = training_sample.reset_index()
        test_sample = test_sample.replace([-np.inf, np.inf], np.nan)
        test_sample = test_sample.fillna(0)
        test_sample = test_sample.reset_index()

        X_train = training_sample.loc[:, pca_factor_list]
        y_train = training_sample['fwd_returns_stdzed']

        X_test = test_sample.loc[:, pca_factor_list]
        y_test = test_sample['fwd_returns_stdzed']

        best_model, params = tuning_xgboost(X_train, y_train, X_test, y_test)

        best_model.save_model(join(save_path1, idx + "_" + hrz + ".model"))

        parameter_dict[idx + "_" + hrz] = params
        f = open(join(save_path4, idx + "_" + hrz + 'parameters.txt'), 'w')
        f.write(str(parameter_dict))
        f.close()

        explainer = shap.TreeExplainer(best_model)
        shap_values = explainer.shap_values(X_test)
        shap_columns = [i + "_shap" for i in X_test.columns]
        shap_values_df = pd.DataFrame(shap_values, columns=shap_columns, index=X_test.index)
        shap_values_df['date'] = test_sample['date']
        shap_values_df['index'] = test_sample['index']
        shap_values_df = pd.merge(shap_values_df, test_sample, left_on=['date', 'index'],
                                  right_on=['date', 'index'])
        shap_values_df.to_csv(join(save_path2, idx + "_" + hrz + "_shap.csv"))

        xgb_fea_imp = pd.DataFrame(list(best_model.get_fscore().items()),
                                   columns=['feature', 'importance']).sort_values('importance', ascending=False)
        xgb_fea_imp.to_csv(join(save_path3, idx + "_" + hrz + ".csv"))
        best_model.__del__()


def create_score(score_date, model_date=None):
    score_date_dt = datetime.datetime.strptime(score_date, '%Y-%m-%d')
    if model_date == None:
        if score_date_dt.date() >= datetime.datetime(score_date_dt.year, 5, 31).date():
            training_date = datetime.datetime(score_date_dt.year, 5, 31).strftime(format='%Y-%m-%d')
        else:
            training_date = datetime.datetime(score_date_dt.year - 1, 5, 31).strftime(format='%Y-%m-%d')
    else:
        training_date = model_date

    blacklist = pd.read_csv(join(data_path, 'blacklist.csv'), index_col=0).index.tolist()

    factor_path = join(data_path, 'factors', 'cs_factor_std')
    draft_path = join(data_path, 'cs_model', 'pca_mapping', 'test2')
    category_path = join(data_path, 'categorization', 'fund_category')
    category_list = ['Equity_US', 'Index_Global', 'Equity_EM', 'Equity_Global', 'Equity_APAC', 'Equity_DMexUS', 'Bond_Global', 'Balance_Global', 'Bond_US', 'Alternative_Gold_Global', 'Alternative_Futures_Global']

    config_file_path = join(config_path, "config.conf")
    config = configparser.ConfigParser()
    config.read(config_file_path)
    hrz = str(config['model_info']['prediction_horizon'])

    model_path = join(data_path, 'cs_model', 'model_file', training_date)

    save_path1 = join(data_path, 'predictions', 'scores')
    if exists(save_path1):
        pass
    else:
        makedirs(save_path1)

    result_list = []
    for ctg in category_list:
        model_file_path = join(model_path, ctg + "_" + hrz + ".model")
        xgb_model = xgb.Booster()
        xgb_model.load_model(model_file_path)

        transition_matrix = pd.read_csv(join(draft_path, ctg, 'transition_matrix', "dim80_" + training_date + ".csv"),
                                        index_col=0)
        list_factors = transition_matrix.index.tolist()
        pca_factor_list = transition_matrix.columns

        factors_all = pd.read_csv(join(factor_path, score_date + '.csv'), index_col=0)
        missing_factors = [fac for fac in list_factors if fac not in factors_all.columns]
        for fac in missing_factors:
            factors_all[fac] = 0
        factors_all = factors_all.loc[:, list_factors]
        factors_all = factors_all.fillna(0)
        pca_factors = np.dot(factors_all, transition_matrix)
        pca_factors = pd.DataFrame(pca_factors, index=factors_all.index, columns=pca_factor_list)

        category_df = pd.read_csv(join(category_path, score_date + '.csv'), index_col=0)

        masterfile = pd.merge(category_df, pca_factors, left_index=True, right_index=True)
        masterfile = masterfile.dropna(subset=['category'])
        masterfile = masterfile[masterfile['category'] == ctg]
        masterfile = masterfile.fillna(0)
        predictors = xgb.DMatrix(masterfile.loc[:, pca_factor_list].values)
        masterfile['score'] = xgb_model.predict(predictors)
        masterfile = masterfile[['score', 'category']]
        result_list.append(masterfile)

    score_save_path = join(save_path1, hrz + "M")
    if exists(score_save_path):
        pass
    else:
        makedirs(score_save_path)

    scores = pd.concat(result_list)
    new_index = [ni for ni in scores.index if ni not in blacklist]
    scores = scores.loc[new_index, :]
    scores.to_csv(join(score_save_path, score_date + ".csv"))


if __name__ == '__main__':
    start_year = 2013
    now_date = datetime.datetime.now().date()
    if now_date >= datetime.datetime(datetime.datetime.now().year, 5, 31).date():
        end_year = datetime.datetime.now().year
    else:
        end_year = datetime.datetime.now().year - 1

    dates = [datetime.datetime(i, 5, 31) for i in range(start_year, end_year + 1)]
    for date in dates:
        rolling_window_xgboost_model(training_date=date.strftime(format='%Y-%m-%d'))

    rolling_window_xgboost_model(training_date='2020-04-30')

    config_file_path = join(config_path, "config.conf")
    config = configparser.ConfigParser()
    config.read(config_file_path)
    start_date = str(config['backtest_info']['start_date'])
    end_date = str(config['backtest_info']['end_date'])

    score_dates = pd.date_range(start_date, end_date, freq='M')
    for score_date in score_dates:
        if score_date == datetime.datetime(2020, 4, 30):
            create_score(score_date.strftime('%Y-%m-%d'), model_date='2020-04-30')
        else:
            create_score(score_date.strftime('%Y-%m-%d'))
