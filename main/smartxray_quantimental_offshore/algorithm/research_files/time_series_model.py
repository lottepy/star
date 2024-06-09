import numpy as np
import pandas as pd
import datetime
import statsmodels.api as sm
from statsmodels.tsa.api import VAR
from os import makedirs
from os.path import exists, join, normpath
from algorithm.addpath import config_path, data_path
from sklearn.preprocessing import StandardScaler
import configparser
from dateutil.relativedelta import relativedelta
import pmdarima as pm
from constant import ts_factor_list
from sklearn.decomposition import PCA


def time_series_model_construction(training_date, category):
    save_path = join(data_path, 'ts_model', 'DFM_params')
    if exists(save_path):
        pass
    else:
        makedirs(save_path)

    ts_factor_path = join(data_path, 'factors', 'time_series_factor', 'processed', 'ts_factors_for_ts_model.csv')
    ts_factors = pd.read_csv(ts_factor_path, parse_dates=[0], index_col=0)
    config_file_path = join(config_path, "config.conf")
    config = configparser.ConfigParser()
    config.read(config_file_path)
    hrz = str(config['model_info']['time_series_prediction_hrz'])

    end_date = datetime.datetime.strptime(training_date, "%Y-%m-%d")
    start_date = datetime.datetime.strptime(config['model_info']['time_series_prediction_sample_start_date'], '%Y-%m-%d')
    dates = pd.date_range(end=end_date, periods=int((end_date.year - start_date.year + 1) * 12 / int(hrz)), freq='3M')
    dates = [date for date in dates if date >= start_date]

    ts_factors = ts_factors.loc[dates, ts_factor_list]
    ts_factors = ts_factors.ffill()
    ts_factors = ts_factors.dropna(axis=1)

    return_path = join(data_path, 'returns', 'category_fwd_return_mean.csv')
    ret = pd.read_csv(return_path, parse_dates=[0], index_col=0)
    ret = ret.loc[dates[:-1], [category]]

    try:
        corr_raw = pd.merge(ret, ts_factors, left_index=True, right_index=True)
        corr = corr_raw.corr()
        corr['abs'] = corr[category].abs()
        corr = corr[corr['abs'] > 0.4]
        new_factor_list = corr.index.tolist()
        del corr_raw
        new_factor_list.remove(category)

        sample_in = ts_factors.loc[:, new_factor_list]
        sample_in = pd.DataFrame(StandardScaler().fit_transform(sample_in.values), columns=new_factor_list, index=sample_in.index)

        # pca = PCA().fit(sample_in)
        # evr = np.cumsum(pca.explained_variance_ratio_)
        # i = 0
        # while evr[i] < 0.8:
        #     i += 1

        i = 1
        pca = PCA(n_components=i + 1)
        factors = pd.DataFrame(pca.fit_transform(sample_in), columns = ['f' + str(n + 1) for n in range(i + 1)], index=sample_in.index)
        try:
            factor_model = VAR(factors)
            factor_model_results = factor_model.fit(maxlags=10, ic='aic')
            lag_order = factor_model_results.k_ar
            print(factor_model_results.summary())
            factors.loc[end_date + relativedelta(months=int(hrz)), :] = factor_model_results.forecast(factors.values[-lag_order:], 1)
        except:
            print("Failed VAR for " + category + " of " + training_date)
            factors.loc[end_date + relativedelta(months=int(hrz)), :] = [np.nan] * (i + 1)
            factors = factors.ffill()

        sample = pd.concat([ret, factors], axis=1)
        sample = sample.dropna()

        sxmodel = pm.arima.auto_arima(sample[[category]], exogenous=sample[['f' + str(n + 1) for n in range(i + 1)]],
                                      start_p=1, start_q=1,
                                      test='adf',
                                      max_p=3, max_q=3, m=1,
                                      start_P=0,
                                      d=None, D=0, trace=True,
                                      error_action='ignore',
                                      suppress_warnings=True,
                                      stepwise=True)

        sxmodel.summary()

        n_periods = 2
        fitted = sxmodel.predict(n_periods=n_periods, exogenous=factors.iloc[-2:, :], return_conf_int=False)
        predicted_mean_return = ((1 + fitted[0]) * (1 + fitted[1])) ** 2 - 1
    except:
        print("DFM Fail for " + category + " of " + training_date)
        ret = ret.dropna()

        sxmodel = pm.arima.auto_arima(ret[[category]], exogenous=None,
                                      start_p=1, start_q=1,
                                      test='adf',
                                      max_p=3, max_q=3, m=1,
                                      start_P=0,
                                      d=None, D=0, trace=True,
                                      error_action='ignore',
                                      suppress_warnings=True,
                                      stepwise=True)
        sxmodel.summary()
        n_periods = 2
        fitted = sxmodel.predict(n_periods=n_periods, return_conf_int=False)
        predicted_mean_return = ((1 + fitted[0]) * (1 + fitted[1])) ** 2 - 1

    # Control for extreme downside risk for stock funds
    if category in ['Balance', 'Equity_Large', 'Equity_Mid']:
        c12 = pd.read_csv(join(data_path, 'factors', 'time_series_factor', 'processed', 'c12.csv'), parse_dates=[0], index_col=0)
        look_back_dates = pd.date_range(end=datetime.datetime.strptime(training_date, "%Y-%m-%d"), freq='M', periods=12)
        c12 = c12.loc[look_back_dates, category]
        max_c12 = c12.max()
        if category == 'Balance' and max_c12 > 2:
            predicted_mean_return = predicted_mean_return - 0.2
        elif category == 'Equity_Large' and max_c12 > 2:
            predicted_mean_return = predicted_mean_return - 0.2
        elif category == 'Equity_Mid' and max_c12 > 1:
            predicted_mean_return = predicted_mean_return - 0.2

    return predicted_mean_return


# def time_series_model_construction(training_date, category):
#     save_path = join(data_path, 'ts_model', 'DFM_params')
#     if exists(save_path):
#         pass
#     else:
#         makedirs(save_path)
#
#     ts_factor_path = join(data_path, 'factors', 'time_series_factor', 'processed', 'ts_factors_for_ts_model.csv')
#     ts_factors = pd.read_csv(ts_factor_path, parse_dates=[0], index_col=0)
#     config_file_path = join(config_path, "config.conf")
#     config = configparser.ConfigParser()
#     config.read(config_file_path)
#     hrz = str(config['model_info']['time_series_prediction_hrz'])
#
#     end_date = datetime.datetime.strptime(training_date, "%Y-%m-%d")
#     start_date = datetime.datetime.strptime(config['model_info']['time_series_prediction_sample_start_date'], '%Y-%m-%d')
#     sample_range = int((end_date.year - start_date.year + 1) * 12 / int(hrz))
#     dates = [end_date - relativedelta(months=i * int(hrz)) for i in range(sample_range + 1) if end_date - relativedelta(months=i * int(hrz)) >= start_date]
#     dates.reverse()
#
#     ts_factors = ts_factors.loc[dates, ts_factor_list]
#     ts_factors = ts_factors.ffill()
#     ts_factors = ts_factors.dropna(axis=1)
#
#     return_path = join(data_path, 'returns', 'category_fwd_return_mean.csv')
#     ret = pd.read_csv(return_path, parse_dates=[0], index_col=0)
#     ret = ret.loc[dates[:-1], [category]]
#
#     try:
#         corr_raw = pd.merge(ret, ts_factors, left_index=True, right_index=True)
#         corr = corr_raw.corr()
#         corr['abs'] = corr[category].abs()
#         corr = corr[corr['abs'] > 0.4]
#         new_factor_list = corr.index.tolist()
#         del corr_raw
#         new_factor_list.remove(category)
#
#         k_factors = 2
#         factor_order = 1
#         error_order = 1
#         init_maxiter = 50
#         maxiter = 1000
#         sample_in = ts_factors.loc[:, new_factor_list]
#         sample_in = pd.DataFrame(StandardScaler().fit_transform(sample_in.values), columns=new_factor_list, index=sample_in.index)
#
#         mod = sm.tsa.DynamicFactor(sample_in, k_factors=k_factors, factor_order=factor_order, error_order=error_order,
#                                    enforce_stationarity=False)
#
#         initial_res = mod.fit(method='powell', disp=False, maxiter=init_maxiter)
#         try:
#             res = mod.fit(initial_res.params, disp=False, maxtier=maxiter)
#         except:
#             res = mod.fit(disp=False, maxiter=maxiter)
#
#         latent_factors = res.factors.filtered
#
#         factors_tmp = latent_factors.transpose()
#         columns = ['f{}'.format(i + 1) for i in range(k_factors)]
#         factors = pd.DataFrame(factors_tmp, columns=columns, index=ts_factors.index._mpl_repr())
#         params = res.params
#
#         f = open(join(save_path, category + "_" + hrz + '_model_params.txt'), 'w')
#         f.write(str(res.params))
#         f.close()
#
#         factors.loc[end_date + relativedelta(months=int(hrz)), 'f1'] = params['L1.f1.f1'] * factors.loc[
#             end_date, 'f1'] + params['L1.f2.f1'] * factors.loc[end_date, 'f2']
#         factors.loc[end_date + relativedelta(months=int(hrz)), 'f2'] = params['L1.f1.f2'] * factors.loc[
#             end_date, 'f1'] + params['L1.f2.f2'] * factors.loc[end_date, 'f2']
#         sample = pd.concat([ret, factors], axis=1)
#         sample = sample.dropna()
#
#         sxmodel = pm.arima.auto_arima(sample[[category]], exogenous=sample[['f1', 'f2']],
#                                       start_p=1, start_q=1,
#                                       test='adf',
#                                       max_p=3, max_q=3, m=1,
#                                       start_P=0,
#                                       d=None, D=0, trace=True,
#                                       error_action='ignore',
#                                       suppress_warnings=True,
#                                       stepwise=True)
#
#         sxmodel.summary()
#
#         n_periods = 2
#
#         fitted = sxmodel.predict(n_periods=n_periods, exogenous=factors.iloc[-2:, :], return_conf_int=False)
#         predicted_mean_return = ((1 + fitted[0]) * (1 + fitted[1])) ** 2 - 1
#     except:
#         try:
#             corr_raw = pd.merge(ret, ts_factors, left_index=True, right_index=True)
#             corr = corr_raw.corr()
#             corr['abs'] = corr[category].abs()
#             corr = corr[corr['abs'] > 0.45]
#             new_factor_list = corr.index.tolist()
#             del corr_raw
#             new_factor_list.remove(category)
#
#             k_factors = 2
#             factor_order = 1
#             error_order = 1
#             init_maxiter = 50
#             maxiter = 1000
#             sample_in = ts_factors.loc[:, new_factor_list]
#             sample_in = pd.DataFrame(StandardScaler().fit_transform(sample_in.values), columns=new_factor_list, index=sample_in.index)
#
#             mod = sm.tsa.DynamicFactor(sample_in, k_factors=k_factors, factor_order=factor_order,
#                                        error_order=error_order,
#                                        enforce_stationarity=False)
#
#             initial_res = mod.fit(method='powell', disp=False, maxiter=init_maxiter)
#             try:
#                 res = mod.fit(initial_res.params, disp=False, maxtier=maxiter)
#             except:
#                 res = mod.fit(disp=False, maxiter=maxiter)
#
#             latent_factors = res.factors.filtered
#
#             factors_tmp = latent_factors.transpose()
#             columns = ['f{}'.format(i + 1) for i in range(k_factors)]
#             factors = pd.DataFrame(factors_tmp, columns=columns, index=ts_factors.index._mpl_repr())
#             params = res.params
#
#             f = open(join(save_path, category + "_" + hrz + '_model_params.txt'), 'w')
#             f.write(str(res.params))
#             f.close()
#
#             factors.loc[end_date + relativedelta(months=int(hrz)), 'f1'] = params['L1.f1.f1'] * factors.loc[
#                 end_date, 'f1'] + params['L1.f2.f1'] * factors.loc[end_date, 'f2']
#             factors.loc[end_date + relativedelta(months=int(hrz)), 'f2'] = params['L1.f1.f2'] * factors.loc[
#                 end_date, 'f1'] + params['L1.f2.f2'] * factors.loc[end_date, 'f2']
#             sample = pd.concat([ret, factors], axis=1)
#             sample = sample.dropna()
#
#             sxmodel = pm.arima.auto_arima(sample[[category]], exogenous=sample[['f1', 'f2']],
#                                           start_p=1, start_q=1,
#                                           test='adf',
#                                           max_p=3, max_q=3, m=1,
#                                           start_P=0,
#                                           d=None, D=0, trace=True,
#                                           error_action='ignore',
#                                           suppress_warnings=True,
#                                           stepwise=True)
#
#             sxmodel.summary()
#
#             n_periods = 2
#
#             fitted = sxmodel.predict(n_periods=n_periods, exogenous=factors.iloc[-2:, :], return_conf_int=False)
#             predicted_mean_return = ((1 + fitted[0]) * (1 + fitted[1])) ** 2 - 1
#         except:
#             try:
#                 corr_raw = pd.merge(ret, ts_factors, left_index=True, right_index=True)
#                 corr = corr_raw.corr()
#                 corr['abs'] = corr[category].abs()
#                 corr = corr[corr['abs'] > 0.5]
#                 new_factor_list = corr.index.tolist()
#                 del corr_raw
#                 new_factor_list.remove(category)
#
#                 k_factors = 2
#                 factor_order = 1
#                 error_order = 1
#                 init_maxiter = 50
#                 maxiter = 1000
#                 sample_in = ts_factors.loc[:, new_factor_list]
#                 sample_in = pd.DataFrame(StandardScaler().fit_transform(sample_in.values), columns=new_factor_list, index=sample_in.index)
#
#                 mod = sm.tsa.DynamicFactor(sample_in, k_factors=k_factors, factor_order=factor_order,
#                                            error_order=error_order,
#                                            enforce_stationarity=False)
#
#                 initial_res = mod.fit(method='powell', disp=False, maxiter=init_maxiter)
#                 try:
#                     res = mod.fit(initial_res.params, disp=False, maxtier=maxiter)
#                 except:
#                     res = mod.fit(disp=False, maxiter=maxiter)
#
#                 latent_factors = res.factors.filtered
#
#                 factors_tmp = latent_factors.transpose()
#                 columns = ['f{}'.format(i + 1) for i in range(k_factors)]
#                 factors = pd.DataFrame(factors_tmp, columns=columns, index=ts_factors.index._mpl_repr())
#                 params = res.params
#
#                 f = open(join(save_path, category + "_" + hrz + '_model_params.txt'), 'w')
#                 f.write(str(res.params))
#                 f.close()
#
#                 factors.loc[end_date + relativedelta(months=int(hrz)), 'f1'] = params['L1.f1.f1'] * factors.loc[
#                     end_date, 'f1'] + params['L1.f2.f1'] * factors.loc[end_date, 'f2']
#                 factors.loc[end_date + relativedelta(months=int(hrz)), 'f2'] = params['L1.f1.f2'] * factors.loc[
#                     end_date, 'f1'] + params['L1.f2.f2'] * factors.loc[end_date, 'f2']
#                 sample = pd.concat([ret, factors], axis=1)
#                 sample = sample.dropna()
#
#                 sxmodel = pm.arima.auto_arima(sample[[category]], exogenous=sample[['f1', 'f2']],
#                                               start_p=1, start_q=1,
#                                               test='adf',
#                                               max_p=3, max_q=3, m=1,
#                                               start_P=0,
#                                               d=None, D=0, trace=True,
#                                               error_action='ignore',
#                                               suppress_warnings=True,
#                                               stepwise=True)
#
#                 sxmodel.summary()
#
#                 n_periods = 2
#
#                 fitted = sxmodel.predict(n_periods=n_periods, exogenous=factors.iloc[-2:, :], return_conf_int=False)
#                 predicted_mean_return = ((1 + fitted[0]) * (1 + fitted[1])) ** 2 - 1
#             except:
#                 try:
#                     corr_raw = pd.merge(ret, ts_factors, left_index=True, right_index=True)
#                     corr = corr_raw.corr()
#                     corr['abs'] = corr[category].abs()
#                     corr = corr[corr['abs'] > 0.35]
#                     new_factor_list = corr.index.tolist()
#                     del corr_raw
#                     new_factor_list.remove(category)
#
#                     k_factors = 2
#                     factor_order = 1
#                     error_order = 1
#                     init_maxiter = 50
#                     maxiter = 1000
#                     sample_in = ts_factors.loc[:, new_factor_list]
#                     sample_in = pd.DataFrame(StandardScaler().fit_transform(sample_in.values), columns=new_factor_list, index=sample_in.index)
#
#                     mod = sm.tsa.DynamicFactor(sample_in, k_factors=k_factors, factor_order=factor_order,
#                                                error_order=error_order,
#                                                enforce_stationarity=False)
#
#                     initial_res = mod.fit(method='powell', disp=False, maxiter=init_maxiter)
#                     try:
#                         res = mod.fit(initial_res.params, disp=False, maxtier=maxiter)
#                     except:
#                         res = mod.fit(disp=False, maxiter=maxiter)
#
#                     latent_factors = res.factors.filtered
#
#                     factors_tmp = latent_factors.transpose()
#                     columns = ['f{}'.format(i + 1) for i in range(k_factors)]
#                     factors = pd.DataFrame(factors_tmp, columns=columns, index=ts_factors.index._mpl_repr())
#                     params = res.params
#
#                     f = open(join(save_path, category + "_" + hrz + '_model_params.txt'), 'w')
#                     f.write(str(res.params))
#                     f.close()
#
#                     factors.loc[end_date + relativedelta(months=int(hrz)), 'f1'] = params['L1.f1.f1'] * factors.loc[
#                         end_date, 'f1'] + params['L1.f2.f1'] * factors.loc[end_date, 'f2']
#                     factors.loc[end_date + relativedelta(months=int(hrz)), 'f2'] = params['L1.f1.f2'] * factors.loc[
#                         end_date, 'f1'] + params['L1.f2.f2'] * factors.loc[end_date, 'f2']
#                     sample = pd.concat([ret, factors], axis=1)
#                     sample = sample.dropna()
#
#                     sxmodel = pm.arima.auto_arima(sample[[category]], exogenous=sample[['f1', 'f2']],
#                                                   start_p=1, start_q=1,
#                                                   test='adf',
#                                                   max_p=3, max_q=3, m=1,
#                                                   start_P=0,
#                                                   d=None, D=0, trace=True,
#                                                   error_action='ignore',
#                                                   suppress_warnings=True,
#                                                   stepwise=True)
#
#                     sxmodel.summary()
#
#                     n_periods = 2
#
#                     fitted = sxmodel.predict(n_periods=n_periods, exogenous=factors.iloc[-2:, :], return_conf_int=False)
#                     predicted_mean_return = ((1 + fitted[0]) * (1 + fitted[1])) ** 2 - 1
#                 except:
#                     print("DFM Fail for " + category + " of " + training_date)
#                     ret = ret.dropna()
#
#                     sxmodel = pm.arima.auto_arima(ret[[category]], exogenous=None,
#                                                   start_p=1, start_q=1,
#                                                   test='adf',
#                                                   max_p=3, max_q=3, m=1,
#                                                   start_P=0,
#                                                   d=None, D=0, trace=True,
#                                                   error_action='ignore',
#                                                   suppress_warnings=True,
#                                                   stepwise=True)
#                     sxmodel.summary()
#                     n_periods = 2
#                     fitted = sxmodel.predict(n_periods=n_periods, return_conf_int=False)
#                     predicted_mean_return = ((1 + fitted[0]) * (1 + fitted[1])) ** 2 - 1
#
#
#     return predicted_mean_return


def predict_category_mean_return(training_date):
    category_list = ['Equity_Large', 'Equity_Mid', 'Bond', 'Money', 'Balance', 'QD']
    save_path = join(data_path, 'predictions', 'category_mean_returns')
    if exists(save_path):
        pass
    else:
        makedirs(save_path)
    rst = pd.DataFrame()
    for category in category_list:
        rst.loc[datetime.datetime.strptime(training_date, "%Y-%m-%d"), category] = time_series_model_construction(training_date, category)

    rst.to_csv(join(save_path, training_date + '.csv'))


if __name__ == "__main__":
    config_file_path = join(config_path, "config.conf")
    config = configparser.ConfigParser()
    config.read(config_file_path)
    start_date = str(config['backtest_info']['start_date'])
    end_date = str(config['backtest_info']['end_date'])

    prediction_dates = pd.date_range(start_date, end_date, freq='M')
    for prediction_date in prediction_dates:
        predict_category_mean_return(prediction_date.strftime('%Y-%m-%d'))
