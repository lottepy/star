'''

Predict regime to control beta of the portfolio

'''

# from hmmlearn.hmm import GaussianHMM
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import statsmodels.api as sm
import configparser
from os import makedirs
from os.path import exists, join, normpath
from algorithm.addpath import config_path, data_path
import datetime
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
import xgboost as xgb


# class RecessionDetector():
#     def __init__(self, training_date):
#
#         def std_scaler(sample_in):
#             sample_out = pd.DataFrame(StandardScaler().fit_transform(sample_in.values), columns=sample_in.columns,
#                                       index=sample_in.index)
#             return sample_out.iloc[-1, :]
#
#         ts_factor_path = join(data_path, 'factors', 'time_series_factor', 'processed', 'cn_market.csv')
#         ts_factors = pd.read_csv(ts_factor_path, parse_dates=[0], index_col=0)
#
#         config_file_path = join(config_path, "config.conf")
#         config = configparser.ConfigParser()
#         config.read(config_file_path)
#         hrz = str(config['model_info']['time_series_prediction_hrz'])
#         self.training_date = datetime.datetime.strptime(training_date, "%Y-%m-%d")
#         self.start_date = datetime.datetime.strptime(config['model_info']['time_series_prediction_sample_start_date'], '%Y-%m-%d')
#
#         self.data_px = ts_factors.loc[: self.training_date + datetime.timedelta(days=1), ['SHCOMP Index', 'SZCOMP Index']].resample('W').last().ffill()
#         self.data_vl = ts_factors.loc[: self.training_date + datetime.timedelta(days=1), ['SHCOMP Index Volume']].dropna().resample('W').sum().fillna(0)
#         self.data = pd.concat([self.data_px, self.data_vl], axis=1)
#         self.mr = self.data.loc[:, ['SHCOMP Index', 'SZCOMP Index']].pct_change().dropna()
#         self.log_data = np.log(self.data.loc[:, ['SHCOMP Index', 'SZCOMP Index']])
#         self.lr = self.log_data.loc[:, ['SHCOMP Index', 'SZCOMP Index']].diff().dropna() * 100.
#         self.log_vl = np.log(self.data.loc[:, ['SHCOMP Index Volume']] + 1)
#         self.obs_data = pd.concat([self.lr, self.log_vl], axis=1).dropna()
#         self.raw = self.data.loc[:, ['SHCOMP Index', 'SZCOMP Index']]
#         self.result = {}
#
#     # @pysnooper.snoop()
#     def detect(self, method='switching', n_state=3):
#         config_file_path = join(config_path, "config.conf")
#         config = configparser.ConfigParser()
#         config.read(config_file_path)
#         if method == 'hmm':
#             obs_data = self.obs_data.copy()
#
#             # 建立HMM模型，并估计隐状态序列
#             hmm_model = GaussianHMM(
#                 n_components=n_state,
#                 covariance_type='full',
#                 n_iter=1000
#             ).fit(obs_data.values)
#             state_series = hmm_model.predict(obs_data.values)
#             hmm_result = obs_data.copy()
#             hmm_result['state'] = state_series
#
#             self._plot_training(
#                 hmm_model,
#                 state_series,
#                 hmm_result,
#                 self.data.loc[self.obs_data.index]
#             )
#         elif method == 'switching':
#             # 建立Markov Switching Model
#             switch_model = sm.tsa.MarkovRegression(
#                 self.lr,
#                 k_regimes=n_state,
#                 switching_variance=True,
#                 # order=4
#             ).fit()
#             # print(switch_model.summary())
#
#             self.result['params'] = switch_model.params
#             self.result['filter_prob'] = switch_model.filtered_marginal_probabilities
#             self.result['smooth_prob'] = switch_model.smoothed_marginal_probabilities
#             self.result['predict_prob'] = switch_model.predicted_marginal_probabilities
#             prob_df = pd.concat([
#                 self.result['filter_prob'][1],
#                 self.result['smooth_prob'][1],
#                 self.result['predict_prob'][1]
#             ], axis=1)
#             save_path = join(data_path, 'predicitons', 'prob_df.csv')
#             prob_df.to_csv(save_path)
#
#             return switch_model.smoothed_marginal_probabilities
#
#     def display(self):
#         pass
#
#     def _plot_training(self, model, state, result, data):
#         plt.figure(figsize=(10, 6))
#         # 使用隐藏态标记训练期内收盘价
#         ax1 = plt.subplot(111)
#         plt.sca(ax1)
#
#         df = result.copy()
#         close_price = data.loc[:, ['SHCOMP Index']].values # 读取窗口内指标和收盘价
#         plt.title('Stock Price Labeled with Latent States')
#         for i in range(model.n_components):
#             idx = (state == i)
#             plt.plot(
#                 data.index.values[idx],
#                 close_price[idx],
#                 '.',
#                 label='%dth latent state' % i,
#                 lw=1
#             )
#             plt.legend()
#             plt.grid(1)
#         plt.show()


class RecessionDetector():
    def __init__(self, training_date):
        ts_factor_path = join(data_path, 'factors', 'time_series_factor', 'processed', 'cn_market.csv')
        self.datain = pd.read_csv(ts_factor_path, parse_dates=[0], index_col=0)
        self.training_date = datetime.datetime.strptime(training_date, "%Y-%m-%d")
        self.datain = self.datain.loc[: self.training_date + datetime.timedelta(days=1), :]

    def detect(self, recession_jugg_horizon=6, recession_jugg_threhold=-0.15):
        self.data_regime = self.datain[['SHCOMP_Index']].resample('M').last().ffill()
        self.data_regime = self.data_regime.shift(-recession_jugg_horizon) / self.data_regime - 1
        self.data_regime['regime'] = np.where(self.data_regime['SHCOMP_Index'] < recession_jugg_threhold, 1, 0)
        self.data_regime = self.data_regime[['regime']]
        self._plot_training(
            self.data_regime['regime'],
            self.datain.loc[self.data_regime.index, ['SHCOMP_Index']]
        )

    def training_data_prep(self):
        def std_scaler(sample_in):
            # sample_in = sample_in.drop(index=sample_in.index[-6:-1])
            sample_out = pd.DataFrame(StandardScaler().fit_transform(sample_in.values), columns=sample_in.columns,
                                      index=sample_in.index)
            return sample_out.iloc[-1, :]

        def ma_reverse(sample_in, window, idx, col):
            sample_in = sample_in.dropna()
            sample_in = sample_in.loc[: idx + datetime.timedelta(days=1), :]
            sample_in = sample_in.iloc[max(0, len(sample_in) + 1 - window):, :]
            ma = sample_in[col].mean()
            return sample_in.loc[sample_in.index[-1], col] / ma -1

        def cal_std(sample_in, window, idx, col):
            sample_in = sample_in.dropna()
            sample_in = sample_in.loc[: idx + datetime.timedelta(days=1), :]
            sample_in = sample_in.iloc[max(0, len(sample_in) + 1 - window):, :]
            sample_out = sample_in[col].std()
            return sample_out

        def cal_skew(sample_in, window, idx, col):
            sample_in = sample_in.dropna()
            sample_in = sample_in.loc[: idx + datetime.timedelta(days=1), :]
            sample_in = sample_in.iloc[max(0, len(sample_in) + 1 - window):, :]
            sample_out = sample_in[col].skew()
            return sample_out

        self.data_px = self.datain.loc[: self.training_date + datetime.timedelta(days=1), ['SHCOMP_Index']].resample('M').last().ffill()
        for i in range(1, 25):
            self.data_px['SHCOMP_Index_cr_' + str(i) + 'M'] = self.data_px['SHCOMP_Index'] / self.data_px['SHCOMP_Index'].shift(i) - 1
        # self.data_px = self.data_px.drop(columns='SHCOMP_Index')
        self.data_cr_zscore = self.data_px.copy()
        for idx in range(12, len(self.data_cr_zscore.index)):
            self.data_cr_zscore.iloc[idx, :] = std_scaler(self.data_px.iloc[max(0, idx - 84): idx + 1, :])
        self.data_cr_zscore.columns = [col + "_zscore" for col in self.data_px.columns]

        self.data_vl = self.datain.loc[: self.training_date + datetime.timedelta(days=1), ['SHCOMP_Index_Volume']].dropna().resample('M').sum().fillna(0)
        for i in range(1, 25):
            self.data_vl['SHCOMP_Index_Volume_' + str(i) + 'M'] = self.data_vl.rolling(window=i).SHCOMP_Index_Volume.sum()
        self.data_vl = self.data_vl.drop(columns='SHCOMP_Index_Volume')
        self.log_vl = np.log(self.data_vl + 1)
        self.vl_growth = self.log_vl.diff().dropna()
        self.vl_growth.columns = [col + "_growth" for col in self.vl_growth.columns]

        self.data_daily_ret = self.datain.loc[: self.training_date + datetime.timedelta(days=1), ['SHCOMP_Index']].ffill().pct_change()
        self.data_std = pd.DataFrame(index=self.data_px.index)
        for i in range(1, 25):
            for idx in self.data_cr_zscore.index[12:]:
                self.data_std.loc[idx, 'SHCOMP_Index_std_' + str(i) + 'M'] = cal_std(self.data_daily_ret, i * 21, idx, 'SHCOMP_Index')
                self.data_std.loc[idx, 'SHCOMP_Index_skew_' + str(i) + 'M'] = cal_skew(self.data_daily_ret, i * 21, idx, 'SHCOMP_Index')

        self.data_x = self.datain.loc[: self.training_date + datetime.timedelta(days=1), ['SHCOMP_Index']].resample('M').last().ffill()
        self.data_ma = pd.DataFrame(index=self.data_px.index)
        for i in range(6, 60, 3):
            for idx in self.data_ma.index[12:]:
                self.data_ma.loc[idx, 'relative_to_ma_' + str(i) + 'M'] = ma_reverse(self.data_x, i, idx, 'SHCOMP_Index')

        self.raw_factors = pd.concat([self.data_ma, self.data_px, self.data_cr_zscore, self.log_vl, self.vl_growth, self.data_std], axis=1).dropna()
        # cov = self.obs_data.corr()
        # self.cov = cov[['regime']].rename(columns={'regime': 'cov'})
        # self.cov['abs_cov'] = self.cov['cov'].abs()
        # self.cov = self.cov.iloc[1:, :]
        # potential_predictors = self.cov[self.cov['abs_cov'] > 0.3].index.tolist()

        # pca = PCA().fit(self.obs_data.iloc[:, 1:])
        # evr = np.cumsum(pca.explained_variance_ratio_)
        # i = 0
        # while evr[i] < 0.8:
        #     i += 1

        self.n_pca = 3
        pca = PCA(n_components=self.n_pca)
        self.factors = pd.DataFrame(pca.fit_transform(self.raw_factors), columns = ['f' + str(n + 1) for n in range(self.n_pca)], index=self.raw_factors.index)

    def xgboost_training(self):

        sample = pd.concat([self.data_regime, self.factors], axis=1).dropna()
        train = sample.iloc[:-6, :]
        test = sample.iloc[-6:, :]

        dtrain = xgb.DMatrix(train.iloc[:, 1:], label=train.iloc[:, 0])
        dtest = xgb.DMatrix(test.iloc[:, 1:], label=test.iloc[:, 0])

        num_boost_round = 1000

        params = {
            # Parameters that we are going to tune.
            'max_depth': 6,
            'eta': 0.3,
            'min_child_weight': 1,
            'subsample': 1,
            'colsample_bytree': 1,
            # Other parameters
            'objective': 'binary:logistic',
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
        print("First round best params: max_depth is {}, min_child_weight is {}, MAE: {}".format(best_params[0],
                                                                                                 best_params[1],
                                                                                                 min_mae))

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
        print("Second round best params: max_depth is {}, min_child_weight is {}, MAE: {}".format(best_params[0],
                                                                                                  best_params[1],
                                                                                                  min_mae))

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

        predictors = xgb.DMatrix(self.factors.loc[[self.training_date], :])
        predicted_regime = best_model.predict(predictors)

        return predicted_regime



    def _plot_training(self, state_series, price_data):
        plt.figure(figsize=(10, 6))
        # 使用隐藏态标记训练期内收盘价
        ax1 = plt.subplot(111)
        plt.sca(ax1)

        close_price = price_data.loc[:, ['SHCOMP_Index']].values # 读取窗口内指标和收盘价
        plt.title('Stock Price Labeled with Latent States')
        for i in range(2):
            idx = (state_series == i)
            plt.plot(
                price_data.index.values[idx],
                close_price[idx],
                '.',
                label='%dth latent state' % i,
                lw=1
            )
            plt.legend()
            plt.grid(1)
        plt.show()



if __name__ == "__main__":
    config_file_path = join(config_path, "config.conf")
    config = configparser.ConfigParser()
    config.read(config_file_path)
    start_date = str(config['backtest_info']['start_date'])
    end_date = str(config['backtest_info']['end_date'])

    score_dates = pd.date_range(start_date, end_date, freq='M')
    predicted_regime = pd.DataFrame(columns=['predicted_regime'])
    for score_date in score_dates:
        X = RecessionDetector(score_date.strftime('%Y-%m-%d'))
        X.detect()
        X.training_data_prep()
        predicted_regime.loc[score_date, 'predicted_regime'] = X.xgboost_training()
    predicted_regime.to_csv(join(data_path, 'predictions', 'predicted_regimes.csv'))


