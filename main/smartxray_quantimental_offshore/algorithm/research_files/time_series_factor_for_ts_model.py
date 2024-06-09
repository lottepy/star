'''

This file is for merge time_series_factors into a monthly sample

'''

from algorithm.addpath import data_path
import pandas as pd
from os import makedirs
from os.path import exists, join
import datetime
from sklearn.preprocessing import StandardScaler


def ts_data_set_preparation():
    factor_path = join(data_path, 'factors')
    input_path = join(factor_path, 'time_series_factor', 'raw')
    save_path = join(factor_path, 'time_series_factor', 'processed')
    if not exists(save_path):
        makedirs(save_path)

    ts_raw = pd.read_csv(join(input_path, "monthly_ts_factor.csv"), parse_dates=[0], index_col=0)
    factor_mean = pd.read_csv(join(input_path, "factor_mean.csv"), parse_dates=[0], index_col=0)
    factor_std = pd.read_csv(join(input_path, "factor_std.csv"), parse_dates=[0], index_col=0)

    ts_factors = pd.concat([ts_raw, factor_mean, factor_std], axis=1)
    ts_factors.to_csv(join(save_path, "ts_factors_for_ts_model.csv"))


def category_mean_reverse():
    save_path = join(data_path, 'factors', 'time_series_factor', 'processed')

    def std_scaler(sample_in):
        sample_out = pd.DataFrame(StandardScaler().fit_transform(sample_in.values), columns=sample_in.columns,
                                  index=sample_in.index)
        return sample_out.iloc[-1, :]

    nav_path = join(data_path, 'bundles', 'daily')
    ticker_path = join(data_path, 'bundles', 'Summary.csv')

    hrz = 1

    ms_secids = pd.read_csv(ticker_path)
    ms_secids = ms_secids['Name'].tolist()
    nav_list = []
    for ms_secid in ms_secids:
        try:
            nav_tmp_path = join(nav_path, str(ms_secid) + ".csv")
            nav_tmp = pd.read_csv(nav_tmp_path, parse_dates=[0], index_col=0)
            nav_tmp = nav_tmp[['close']].rename(columns={'close': ms_secid})
            nav_tmp = nav_tmp.resample('1M').last().ffill()
            nav_list.append(nav_tmp)
        except:
            print("Fail for " + str(ms_secid))
    nav = pd.concat(nav_list, axis=1)
    ret_tmp = nav / nav.shift(hrz) - 1

    start_date = datetime.datetime.strptime('2005-01-31', '%Y-%m-%d')
    end_date = datetime.datetime.strptime('2020-02-29', '%Y-%m-%d')
    dates = pd.date_range(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'), freq='M')

    category_path = join(data_path, 'categorization', 'fund_category')

    category_return_list = []

    for date in dates:
        category_df = pd.read_csv(join(category_path, date.strftime("%Y-%m-%d") + '.csv'), index_col=0)
        ret = ret_tmp.loc[date.strftime('%Y-%m-%d'), :]
        category_df['returns'] = ret
        category_return_tmp = pd.DataFrame(category_df.groupby('category').returns.mean()).transpose()
        category_return_tmp.index = [date]
        category_return_list.append(category_return_tmp)
    category_return = pd.concat(category_return_list)
    category_return.loc[datetime.datetime.strptime('2004-12-31', '%Y-%m-%d'), :] = [0, 0, 0, 0, 0, 0]
    category_return = category_return.sort_index()

    categories = ['Balance', 'Equity_Large', 'Equity_Mid']
    tmp = category_return.loc[:, categories]
    tmp = tmp + 1
    pv = tmp.cumprod()
    c12 = pv / pv.shift(12) - 1
    tmp2 = pv / pv.shift(12) - 1
    for idx in range(12, len(tmp2.index)):
        c12.iloc[idx, :] = std_scaler(tmp2.iloc[max(0, idx - 60): idx + 1, :])

    c12.to_csv(join(save_path, 'c12.csv'))


if __name__ == "__main__":
    ts_data_set_preparation()
    category_mean_reverse()