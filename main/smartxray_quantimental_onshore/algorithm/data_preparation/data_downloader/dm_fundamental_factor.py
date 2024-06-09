import pandas as pd
import numpy as np
import os
import math
from algorithm import addpath
from datetime import datetime, timedelta
from datamaster.constants import fund_factor_columns_dict
from datamaster import dm_client
dm_client.refresh_config()
dm_client.start()


def download_fund_data(start, end):
    # factors = ['turnover_ratio', 'expense_ratio', 'aum_factor', 'average_tenure', 'style_drift_factor', 'tail_risk_factors']
    factors = ['average_tenure', 'style_drift_factor', 'tail_risk_factors']
    savepath = os.path.join(addpath.historical_path, 'fundamental_factor', 'fundamental_factor_raw')

    if not os.path.exists(savepath):
        os.makedirs(savepath)

    secid_list = pd.read_csv(addpath.ref_path, index_col=0)
    secid_list = secid_list['ms_secid'].tolist()

    dates = pd.date_range(start, end, freq='M')
    dates = pd.Series(dates.format()).tolist()

    batch_size = 10
    start = dates[0]
    end = dates[-1]
    for f in factors:
        l = fund_factor_columns_dict[f]
        output_path = os.path.join(savepath, f)
        if not os.path.exists(output_path):
            os.makedirs(output_path)
        for n in range(math.ceil((len(secid_list) / batch_size))):
            sub_secid_list = secid_list[n*batch_size:min(n*batch_size+batch_size, len(secid_list))]
            try:
                res = dm_client.historical(symbols=sub_secid_list, start_date=start, end_date=end, fields=l)
            except Exception as e:
                print("***********")
                print(sub_secid_list)
                print(e)
                continue

            for secid in sub_secid_list:
                factor = pd.DataFrame()
                factor_tmp_ls = res['values'][secid]
                if len(factor_tmp_ls) != 0:
                    factor_tmp = pd.DataFrame(factor_tmp_ls)
                    pd.to_datetime(factor_tmp[0], format="%Y-%m-%d")
                    factor_tmp.set_index(pd.DatetimeIndex(factor_tmp[0]), inplace=True)
                    l_tmp = range(1, len(l) + 1)
                    factor_tmp = factor_tmp.loc[:, [*l_tmp]]

                    factor = pd.DataFrame(factor_tmp).resample('1M').last().ffill()
                    first_date = factor_tmp_ls[0][0]

                    last_date = factor_tmp_ls[-1][0]
                    if pd.to_datetime(last_date) > pd.to_datetime(end) - timedelta(weeks=60):
                        first_to_end_date = list(pd.date_range(first_date, end, freq='M').tolist())
                    else:
                        first_to_end_date = list(pd.date_range(first_date, last_date, freq='M').tolist())

                    # first_to_end_date = list(pd.date_range(first_date, end, freq='M').tolist())
                    factor = factor.reindex(first_to_end_date, fill_value=np.nan)
                    factor.columns = l
                    factor.fillna(method='ffill', inplace=True)
                    factor.to_csv(os.path.join(output_path, secid + '.csv'), mode='w')
                else:
                    # print("%s does not have %s from DM" %(secid, f))
                    continue
            print('%s: download %i @ %s' % (f, ((n+1)*10), datetime.now().date()))
        monthly_factor_file_prepare(f, start=start, end=end)

def monthly_factor_file_prepare(factor_name, start, end):
    summary_path = addpath.bundle_path
    rawpath = os.path.join(addpath.historical_path, 'fundamental_factor','fundamental_factor_raw', factor_name)
    savepath = os.path.join(addpath.historical_path, 'fundamental_factor', factor_name)
    summary_header = 'Name'

    if os.path.exists(savepath):
        pass
    else:
        os.makedirs(savepath)

    ticker_path = os.path.join(summary_path, "Summary.csv")
    ms_secids = pd.read_csv(ticker_path)
    ms_secids = ms_secids[summary_header].tolist()

    data_list = []
    for ms_secid in ms_secids:
        try:
            tmp_path = os.path.join(rawpath, ms_secid + ".csv")
            tmp = pd.read_csv(tmp_path, index_col=0)
            tmp['ms_secid'] = ms_secid
            data_list.append(tmp)
        except:
            print("No data for " + ms_secid)
    data_raw = pd.concat(data_list)
    if 'date' in data_raw.columns:
        data_raw = data_raw.set_index('date')

    dates = pd.date_range(start, end, freq='M').tolist()
    for date in dates:
        factor_monthly = data_raw[data_raw.index == date.strftime("%Y-%m-%d")]
        factor_monthly = factor_monthly.set_index('ms_secid')
        factor_monthly = factor_monthly.dropna(how='all', axis=0)
        output_path = os.path.join(savepath, date.strftime("%Y-%m-%d") + '.csv')
        factor_monthly.to_csv(output_path)


if __name__ == '__main__':
    download_fund_data(start='2013-09-30', end='2020-12-31')