import numpy as np
import pandas as pd
import os, time
import math
import datetime
from algorithm import addpath
from datamaster import dm_client
dm_client.refresh_config()
dm_client.start()

def fund_aum(secid_list, date):
    output = pd.DataFrame()
    for n in range(math.ceil((len(secid_list) / 50))):
        time.sleep(0.5)
        sub_secid_list = secid_list[n * 50:n * 50 + 50]
        result_aum = dm_client.historical(symbols=sub_secid_list, start_date='2014-12-31', end_date=date,
                                          fields='fund_cltna')
        for secid in sub_secid_list:
            try:
                temp_df = pd.DataFrame(result_aum['values'][secid])
                temp_df.columns = result_aum['fields']
                temp_df.set_index(pd.to_datetime(temp_df['date']), inplace=True)
                temp_df = temp_df[['fund_cltna']]
            except:
                print('NO DATA FOR ' + secid)
                output.loc[secid, 'aum'] = np.nan
                continue

            temp_df = temp_df.resample('M').last().ffill()

            if temp_df.index.tolist()[-1].year >= datetime.datetime.now().year - 1:
                output.loc[secid, 'aum'] = temp_df.iloc[-1]['fund_cltna']
                last_date = datetime.datetime.strptime(date, '%Y-%m-%d')
                temp_df.loc[last_date, 'fund_cltna'] = temp_df.iloc[-1]['fund_cltna']
                temp_df = temp_df.resample('M').last().ffill()
            else:
                output.loc[secid, 'aum'] = np.nan
            temp_df.to_csv(os.path.join(addpath.historical_path, 'aum_info', 'aum_raw', secid + '.csv'))
    return output

def aum_info_to_date():
    aum_path = os.path.join(addpath.historical_path, 'aum_info', 'aum_raw')
    files = os.listdir(aum_path)

    date_all = pd.DataFrame()
    for f in files:
        temp_df = pd.read_csv(os.path.join(aum_path, f), index_col='date', parse_dates=['date'])
        temp_df.rename(columns={'fund_cltna': f[:-4]}, inplace=True)

        date_all = pd.concat([date_all, temp_df], axis=1)

    for date in date_all.index.tolist():
        temp_output = pd.DataFrame(date_all.loc[date, :])
        temp_output.columns = ['aum']
        date = datetime.datetime.strftime(date, '%Y-%m-%d')
        temp_output.to_csv(os.path.join(addpath.historical_path, 'aum_info', str(date)+'.csv'))


if __name__ == "__main__":
    fund_list = pd.read_csv(addpath.ref_path, index_col=0)
    secid_list = fund_list['ms_secid'].tolist()
    date = '2020-12-31'

    fund_aum_info = fund_aum(secid_list, date)
    aum_info_to_date()