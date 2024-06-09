import pandas as pd
import requests, json
import os, sys, time
from algorithm import addpath
from datamaster import dm_client
dm_client.refresh_config()
dm_client.start()


def createSummary():
    bundles_path = os.path.join(addpath.bundle_path, 'daily')
    save_path = addpath.bundle_path
    equities = os.listdir(bundles_path)
    equities = [f[:-4] for f in equities if f[-4:] == '.csv']
    df = pd.DataFrame(equities)
    df.columns = ['Name']
    df.set_index('Name', inplace=True)
    df.to_csv(os.path.join(save_path, 'Summary.csv'))


def get_historical_price(calendar):
    print('Downloading historical data!')
    offshore_fund_sub = pd.read_csv(addpath.ref_path, index_col=0)
    secid_list = offshore_fund_sub['ms_secid'].tolist()
    save_path = os.path.join(addpath.bundle_path, 'raw')
    if not os.path.exists(save_path):
        os.makedirs(save_path)

    empty_fund = []
    count = 0

    for secid in secid_list:
        if count % 50 ==0:
            time.sleep(1)
        count += 1

        print("downloading {}".format(secid))
        result = historical_price_download(secid)
        try:
            result = pd.DataFrame(result['values'][secid])
            result.columns = ['date', 'adj_nav']
            result = result[result['date'].isin(calendar)]
            result.set_index(['date'], inplace=True)
            result.to_csv(os.path.join(save_path, secid + '.csv'))
        except:
            empty_fund.append(secid)
    empty_fund = pd.DataFrame(empty_fund)
    empty_fund.to_csv(os.path.join(addpath.bundle_path, 'empty_fund.csv'))


def format_bundle_data():
    print('Format bundle data!')
    offshore_fund_sub = pd.read_csv(addpath.ref_path, index_col=0)
    empty_fund = pd.read_csv(os.path.join(addpath.bundle_path, 'empty_fund.csv'), index_col=0)
    all_fund_list = offshore_fund_sub['ms_secid'].tolist()
    empty_fund_list = list(empty_fund['0'])
    all_fund_list = set(all_fund_list) - set(empty_fund_list)
    summary_list = []

    for f in all_fund_list:
        print('generate bundle : {}'.format(f))

        daily_data = pd.read_csv(os.path.join(addpath.bundle_path, 'raw', '{}.csv'.format(f)),
                                 parse_dates=['date'], index_col='date', infer_datetime_format=True)
        # daily_data['date'] = daily_data.index.tolist()
        daily_data = daily_data[~daily_data.index.duplicated()]
        daily_data = daily_data.sort_index(ascending=True)
        daily_data = daily_data.iloc[1:, :]
        try:
            adj_close = daily_data['adj_nav']
        except:
            adj_close = daily_data['adjust_nav']

        cap = pd.Series([100000000] * len(daily_data), index=daily_data.index.tolist())
        bundle = pd.concat([adj_close, adj_close, adj_close, adj_close, cap / 10000], axis=1)
        bundle.columns = ['open', 'high', 'low', 'close', 'volume']
        bundle['dividend'] = [0] * len(bundle)
        bundle['split'] = [1] * len(bundle)
        bundle = bundle.dropna(axis=0)
        bundle.index.name = 'date'

        if len(bundle) > 10:
            if f in summary_list:
                bundle.to_csv(os.path.join(addpath.bundle_path, 'daily', '{}.csv'.format(f)))
            else:
                summary_list.append(f)
                bundle.to_csv(os.path.join(addpath.bundle_path, 'daily', '{}.csv'.format(f)))


def historical_price_download(secid):
    result = dm_client.historical(symbols=secid, start_date='2000-01-04', fields='fund_fqnav')

    return result


if __name__ == '__main__':
    r = requests.get(
        'https://algo-internal.aqumon.com/datamaster/exchange_calendar/?start_date='+'2000-01-01'+'&end_date='+'2022-12-31'+'&ex='+'CN')
    user_info = r.content.decode()
    user_dict = json.loads(user_info)

    calendar = pd.DataFrame(user_dict)
    calendar.sort_values(by='data', inplace=True)
    calendar = calendar['data'].tolist()
    # output.to_csv('Trading_Calendar.csv')

    get_historical_price(calendar = calendar)
    format_bundle_data()
    createSummary()
