import itertools
import json
import os
import time
import uuid
from collections import defaultdict
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import requests
from datamaster import dm_client

dm_client.refresh_config()
dm_client.start()

NDF = {'USDKRW': 'KWN+1M',
       'USDTWD': 'NTN+1M',
       'USDINR': 'IRN+1M',
       'USDIDR': 'IHN+1M'}
NDF_ = {'KWN+1M': 'USDKRW',
        'NTN+1M': 'USDTWD',
        'IRN+1M': 'USDINR',
        'IHN+1M': 'USDIDR'}


def get_fx_snapshot(symbols):
    req_url = "https://algo-internal.aqumon.com/datamaster/api/v1/real_time/snapshot/"
    request_id = uuid.uuid1().hex
    params = {'symbols': ','.join(symbols)}
    res = requests.get(url=req_url, headers={
        'x-request-id': request_id}, params=params)
    return json.loads(res.text)['data']


def check_snapshots(snapshots):
    error_symbols = []
    snapshots_dict = {}
    for snapshot in snapshots:
        sym = NDF_.get(snapshot.get('symbol')[:6], snapshot.get('symbol')[:6])
        if 'error' in snapshot.keys():
            error_symbols.append(sym)
        else:
            last = snapshot.get('last', np.nan)
            if last > 0:
                snapshots_dict[sym] = last
            else:
                error_symbols.append(sym)
    return error_symbols, snapshots_dict


def get_Txxx_data(symbols):
    dm_symbol_list = [f'{NDF.get(symbol,symbol)} T{h:02d}{m} Curncy' for symbol,
                      h, m in itertools.product(symbols, range(24), [0, 3])]
    today = datetime.today().strftime('%Y-%m-%d')
    start_day = (datetime.today()-timedelta(days=5)).strftime('%Y-%m-%d')
    res = dm_client.get_historical_data(
        symbols=dm_symbol_list,
        start_date=start_day,
        end_date=today,
        fields='close',
        to_dataframe=True,
        fill_method='ffill'
    )
    rslt = defaultdict(dict)
    for key, value in res.items():
        rslt[NDF_.get(key[:6], key[:6])][key] = value
    return rslt


def calc_bt150(dm_data, snapshots_dict):
    today = datetime.today().strftime('%Y-%m-%d')
    rslt_list = {}
    for sym in dm_data.keys():
        prices = []
        for key, value in dm_data[sym].items():
            if key[8:11] == '150':
                if value['date'].iloc[-1] == today:
                    o = value.iloc[-2, -1]
                    c = value.iloc[-1, -1]
                else:
                    o = value.iloc[-1, -1]
                    c = snapshots_dict[sym]
                prices.append(o)
                prices.append(c)
            else:
                prices.append(value.iloc[-1, -1])
        h = np.max(prices)
        l = np.min(prices)
        tmp = {'open': o, 'high': h, 'low': l, 'close': c}
        rslt_list[sym] = pd.DataFrame(
            tmp, index=[int(pd.to_datetime(today).timestamp())])
    return rslt_list


def save_to_local(data):
    LOCAL_DATA_DIR = os.environ.get("QC_DM_LOCAL_DATA_DIR", "")
    new_df = None
    for key, value in data.items():
        if os.path.exists(os.path.join(LOCAL_DATA_DIR, NDF.get(key, key)+' BT150 Curncy.csv')):
            tmp = pd.read_csv(os.path.join(LOCAL_DATA_DIR, NDF.get(key, key)+' BT150 Curncy.csv'), index_col=0)
            tmp.index = pd.to_datetime(tmp.index, format='%Y-%m-%d', utc=True)
            tmp.index = list(map(lambda x: int(x.timestamp()),tmp.index))
            index = [idx for idx in tmp.index if idx not in value.index]
            tmp = tmp.loc[index]
            new_df = pd.concat([tmp, value], sort=True, axis=0)
        else:
            new_df = value
        new_df.index.name = 'date'
        new_df.index = pd.to_datetime(list(map(lambda x: datetime.utcfromtimestamp(int(x)),new_df.index)), format='%Y-%m-%d', utc=True)
        new_df.to_csv(os.path.join(LOCAL_DATA_DIR,NDF.get(key, key)+' BT150 Curncy.csv'))


def main(symbols=None):
    if symbols is None:
        symbols = [
            'EURUSD', 'USDJPY', 'NZDJPY', 'NOKSEK', 'GBPJPY', 'CADJPY', 'CADCHF', 'AUDNZD', 'AUDJPY', 'AUDCAD', 'USDSEK', 'USDNOK', 'EURCHF', 'USDCHF', 'USDCAD', 'NZDUSD', 'GBPUSD', 'AUDUSD', 'USDSGD', 'USDTHB', 'GBPCAD', 'EURCAD', 'EURJPY', 'EURGBP', 'GBPCHF', 'USDINR', 'USDIDR', 'USDTWD', 'USDKRW'
        ]

    snapshots_dict = {}
    snapshots = get_fx_snapshot(
        [NDF.get(sym, sym)+' CMPL Curncy' for sym in symbols])
    for i in range(5):
        error_symbols, filtered_snapshots_dict = check_snapshots(snapshots)
        snapshots_dict.update(filtered_snapshots_dict)
        if len(error_symbols) == 0:
            break
        else:
            snapshots = get_fx_snapshot(error_symbols)
    if len(error_symbols) != 0:
        raise ValueError('Data can not be downloaded via Snapshot')

    dm_data = get_Txxx_data(symbols)
    bt150 = calc_bt150(dm_data, snapshots_dict)
    save_to_local(bt150)
    return


if __name__ == '__main__':
    path = "BT150_data"
    os.environ["QC_DM_LOCAL_DATA_DIR"] = path
    main()
