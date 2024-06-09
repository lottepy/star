import json, requests, platform
import pandas as pd
from multiprocessing import Pool
from itertools import product
from functools import partial

def make_get_req(params, category):
    res = requests.get(f'http://hk-qb-data01.aqumonx.xyz:8000/{category}/symbol={params[0]}&day={params[1]}')
    if res.status_code != 404:
        return [x.split(',') for x in res.content.decode().replace('\r','').split('\n')[1:-1]]
    return []

exs = {"SZ":"SZSE", "SH":"SSE"}

def get_cn_equity_data(symbol, category, start_day, end_day):
    ex = exs[symbol.split('.')[1]]
    res = requests.get(f'http://hk-qb-data01.aqumonx.xyz:8000/exchange_trading_days/{ex}&start_day={start_day}&end_day={end_day}')
    if res.status_code != 404:
        trading_days = json.loads(res.content.decode())[ex]
    else:
        raise Exception("needs to add retry later")
    print(trading_days)
    if platform.system() == 'Windows':
        ress = []
        for day in trading_days:
            res = requests.get(f'http://hk-qb-data01.aqumonx.xyz:8000/{category}/symbol={symbol}&day={day}')
            if res.status_code != 404:
                ress.extend([x.split(',') for x in res.content.decode().replace('\r','').split('\n')[1:-1]])
    else:
        p = Pool(8)
        make_get_req_for_category = partial(make_get_req, category=category)
        ress = p.map(make_get_req_for_category, product([symbol], trading_days))
        ress = sum(ress, [])
    df = pd.DataFrame(ress, columns=eval(requests.get(f'http://hk-qb-data01.aqumonx.xyz:8000/{category}_header/cn_equity').content.decode()))
    return df

if __name__ == "__main__":
    from datetime import datetime
    ts = datetime.now()
    df = get_cn_equity_data("000001.SZ", "snapshot", '20190202', '20190305')
    te = datetime.now()
    print(te - ts)
    print(df)

    ts = datetime.now()
    df = get_cn_equity_data("000001.SZ", "trade", '20190202', '20190305')
    te = datetime.now()
    print(te - ts)
    print(df)
