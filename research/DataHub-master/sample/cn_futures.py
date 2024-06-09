import json, requests, platform
import pandas as pd
from multiprocessing import Pool
from itertools import product

def make_get_req(params):
    res = requests.get(f'http://hk-qb-data01.aqumonx.xyz:8000/snapshot/symbol={params[0]}&day={params[1]}')
    if res.status_code != 404:
        return [x.split(',') for x in res.content.decode().split('\r\n')[1:-1]]
    return []

def get_futures_snapshot(symbol, start_day, end_day):
    ex = symbol.split('.')[1]
    res = requests.get(f'http://hk-qb-data01.aqumonx.xyz:8000/exchange_trading_days/{ex}&start_day={start_day}&end_day={end_day}')
    if res.status_code != 404:
        trading_days = json.loads(res.content.decode())[ex]
    else:
        raise Exception("needs to add retry later")
    if platform.system() == 'Windows':
        ress = []
        for day in trading_days:
            res = requests.get(f'http://hk-qb-data01.aqumonx.xyz:8000/snapshot/symbol={symbol}&day={day}')
            if res.status_code != 404:
                ress.extend([x.split(',') for x in res.content.decode().split('\r\n')[1:-1]])
    else:
        p = Pool(8)
        ress = p.map(make_get_req, product([symbol], trading_days))
        ress = sum(ress, [])
    df = pd.DataFrame(ress, columns=eval(requests.get('http://hk-qb-data01.aqumonx.xyz:8000/snapshot_header/cn_futures').content.decode()))
    return df

if __name__ == "__main__":
    from datetime import datetime
    ts = datetime.now()
    df = get_futures_snapshot("sc_M1.INE", '20190202', '20190305')
    te = datetime.now()
    print(te - ts)
