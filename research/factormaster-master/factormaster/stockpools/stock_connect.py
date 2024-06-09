from qtoolkit.data import choice_client
import pandas as pd
import os
from datetime import timedelta
import time
from tqdm import tqdm


SSE_CALENDAR = pd.read_csv('data/TRADEDATE.csv', index_col=0, parse_dates=True)

def offset_trading_day(start_day, offset):
    assert offset != 0
    union = list(sorted(set(SSE_CALENDAR.index.tolist() + [start_day])))
    ix = union.index(start_day)
    if ix + offset < 0:
        raise ValueError('Exceed TRADEDATE start day')
    if ix + offset >= len(union):
        raise ValueError('Exceed TRADEDATE end day')
    return union[ix + offset]


def download_sector(sector_code, date):
    time.sleep(0.1)
    data = choice_client.sector(sector_code, date.strftime("%Y-%m-%d")).iloc[:, 0].tolist()
    return data


def download_all_stockconnect(start_date, end_date, freq='W'):
    if isinstance(start_date, str):
        start_date = pd.to_datetime(start_date)
    if isinstance(end_date, str):
        end_date = pd.to_datetime(end_date)
    try:
        last_update_date = pd.read_csv('data/stockpools/raw/STOCKCONNECTALL.csv', index_col=0, parse_dates=True).index[-1]
    except FileNotFoundError:
        last_update_date = start_date
    start_date = max(start_date, last_update_date)
    dates = pd.date_range(start_date, end_date, freq='w').tolist()
    dates = dates + [end_date]
    df_list = []
    try:
        df = pd.read_csv('data/stockpools/raw/STOCKCONNECTALL.csv', index_col=0, parse_dates=True)
        df_list.append(df)
    except FileNotFoundError:
        pass
    last_date = start_date
    last_names = download_sector('001047', start_date)
    df = pd.DataFrame(1, index=[last_date], columns=last_names)
    df_list.append(df)
    try:
        for date in tqdm(dates):
            names = download_sector('001047', date)
            df = pd.DataFrame(1, index=[date], columns=names)
            df_list.append(df)
            if freq == 'D' and len([i for i in last_names if i not in names]) + len([i for i in names if i not in last_names]) > 0:
                # print(len([i for i in last_names if i not in names]) + len([i for i in names if i not in last_names]) )
                detail_dates = [i for i in SSE_CALENDAR.index.tolist() if i >= last_date and i <= date]
                for date_ in detail_dates:
                    names_ = download_sector('001047', date_)
                    df = pd.DataFrame(1, index=[date_], columns=names_)
                    df_list.append(df)
            last_date = date
            last_names = names
    except:
        pass
    df = pd.concat(df_list, axis=0)
    df.sort_index(inplace=True)
    df.sort_index(1, inplace=True)
    df = df.loc[~df.index.duplicated(keep='first')]
    df.fillna(0, inplace=True)
    ix = df.diff().abs().sum(axis=1).values.astype(bool)
    ix[0] = True
    ix[-1] = True
    df = df.loc[ix]
    df.to_csv('data/stockpools/raw/STOCKCONNECTALL.csv')


def download_stockconnect(start_date, end_date):
    if isinstance(start_date, str):
        start_date = pd.to_datetime(start_date)
    if isinstance(end_date, str):
        end_date = pd.to_datetime(end_date)
    download_all_stockconnect(start_date, end_date, 'W')
    ashare = pd.read_csv('data/stockpools/ASHARE_COMPONENT.csv', index_col=0, parse_dates=True)
    stockconnectall = pd.read_csv('data/stockpools/raw/STOCKCONNECTALL.csv', index_col=0, parse_dates=True)
    
    index = SSE_CALENDAR.loc[start_date: end_date].index
    columns = sorted(list(set(ashare.columns.tolist()).intersection(stockconnectall.columns.tolist())))
    ashare = ashare.loc[index, columns]
    stockconnectall = stockconnectall.loc[:, columns]
    stockconnectall = pd.concat([stockconnectall, pd.DataFrame(index=index)], axis=1).ffill().fillna(0)
    stockconnectall = stockconnectall.loc[index]
    stockconnect = ashare * stockconnectall
    stockconnect.to_csv('data/stockpools/STOCKCONNECT_COMPONENT.csv')
    return

if __name__ == '__main__':
    download_stockconnect("2017-01-01", "2020-12-14")