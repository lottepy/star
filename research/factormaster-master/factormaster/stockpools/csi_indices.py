from qtoolkit.data import choice_client
import os
import pandas as pd
import numpy as np


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


def find_mid_point(start, end):
    # assume start, end are both trading days
    aList = SSE_CALENDAR.loc[start: end].index.tolist()
    return aList[int(np.floor(len(aList)/2))]


def download_one_day_component(sector_code, date):
    return choice_client.sector(
        sector_code, date.strftime("%Y-%m-%d")
    ).iloc[:, 0].tolist()


def _download_csi_indices_component(index_name, start_date, end_date):
    if isinstance(start_date, str):
        start_date = pd.to_datetime(start_date)
    if isinstance(end_date, str):
        end_date = pd.to_datetime(end_date)
    index_name = index_name.upper()
    index_code_dict = {'CSI300': '009006195', 'CSI500': '009006062'}
    index_code = index_code_dict[index_name]
    trading_days = SSE_CALENDAR.loc[start_date: end_date].index.tolist()
    df_list = []
    if os.path.exists(f'data/stockpools/{index_name}_COMPONENT.csv'):
        df = pd.read_csv(f'data/stockpools/{index_name}_COMPONENT.csv', index_col=0, parse_dates=True)
        df_list.append(df)
    for date in trading_days:
        stock_list = download_one_day_component(index_code, date)
        df = pd.DataFrame(1, columns=stock_list, index=[date])
        df_list.append(df)
    rslt = pd.concat(df_list, axis=0)
    rslt.sort_index(inplace=True)
    rslt = rslt.loc[~rslt.index.duplicated(keep='last')]
    rslt = rslt.fillna(0).astype(int)
    ix = rslt.diff().abs().sum(axis=1).values.astype(bool)
    ix[0] = True
    ix[-1] = True
    rslt = rslt.loc[ix]
    rslt.sort_index(axis=1, inplace=True)
    rslt.to_csv(f'data/stockpools/{index_name}_COMPONENT.csv')
    
    
def update_csi_indices_component(start_date, end_date):
    if isinstance(start_date, str):
        start_date = pd.to_datetime(start_date)
    if isinstance(end_date, str):
        end_date = pd.to_datetime(end_date)
    # csi300
    try:
        last_update_date = pd.read_csv('data/stockpools/CSI300_COMPONENT.csv', index_col=0, parse_dates=True).index[-1]
    except FileNotFoundError:
        last_update_date = start_date
    _download_csi_indices_component('CSI300', max(start_date, last_update_date), end_date)
    # csi500
    try:
        last_update_date = pd.read_csv('data/stockpools/CSI500_COMPONENT.csv', index_col=0, parse_dates=True).index[-1]
    except FileNotFoundError:
        last_update_date = start_date
    _download_csi_indices_component('CSI500', max(start_date, last_update_date), end_date)
    # csi800
    csi300 = pd.read_csv('data/stockpools/CSI300_COMPONENT.csv', index_col=0, parse_dates=True)
    csi500 = pd.read_csv('data/stockpools/CSI500_COMPONENT.csv', index_col=0, parse_dates=True)
    columns = sorted(list(set(csi300.columns.tolist() + csi500.columns.tolist())))
    index = sorted(list(set(csi300.index.tolist() + csi500.index.tolist())))
    csi800_300 = pd.DataFrame(np.nan, index=index, columns=csi300.columns)
    csi800_300.loc[csi300.index] = csi300
    csi800_300 = csi800_300.ffill()
    csi800_500 = pd.DataFrame(np.nan, index=index, columns=csi500.columns)
    csi800_500.loc[csi500.index] = csi500
    csi800_500 = csi800_500.ffill()
    csi800_300 = pd.concat([csi800_300, pd.DataFrame(columns=columns)], axis=0).fillna(0)
    csi800_500 = pd.concat([csi800_500, pd.DataFrame(columns=columns)], axis=0).fillna(0)
    csi800 = csi800_300 + csi800_500
    csi800.to_csv('data/stockpools/CSI800_COMPONENT.csv')
    
    
if __name__ == '__main__':
    update_csi_indices_component(start_date='2000-01-01', end_date='2020-12-15')