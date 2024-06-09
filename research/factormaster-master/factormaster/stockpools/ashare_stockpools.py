from qtoolkit.data import choice_client
import pandas as pd
import os
from datetime import timedelta
import time
from tqdm import tqdm


SSE_CALENDAR = pd.read_csv('data/TRADEDATE.csv', index_col=0, parse_dates=True)


def _concat_by_col(df_list):
    df = pd.concat(df_list, axis=1, sort=True)
    return df


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


def download_asharenst(start_date, end_date, freq='W'):
    if isinstance(start_date, str):
        start_date = pd.to_datetime(start_date)
    if isinstance(end_date, str):
        end_date = pd.to_datetime(end_date)
    try:
        last_update_date = pd.read_csv('data/stockpools/raw/ASHARENST.csv', index_col=0, parse_dates=True).index[-1]
    except FileNotFoundError:
        last_update_date = start_date
    start_date = max(start_date, last_update_date)
    dates = pd.date_range(start_date, end_date, freq='w').tolist()
    dates = dates + [end_date]
    df_list = []
    try:
        df = pd.read_csv('data/stockpools/raw/ASHARENST.csv', index_col=0, parse_dates=True)
        df_list.append(df)
    except FileNotFoundError:
        pass
    last_date = start_date
    last_names = download_sector('001048', start_date)
    df = pd.DataFrame(1, index=[last_date], columns=last_names)
    df_list.append(df)
    try:
        for date in tqdm(dates):
            names = download_sector('001048', date)
            df = pd.DataFrame(1, index=[date], columns=names)
            df_list.append(df)
            if freq == 'D' and len([i for i in last_names if i not in names]) + len([i for i in names if i not in last_names]) > 0:
                # print(len([i for i in last_names if i not in names]) + len([i for i in names if i not in last_names]) )
                detail_dates = [i for i in SSE_CALENDAR.index.tolist() if i >= last_date and i <= date]
                for date_ in detail_dates:
                    names_ = download_sector('001048', date_)
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
    df.to_csv('data/stockpools/raw/ASHARENST.csv')
    

def download_ashare_alert(start_date, end_date, freq='W'):
    if isinstance(start_date, str):
        start_date = pd.to_datetime(start_date)
    if isinstance(end_date, str):
        end_date = pd.to_datetime(end_date)
    try:
        last_update_date = pd.read_csv('data/stockpools/raw/ASHAREALERT.csv', index_col=0, parse_dates=True).index[-1]
    except FileNotFoundError:
        last_update_date = start_date
    start_date = max(start_date, last_update_date)
    dates = pd.date_range(start_date, end_date, freq='w').tolist()
    dates = dates + [end_date]
    df_list = []
    try:
        df = pd.read_csv('data/stockpools/raw/ASHAREALERT.csv', index_col=0, parse_dates=True)
        df_list.append(df)
    except FileNotFoundError:
        pass
    last_date = start_date
    last_names = download_sector('001023', start_date)
    df = pd.DataFrame(1, index=[last_date], columns=last_names)
    df_list.append(df)
    try:
        for date in tqdm(dates):
            names = download_sector('001023', date)
            df = pd.DataFrame(1, index=[date], columns=names)
            df_list.append(df)
            if freq == 'D' and len([i for i in last_names if i not in names]) + len([i for i in names if i not in last_names]) > 0:
                # print(len([i for i in last_names if i not in names]) + len([i for i in names if i not in last_names]) )
                detail_dates = [i for i in SSE_CALENDAR.index.tolist() if i >= last_date and i <= date]
                for date_ in detail_dates:
                    names_ = download_sector('001023', date_)
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
    df.to_csv('data/stockpools/raw/ASHAREALERT.csv')


def download_ashare_listdate(names_list):
    try:
        df = pd.read_csv('data/stockpools/raw/ASHARELISTDATE.csv', index_col=0)
    except FileNotFoundError:
        df = pd.DataFrame(columns=['listdate'])
    for name in names_list:
        if name not in df.index:
            data = choice_client.css(name, "LISTDATE").values[0, 0]
            df.loc[name, 'listdate'] = data
    df.to_csv('data/stockpools/raw/ASHARELISTDATE.csv')
        
    
def download_ashare_bps(start_date, end_date):
    if isinstance(start_date, str):
        start_date = pd.to_datetime(start_date)
    if isinstance(end_date, str):
        end_date = pd.to_datetime(end_date)
    try:
        df = pd.read_csv('data/stockpools/raw/ASHAREBPS.csv', index_col=0, parse_dates=True)
        last_update_date = df.index[-1]
    except FileNotFoundError:
        df = pd.DataFrame()
        last_update_date = start_date
    last_update_date = max(start_date, last_update_date)
    old_stock_list = df.columns.tolist()
    stock_list = pd.read_csv('data/stockpools/raw/ASHARENST.csv', index_col=0, parse_dates=True).columns.tolist()
    # update old names
    if len(old_stock_list):
        dates_list = [last_update_date] + pd.date_range(last_update_date, end_date, freq='M').tolist() + [end_date]
        df_list = [df]
        for date in tqdm(dates_list):
            df_list_2 = []
            for i in range(len(old_stock_list)//50+1):
                symbols = ','.join(old_stock_list[i*50: (i+1)*50])
                data = choice_client.css(symbols, "BPSNEW", f"EndDate={date.strftime('%Y-%m-%d')}")
                df = pd.DataFrame(data.values, index=[date], columns=data.columns.get_level_values(0))
                df_list_2.append(df)
            df = pd.concat(df_list_2, axis=1)
            df_list.append(df)
        df = pd.concat(df_list, axis=0)
        df.sort_index(inplace=True)
        old_df = df.loc[~df.index.duplicated(keep='first')]
    else:
        old_df = pd.DataFrame()
        
    # download new names
    new_stock_list = [i for i in stock_list if i not in old_stock_list]
    if len(new_stock_list):
        dates_list = [start_date] + pd.date_range(start_date, end_date, freq='M').tolist() + [end_date]
        df_list = []
        for date in tqdm(dates_list):
            df_list_2 = []
            for i in range(len(new_stock_list)//50+1):
                symbols = ','.join(new_stock_list[i*50: (i+1)*50])
                data = choice_client.css(symbols, "BPSNEW", f"EndDate={date.strftime('%Y-%m-%d')}")
                df = pd.DataFrame(data.values, index=[date], columns=data.columns.get_level_values(0))
                df_list_2.append(df)
            df = pd.concat(df_list_2, axis=1)
            df_list.append(df)
        df = pd.concat(df_list, axis=0)
        df.sort_index(inplace=True)
        new_df = df.loc[~df.index.duplicated(keep='first')]
    else:
        new_df = pd.DataFrame()
    index = SSE_CALENDAR.loc[start_date: end_date].index
    all_df = pd.concat([old_df, new_df, pd.DataFrame(index=index)], axis=1).ffill().loc[index]
    all_df.to_csv('data/stockpools/raw/ASHAREBPS.csv')
    
    
def download_ashare(start_date, end_date, freq='W'):
    if isinstance(start_date, str):
        start_date = pd.to_datetime(start_date)
    if isinstance(end_date, str):
        end_date = pd.to_datetime(end_date)
    download_asharenst(start_date, end_date, freq)
    download_ashare_alert(start_date, end_date, freq)
    stock_list = pd.read_csv('data/stockpools/raw/ASHARENST.csv', index_col=0, parse_dates=True).columns.tolist()
    download_ashare_listdate(stock_list)
    # download_ashare_bps(start_date, end_date)
    
    asharenst = pd.read_csv('data/stockpools/raw/ASHARENST.csv', index_col=0, parse_dates=True)
    asharealert = pd.read_csv('data/stockpools/raw/ASHAREALERT.csv', index_col=0, parse_dates=True)
    asharelistdate = pd.read_csv('data/stockpools/raw/ASHARELISTDATE.csv', index_col=0)
    asharelistdate['listdate'] = pd.to_datetime(asharelistdate['listdate'])
    # asharebps = pd.read_csv('data/stockpools/raw/ASHAREBPS.csv', index_col=0, parse_dates=True)
    # asharebps = (asharebps > 0).astype(bool)
    
    dates = SSE_CALENDAR.loc[start_date: end_date].index.tolist()
    asharenst = pd.concat([asharenst, pd.DataFrame(index=dates)], axis=1).ffill().loc[dates]
    asharealert = pd.concat([asharealert, pd.DataFrame(index=dates)], axis=1).ffill().loc[dates]
    
    columns = sorted(list(set(asharenst.columns.tolist()+asharealert.columns.tolist())))
    asharenst = pd.concat([asharenst, pd.DataFrame(columns=columns)], axis=0).fillna(0)
    asharealert = pd.concat([asharealert, pd.DataFrame(columns=columns)], axis=0).fillna(0)
    ashare = ((asharenst - asharealert) > 0).astype(int)
    ashare = ashare.loc[:, stock_list]
    for name in tqdm(ashare.columns):
        listdate = asharelistdate.loc[name, 'listdate']
        if listdate <= SSE_CALENDAR.index[0]:
            continue
        can_trade_day = offset_trading_day(listdate, 63)
        ashare.loc[:can_trade_day, name] = 0
    ashare = ashare.loc[:, ashare.sum(axis=0).astype(bool)]
    
    # asharebps = asharebps.loc[ashare.index, ashare.columns]
    # ashare = (ashare * asharebps).astype(int)
    # ashare = ashare.loc[:, ashare.sum(axis=0).astype(bool)]
    
    ashare.to_csv('data/stockpools/ASHARE_COMPONENT.csv')
    return
    
    
def generate_ashare():
    ashare = pd.read_csv('data/stockpools/ASHARE_COMPONENT.csv', index_col=0, parse_dates=True)
    bm = pd.read_csv('data/features/bm.csv', index_col=0, parse_dates=True)
    bm = bm.loc[ashare.index, ashare.columns]
    ashare = (ashare * (bm > 0).fillna(0).astype(int)).astype(int)
    ashare.to_csv('data/stockpools/ASHARE_COMPONENT.csv')
    
    
def generate_ashare_trade():
    ashare = pd.read_csv('data/stockpools/ASHARE_COMPONENT.csv', index_col=0, parse_dates=True)
    df_list = []
    for name in tqdm(ashare.columns):
        try:
            df = pd.read_csv(f'data/features/raw/volume/{name}.csv', index_col=0, parse_dates=True)
            df.columns = [name]
        except:
            df = pd.DataFrame(columns=[name])
        df_list.append(df)
    volume = _concat_by_col(df_list)
    volume = pd.concat([volume, pd.DataFrame(index=ashare.index)], axis=1)
    volume.sort_index(inplace=True)
    volume = volume.loc[~volume.index.duplicated(keep='first')]
    volume = volume.fillna(0)
    ashare_trade = ashare * volume.astype(bool).astype(int)
    
    df_list = []
    for name in tqdm(ashare.columns):
        try:
            df = pd.read_csv(f'data/features/raw/adjust_ohlc/{name}.csv', index_col=0, parse_dates=True)
            # df = pd.read_csv(f'data/features/raw/ohlc/{name}.csv', index_col=0, parse_dates=True)
        except:
            df = pd.DataFrame(columns=['adjust_open', 'adjust_high', 'adjust_low', 'adjust_close'])
            # df = pd.DataFrame(columns=['open', 'high', 'low', 'close'])
        df['zero_amplitude'] = (df.max(axis=1) == df.min(axis=1))
        df['rtn'] = df['adjust_close'].pct_change()
        # df['rtn'] = df['close'].pct_change()
        df['bs_limit'] = (df['rtn'] > 0.095) | (df['rtn'] < -0.095)
        df_list.append((df['zero_amplitude'].astype(bool) & df['bs_limit'].astype(bool)).to_frame(name))
    bslimit = _concat_by_col(df_list)
    bslimit = pd.concat([bslimit, pd.DataFrame(index=ashare.index)], axis=1)
    bslimit.sort_index(inplace=True)
    bslimit = bslimit.loc[~bslimit.index.duplicated(keep='first')]
    bslimit = bslimit.fillna(0)
    bslimit = (~bslimit.astype(bool)).astype(int)
    ashare_trade = ashare_trade * bslimit
        
    ashare_trade.to_csv('data/stockpools/ASHARE_TRADE_COMPONENT.csv')


if __name__ == '__main__':
    # download_ashare(pd.to_datetime("2010-01-01"), pd.to_datetime("2020-11-30"))
    # generate_ashare()
    generate_ashare_trade()
