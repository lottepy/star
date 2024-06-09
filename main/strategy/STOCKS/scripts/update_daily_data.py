from datamaster import dm_client
import pandas as pd
from datetime import datetime
import os
from tqdm import tqdm


dm_client.start()


def update_daily(update_only=True):
    if not os.path.exists('strategy/stocks/data/daily_data'):
        os.mkdir('strategy/stocks/data/daily_data')
    all_stock_set = pd.read_csv('strategy/stocks/stock_universe/aqm_cn_stock.csv', index_col=0)['SECUCODE'].values
    start_date = pd.to_datetime('2010-01-01')
    end_date = pd.to_datetime(datetime.today().strftime('%Y-%m-%d'))
    for stock in tqdm(all_stock_set):
        try:
            df = pd.read_csv(f'strategy/stocks/data/daily_data/{stock}.csv', index_col=0, parse_dates=True)
        except FileNotFoundError:
            df = pd.DataFrame()
        if len(df) > 0:
            last_updated_date = df.index[-1]
        else:
            last_updated_date = start_date
        if update_only:
            start_date_ = last_updated_date.strftime("%Y-%m-%d")
        else:
            start_date_ = start_date.strftime("%Y-%m-%d")
        end_date_ = end_date.strftime("%Y-%m-%d")
        res = dm_client.historical(symbols=stock,
                                      start_date=start_date_,
                                      end_date=end_date_,
                                      fields='open,high,low,close,volume,amount,choice_turnover',
                                      calendar='SSE',
                                      fill_method='ffill')
        new_df = pd.DataFrame(res['values'][stock], columns=res['fields'])
        new_df.set_index('date', inplace=True, drop=True)
        new_df.index = pd.to_datetime(new_df.index)
        df = pd.concat([df, new_df], axis=0)
        df = df[~df.index.duplicated(keep='last')]
        df.to_csv(f'strategy/stocks/data/daily_data/{stock}.csv')
    return


def update_daily_adjust(update_only=True):
    if not os.path.exists('strategy/stocks/data/daily_data_adjust'):
        os.mkdir('strategy/stocks/data/daily_data_adjust')
    all_stock_set = pd.read_csv('strategy/stocks/stock_universe/aqm_cn_stock.csv', index_col=0)['SECUCODE'].values
    start_date = pd.to_datetime('2010-01-01')
    end_date = pd.to_datetime(datetime.today().strftime('%Y-%m-%d'))
    for stock in tqdm(all_stock_set):
        try:
            df = pd.read_csv(f'strategy/stocks/data/daily_data_adjust/{stock}.csv', index_col=0, parse_dates=True)
        except FileNotFoundError:
            df = pd.DataFrame()
        if len(df) > 0:
            last_updated_date = df.index[-1]
        else:
            last_updated_date = start_date
        if update_only:
            start_date_ = last_updated_date.strftime("%Y-%m-%d")
        else:
            start_date_ = start_date.strftime("%Y-%m-%d")
        end_date_ = end_date.strftime("%Y-%m-%d")
        res = dm_client.historical(symbols=stock,
                                      start_date=start_date_,
                                      end_date=end_date_,
                                      fields='open,high,low,close,volume,amount,choice_turnover',
                                      calendar='SSE',
                                      fill_method='ffill',
                                      adjust_type=3)
        new_df = pd.DataFrame(res['values'][stock], columns=res['fields'])
        new_df.set_index('date', inplace=True, drop=True)
        new_df.index = pd.to_datetime(new_df.index)
        df = pd.concat([df, new_df], axis=0)
        df = df[~df.index.duplicated(keep='last')]
        df.to_csv(f'strategy/stocks/data/daily_data_adjust/{stock}.csv')
    return
        

def update_close_agg():
    all_stock_set = pd.read_csv('strategy/stocks/stock_universe/aqm_cn_stock.csv', index_col=0)['SECUCODE'].values
    df_list = []
    for stock in tqdm(all_stock_set):
        df = pd.read_csv(f'strategy/stocks/data/daily_data/{stock}.csv', index_col=0, parse_dates=True)
        df = df[['close']].rename(columns={'close': stock})
        df_list.append(df)
    df = pd.concat(df_list, axis=1)
    df.to_csv('strategy/stocks/data/daily_close.csv')
    return


def update_adjust_close_agg():
    all_stock_set = pd.read_csv('strategy/stocks/stock_universe/aqm_cn_stock.csv', index_col=0)['SECUCODE'].values
    df_list = []
    for stock in tqdm(all_stock_set):
        df = pd.read_csv(f'strategy/stocks/data/daily_data_adjust/{stock}.csv', index_col=0, parse_dates=True)
        df = df[['close']].rename(columns={'close': stock})
        df_list.append(df)
    df = pd.concat(df_list, axis=1)
    df.to_csv('strategy/stocks/data/adjust_daily_close.csv')
    return


if __name__ == '__main__':
    update_daily()
    update_daily_adjust()
    update_close_agg()
    update_adjust_close_agg()