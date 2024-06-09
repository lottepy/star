import pandas as pd
import numpy as np
from collections import defaultdict
from tqdm import tqdm

all_month_date_set = pd.date_range("2012-01-01", "2020-09-30", freq="M")
all_week_date_set = pd.date_range("2012-01-01", "2020-09-30", freq="W")
all_day_date_set = pd.date_range("2012-01-01", "2020-09-30", freq="D")
all_stock_set = pd.read_csv(
    "data/aqm_cn_stock.csv", index_col=0).iloc[:, 0].values.tolist()


def _ffill(ori_dict, target_date_set):
    ori_date = np.array(sorted(ori_dict.keys()))
    output_dict = {}
    for date in target_date_set:
        if date >= np.min(ori_date):
            output_dict[date] = ori_dict[np.max(ori_date[ori_date <= date])]
    return output_dict


def ashare_filter():
    data_dict = {}
    for date in all_week_date_set:
        date_str = date.strftime("%Y-%m-%d")
        df = pd.read_csv(f'data/ashare/{date_str}.csv', index_col=0)
        data_dict[date] = df
    new_data_dict = _ffill(data_dict, all_day_date_set)
    for date, data in new_data_dict.items():
        data.to_csv(f'filter/ashare/{date.strftime("%Y-%m-%d")}.csv')


def price_limit_filter():
    data_dict = defaultdict(list)
    for stock in tqdm(all_stock_set):
        df = pd.read_csv(f'data/daily_data/{stock}.csv',
                         index_col=0, parse_dates=True)
        adj_close = df['adj_close'].pct_change()
        for i in adj_close[adj_close > 0.098].index:
            data_dict[i].append(stock)
        for i in adj_close[adj_close < -0.098].index:
            data_dict[i].append(stock)
    for date in tqdm(all_day_date_set):
        stock_list = sorted(data_dict.get(date, []))
        df = pd.DataFrame(stock_list, columns=['symbols'])
        df.to_csv(f'filter/price_limit/98/{date.strftime("%Y-%m-%d")}.csv')

    data_dict = defaultdict(list)
    for stock in tqdm(all_stock_set):
        df = pd.read_csv(f'data/daily_data/{stock}.csv',
                         index_col=0, parse_dates=True)
        adj_close = df['adj_close'].pct_change()
        for i in adj_close[adj_close > 0.099].index:
            data_dict[i].append(stock)
        for i in adj_close[adj_close < -0.099].index:
            data_dict[i].append(stock)
    for date in tqdm(all_day_date_set):
        stock_list = sorted(data_dict.get(date, []))
        df = pd.DataFrame(stock_list, columns=['symbols'])
        df.to_csv(f'filter/price_limit/99/{date.strftime("%Y-%m-%d")}.csv')


def suspension():
    data_dict = defaultdict(list)
    for stock in tqdm(all_stock_set):
        df = pd.read_csv(f'data/daily_data/{stock}.csv',
                         index_col=0, parse_dates=True)
        amount = df['amount']
        for i in amount[amount < 1].index:
            data_dict[i].append(stock)
    for date in tqdm(all_day_date_set):
        stock_list = sorted(data_dict.get(date, []))
        df = pd.DataFrame(stock_list, columns=['symbols'])
        df.to_csv(f'filter/suspension/{date.strftime("%Y-%m-%d")}.csv')


def price_level():
    data_dict = defaultdict(list)
    for stock in tqdm(all_stock_set):
        df = pd.read_csv(f'data/daily_data/{stock}.csv',
                         index_col=0, parse_dates=True)
        close_ = df['close'].shift(1)
        for i in close_[close_ < 1].index:
            data_dict[i].append(stock)
    for date in tqdm(all_day_date_set):
        stock_list = sorted(data_dict.get(date, []))
        df = pd.DataFrame(stock_list, columns=['symbols'])
        df.to_csv(f'filter/price_level/1/{date.strftime("%Y-%m-%d")}.csv')

    data_dict = defaultdict(list)
    for stock in tqdm(all_stock_set):
        df = pd.read_csv(f'data/daily_data/{stock}.csv',
                         index_col=0, parse_dates=True)
        close_ = df['close'].shift(1)
        for i in close_[close_ < 2].index:
            data_dict[i].append(stock)
    for date in tqdm(all_day_date_set):
        stock_list = sorted(data_dict.get(date, []))
        df = pd.DataFrame(stock_list, columns=['symbols'])
        df.to_csv(f'filter/price_level/2/{date.strftime("%Y-%m-%d")}.csv')


def amount():
    threshold = [500000,
                 1000000,
                 2500000,
                 5000000,
                 10000000,
                 25000000,
                 50000000,
                 100000000]
    data_dict_list = [defaultdict(list) for i in range(len(threshold))]
    for stock in tqdm(all_stock_set):
        df = pd.read_csv(f'data/daily_data/{stock}.csv',
                         index_col=0, parse_dates=True)
        amount = df['amount']
        for j in range(len(threshold)):
            for i in amount[amount < threshold[j]].index:
                data_dict_list[j][i].append(stock)
    for date in tqdm(all_day_date_set):
        for j in range(len(threshold)):
            stock_list = sorted(data_dict_list[j].get(date, []))
            df = pd.DataFrame(stock_list, columns=['symbols'])
            df.to_csv(f'filter/amount/{j+1}/{date.strftime("%Y-%m-%d")}.csv')


def last_amount():
    threshold = [500000,
                 1000000,
                 2500000,
                 5000000,
                 10000000,
                 25000000,
                 50000000,
                 100000000]
    data_dict_list = [defaultdict(list) for i in range(len(threshold))]
    for stock in tqdm(all_stock_set):
        df = pd.read_csv(f'data/daily_data/{stock}.csv',
                         index_col=0, parse_dates=True)
        amount = df['amount'].shift(1)
        for j in range(len(threshold)):
            for i in amount[amount < threshold[j]].index:
                data_dict_list[j][i].append(stock)
    for date in tqdm(all_day_date_set):
        for j in range(len(threshold)):
            stock_list = sorted(data_dict_list[j].get(date, []))
            df = pd.DataFrame(stock_list, columns=['symbols'])
            df.to_csv(f'filter/last_amount/{j+1}/{date.strftime("%Y-%m-%d")}.csv')


def market_cap():
    threshold = [2000000,
                 4000000,
                 10000000,
                 20000000,
                 40000000,
                 100000000,
                 200000000,
                 400000000]
    data_dict_list = [defaultdict(list) for i in range(len(threshold))]
    for stock in tqdm(all_stock_set):
        df = pd.read_csv(f'data/daily_data/{stock}.csv',
                         index_col=0, parse_dates=True)
        market_cap = df['mkt_cap_ard']
        for j in range(len(threshold)):
            for i in market_cap[market_cap < threshold[j]].index:
                data_dict_list[j][i].append(stock)
    for date in tqdm(all_day_date_set):
        for j in range(len(threshold)):
            stock_list = sorted(data_dict_list[j].get(date, []))
            df = pd.DataFrame(stock_list, columns=['symbols'])
            df.to_csv(f'filter/market_cap/{j+1}/{date.strftime("%Y-%m-%d")}.csv')


def last_market_cap():
    threshold = [2000000,
                 4000000,
                 10000000,
                 20000000,
                 40000000,
                 100000000,
                 200000000,
                 400000000]
    data_dict_list = [defaultdict(list) for i in range(len(threshold))]
    for stock in tqdm(all_stock_set):
        df = pd.read_csv(f'data/daily_data/{stock}.csv',
                         index_col=0, parse_dates=True)
        market_cap = df['mkt_cap_ard'].shift(1)
        for j in range(len(threshold)):
            for i in market_cap[market_cap < threshold[j]].index:
                data_dict_list[j][i].append(stock)
    for date in tqdm(all_day_date_set):
        for j in range(len(threshold)):
            stock_list = sorted(data_dict_list[j].get(date, []))
            df = pd.DataFrame(stock_list, columns=['symbols'])
            df.to_csv(f'filter/last_market_cap/{j+1}/{date.strftime("%Y-%m-%d")}.csv')


def small_market_cap():
    data_list = []
    for stock in tqdm(all_stock_set):
        df = pd.read_csv(f'data/daily_data/{stock}.csv',
                         index_col=0, parse_dates=True)
        market_cap = df[['mkt_cap_ard']].shift(1)
        market_cap.columns = [stock]
        data_list.append(market_cap)
    df = pd.concat(data_list, axis=1)
    data_dict = {}
    for date in tqdm(all_day_date_set):
        if date in df.index:
            ds = df.loc[date]
            ds = ds.dropna()
            data_dict[date] = ds[ds.rank(pct=True) <= 0.1].index.tolist()
    for date in tqdm(all_day_date_set):
        stock_list = sorted(data_dict.get(date, []))
        df = pd.DataFrame(stock_list, columns=['symbols'])
        df.to_csv(f'filter/small_market_cap/{date.strftime("%Y-%m-%d")}.csv')


if __name__ == '__main__':
    ashare_filter()
    price_limit_filter()
    suspension()
    price_level()
    amount()
    last_amount()
    market_cap()
    last_market_cap()
    small_market_cap()
