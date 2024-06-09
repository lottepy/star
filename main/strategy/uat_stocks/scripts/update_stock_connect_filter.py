import sys
sys.path.append(".")
from strategy.commonalgo.data.choice_proxy_client import choice_client as c
from datetime import datetime

import pandas as pd


if __name__ == '__main__':
    start_date = pd.to_datetime('2017-01-01')
    end_date = datetime.today()
    try:
        old_df = pd.read_csv("//192.168.9.170/share/alioss/1_StockStrategy/data/filter/"
                             "StockConnectFilter.csv",
                             index_col=0, parse_dates=True)
    except FileNotFoundError:
        stock_list = pd.read_csv("strategy/stocks/stock_universe/aqm_cn_stock.csv",
                                 index_col=0)['SECUCODE'].tolist()
        old_df = pd.DataFrame(columns=sorted(stock_list))
    if len(old_df) > 0:
        last_date = old_df.index[-1]
    else:
        last_date = start_date
    start_date = max(start_date, last_date)
    new_df_list = [old_df]
    date_set = pd.date_range(start_date.strftime("%Y-%m-%d"),
                             end_date.strftime("%Y-%m-%d"), freq="W")
    for i in date_set:
        df = c.sector("001047", i.strftime("%Y-%m-%d"))[[0]].rename(
            columns={0: 'symbols'}
        )
        one_filter = pd.DataFrame(1.0, index=[i], columns=df['symbols'].tolist())
        new_df_list.append(one_filter)
    new_df = pd.concat(new_df_list, axis=0).fillna(0)[old_df.columns]
    new_df = new_df[~new_df.index.duplicated(keep='last')]
    new_df.to_csv("//192.168.9.170/share/alioss/1_StockStrategy/data/filter/"
                  "StockConnectFilter.csv")
