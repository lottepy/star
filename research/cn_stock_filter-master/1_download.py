import pandas as pd
from lib.commonalgo.data.choice_proxy_client import choice_client as c
import numpy as np
from tqdm import tqdm
import time


if __name__ == "__main__":
    all_month_date_set = pd.date_range("2012-01-01", "2020-09-30", freq="M")
    all_week_date_set = pd.date_range("2012-01-01", "2020-09-30", freq="W")
    all_day_date_set = pd.date_range("2012-01-01", "2020-09-30", freq="D")

    all_stock_set = pd.read_csv(
        "data/aqm_cn_stock.csv", index_col=0).iloc[:, 0].values.tolist()

    # download ashare list weekly
    # ================================================
    # for date in all_week_date_set:
    #     date_str = date.strftime("%Y-%m-%d")
    #     df = c.sector("001004", date_str)
    #     df = df[[0]].rename(columns={0: 'symbols'})
    #     df.to_csv(f'data/ashare/{date_str}.csv')
    # ================================================

    # check stock not in 3109
    # ================================================
    # stock_list = []
    # for date in tqdm(all_week_date_set):
    #     date_str = date.strftime("%Y-%m-%d")
    #     df = pd.read_csv(f'data/ashare/{date_str}.csv', index_col=0)
    #     stock_list += [i for i in df['symbols']]
    # stock_list = sorted(list(set(stock_list)))
    # stock_list = [i for i in stock_list if i not in all_stock_set]

    # for stock in tqdm(stock_list):
    #     list_data = c.css(stock, "LISTDATE,DELISTDATE", "")
    #     if list_data.iloc[0, 0] is not None:
    #         start_date = max(pd.to_datetime(list_data.iloc[0, 0]),
    #                          pd.to_datetime("2012-01-01"))
    #     else:
    #         start_date = pd.to_datetime("2012-01-01")
    #     if list_data.iloc[0, 1] is not None:
    #         end_date = min(pd.to_datetime(list_data.iloc[0, 1]),
    #                        pd.to_datetime("2020-09-30"))
    #     else:
    #         end_date = pd.to_datetime("2020-09-30")
    #     data = c.csd(stock, "OPEN,HIGH,LOW,CLOSE,AMOUNT,VOLUME",
    #                  start_date.strftime("%Y-%m-%d"),
    #                  end_date.strftime("%Y-%m-%d"),
    #                  "period=1,adjustflag=1,curtype=1,order=1,market=CNSESH")
    #     data.columns = data.columns.get_level_values(1)
    #     adj_data = c.csd(stock, "CLOSE",
    #                      start_date.strftime("%Y-%m-%d"),
    #                      end_date.strftime("%Y-%m-%d"),
    #                      "period=1,adjustflag=2,curtype=1,"
    #                      "order=1,market=CNSESH")
    #     adj_data.columns = adj_data.columns.get_level_values(1)
    #     adj_data = adj_data.add_prefix("ADJ_")
    #     data = pd.concat([data, adj_data], axis=1)
    #     data.to_csv(f"data/price/{stock}.csv")
    # ================================================
    # amount_list = []
    # adj_close_list = []
    # for stock in tqdm(all_stock_set):
    #     df = pd.read_csv('D:/projects/research/research/lib/'
    #                      'equity_strategies_weekly/research/'
    #                      f'data/daily_data_adjust/{stock}.csv',
    #                      index_col=0, parse_dates=True)
    #     amount_list.append(df[['amount']].rename(columns={'amount': stock}))
    #     adj_close_list.append(df[['close']].rename(columns={'close': stock}))
    # adj_close = pd.concat(adj_close_list, axis=1)

    # ===================================================
    # 校对junxin数据与datamaster数据 (fundamental)
    # from datamaster import dm_client
    # dm_client.start()
    # for stock in tqdm(all_stock_set):
    #     acas_df = pd.read_csv(
    #         f"data/stock_20201021/monthly_data/{stock}.csv", index_col=0, parse_dates=True)
    #     start_date = acas_df.index[0]
    #     end_date = acas_df.index[-1]
    #     dm_df = pd.DataFrame(dm_client.historical(symbols=stock,
    #                                               fields='acas_qfa_tot_oper_rev',
    #                                               start_date=start_date.strftime(
    #                                                   "%Y-%m-%d"),
    #                                               end_date=end_date.strftime("%Y-%m-%d"))['values'][stock])
    #     dm_df.set_index(0, drop=True, inplace=True)
    #     dm_df.index.name = 'datetime'
    #     dm_df.index = pd.to_datetime(dm_df.index)
    # ===================================================
    # with open('tmp2.txt', 'w') as f:
    #     for stock in tqdm(all_stock_set):
    #         daily_df1 = pd.read_csv(
    #             f"data/stock_20201021/daily_data/{stock}.csv", index_col=0, parse_dates=True)
    #         max_daily_df1 = daily_df1.pct_change().max()
    #         min_daily_df1 = daily_df1.pct_change().min()
    #         if max_daily_df1['adj_close'] > 0.105:
    #             f.write(f"{stock}'s adjust close has a abnormal maximum of {max_daily_df1['adj_close']:.4f} at {daily_df1.index[daily_df1.pct_change()['adj_close'].argmax()].strftime('%Y-%m-%d')}\n")
    #         if min_daily_df1['adj_close'] < -0.105:
    #             f.write(f"{stock}'s adjust close has a abnormal minimum of {min_daily_df1['adj_close']:.4f} at {daily_df1.index[daily_df1.pct_change()['adj_close'].argmin()].strftime('%Y-%m-%d')}\n")

    #         tafactor = daily_df1['adj_close']/daily_df1['close']
    #         daily_df2 = pd.DataFrame(dm_client.historical(symbols=stock,
    #                                          start_date='2012-01-01',
    #                                          end_date='2020-09-30',
    #                                          fields='choice_fq_factor',
    #                                          calendar='SSE')['values'][stock])
    #         daily_df2.set_index(0, inplace=True, drop=True)
    #         daily_df2.index = pd.to_datetime(daily_df2.index)
    #         df = pd.concat([tafactor, daily_df2], axis=1).dropna()
    #         df = df/df.iloc[0]
    #         if np.sum(np.abs(df[0]-df[1])) > 0.1:
    #             f.write(f"{stock} has different tafactor. ref_diff: {np.sum(np.abs(df[0]-df[1]))}\n")

    # =====================================================
    # download price data
    for stock in tqdm(all_stock_set):
        df = pd.read_csv(
            f"data/stock_20201021/daily_data/{stock}.csv", index_col=0, parse_dates=True)
        df['amount'] = df['close'] * df['volume']
        df.to_csv(f"data/daily_data/{stock}.csv")

    print(1)
