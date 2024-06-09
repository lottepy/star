# -*- coding:utf-8 -*-
# python 3.5+
# from futuquant import *
from lib.commonalgo.data.futu_client import FutuClient
from datetime import timedelta, datetime
# https://futunnopen.github.io/futuquant/api/Quote_API.html

quote_ctx = FutuClient(host='internal').connect

# tickers = ['US.VTI','US.HYG']
futu_symbols = ['US.GLD']
# futu_symbols = ['HK.00700', 'HK.00005', 'HK.02823']
quote_ctx.subscribe(futu_symbols, subtype_list='ORDER_BOOK')

# k line
ret, data = quote_ctx.get_market_snapshot(futu_symbols)
# ret, data = quote_ctx.get_history_kline('US.IBM', start='2017-06-20', end='2017-06-22')
# ret, data = quote_ctx.get_multi_points_history_kline(['US.VTI','US.VEA'], ['2017-06-20', '2017-06-25'], KL_FIELD.ALL, KLType.K_1M, AuType.NONE)
# ret, data = quote_ctx.get_multiple_history_kline(tickers, '2017-06-01', '2018-11-23', KLType.K_1M, AuType.NONE)
# ret1, data1 = quote_ctx.get_multiple_history_kline(tickers, '2017-06-01', '2017-07-30', KLType.K_1M, AuType.HFQ)

# ret, data = quote_ctx.request_history_kline('US.IBM', start='2017-06-20', end='2017-06-22',ktype=KLType.K_1M)
# print(data)
# quote_ctx.close()
# 获取市场快照
# 股票列表，限制最多200只股票
# futu_symbols = ['HK.00700','HK.00005','HK.02823']
# quote_ctx.subscribe(futu_symbols, [SubType.QUOTE,SubType.ORDER_BOOK])
# quote_ctx.subscribe(futu_symbols, [SubType.ORDER_BOOK])
# prices_df = quote_ctx.get_stock_quote(futu_symbols)[1]
# print(prices_df)
# get_stock_basicinfo
# 获取指定市场中特定类型的股票基本信息
# data = quote_ctx.get_stock_basicinfo(Market.HK, SecurityType.STOCK)[1]

# 获取特定板块下的股票列表
# data = quote_ctx.get_plate_stock('US.200306')[1] # 全部港股(正股)

# 获取实时摆盘数据
print(datetime.now())
data = quote_ctx.get_order_book('HK.02823')
print(datetime.now())
print (data)




