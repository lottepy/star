# -*- coding:utf-8 -*-
# pip install futu-api
# https://github.com/FutunnOpen/py-futu-api
import futu as ft
from futu import *
from lib.commonalgo.data.futu_client import FutuClient


quote_ctx = FutuClient(server='internal').connect
# 低频数据接口
market = ft.Market.HK
code = 'HK.00123'
code_list = ['HK.00123','HK.00700','HK.00005']
plate = 'HK.BK1107'

page_k = None
ret,qfq_data,page_k = quote_ctx.request_history_kline('SZ.000001',start='2019-01-01',end='2019-05-31',ktype=KLType.K_1M,autype=AuType.NONE,page_req_key=page_k)
ret,qfq_data1,page_k = quote_ctx.request_history_kline('SZ.000001',start='2019-01-01',end='2019-05-31',ktype=KLType.K_1M,autype=AuType.NONE,page_req_key=page_k)
ret,qfq_data2,page_k = quote_ctx.request_history_kline('SZ.000001',start='2019-01-01',end='2019-05-31',ktype=KLType.K_1M,autype=AuType.NONE,page_req_key=page_k)
ret,qfq_data3,page_k = quote_ctx.request_history_kline('SZ.000001',start='2019-01-01',end='2019-05-31',ktype=KLType.K_1M,autype=AuType.NONE,page_req_key=page_k)
# print()

# print(quote_ctx.get_trading_days(market, start=None, end=None))   # 获取交易日
# print(quote_ctx.get_stock_basicinfo(market, stock_type=ft.SecurityType.STOCK))   # 获取股票信息
# print(quote_ctx.get_autype_list(code_list))                                  # 获取复权因子
# print(quote_ctx.get_market_snapshot(code_list))                              # 获取市场快照
# _,data= quote_ctx.get_market_snapshot(code_list)                            # 获取市场快照
# print(quote_ctx.get_plate_list(market, ft.Plate.ALL))                         # 获取板块集合下的子板块列表
# print(quote_ctx.get_plate_stock(plate))


# 高频数据接口
# quote_ctx.subscribe(code, [ft.SubType.QUOTE, ft.SubType.TICKER, ft.SubType.K_DAY, ft.SubType.ORDER_BOOK, ft.SubType.RT_DATA, ft.SubType.BROKER])
# quote_ctx.subscribe(code_list, [ft.SubType.QUOTE, ft.SubType.TICKER, ft.SubType.K_DAY, ft.SubType.ORDER_BOOK, ft.SubType.RT_DATA, ft.SubType.BROKER])
# ret, data = quote_ctx.get_stock_quote(code)
# ret, data = quote_ctx.get_cur_kline(code, num=100, ktype=ft.KLType.K_DAY)
# ret, data = quote_ctx.get_order_book(code)
# ret, data = quote_ctx.get_rt_data(code)
# ret, data = quote_ctx.get_broker_queue(code)


# 获取板块下的股票列表

# quote_ctx = ft.OpenQuoteContext(host="rabbitmq.aqumon.com", port=22222)
# ret,data = quote_ctx.get_cur_kline('HK.00700',num=1000,ktype=KLType.K_1M)
ret,qfq_data,page_k = quote_ctx.request_history_kline('HK.00700',start='2018-01-01',end='2018-03-10',ktype=KLType.K_1M)
ret,qfq_data,page_k = quote_ctx.request_history_kline('SZ.000001',start='2019-02-25',end='2019-02-27',ktype=KLType.K_1M)
# ret,nfq_data,page_k = quote_ctx.request_history_kline('HK.00700',start='2018-01-01',end='2018-01-10',ktype=KLType.K_1M,autype=AuType.NONE)
ret,nfq_data,page_k = quote_ctx.request_history_kline('SZ.000001',start='2019-02-25',end='2019-02-27',ktype=KLType.K_1M,autype=AuType.NONE)
ret,qfq_data,page_k = quote_ctx.request_history_kline('SZ.000001',start='2019-02-25',end='2019-02-27',ktype=KLType.K_1M,autype=AuType.QFQ)
# ret,data = quote_ctx.get_history_kline('HK.00700',start='2018-01-01',end='2018-01-10',ktype=KLType.K_1M)
print (nfq_data)