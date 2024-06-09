# -*- coding:utf-8 -*-
#!/usr/bin/env python
# date: 30/11/2018
# author: Xiandong QI,
# copyright: AQUMON
# description:
# python: 3.5+
import csv
import itertools
import os.path

from futuquant import *
from lib.commonalgo.data.futu_client import quote_ctx
# from ..lib.commonalgo.data import futu_client
# https://futunnopen.github.io/futuquant/api/Quote_API.html

my_path = os.path.abspath(os.path.dirname(__file__))
path = os.path.join(my_path, "../data/US_symbol_list.csv")
with open(path) as f:
    for row in f:
        test = list(csv.reader(f))
# print(test)

tickers_list = list(itertools.chain(*test))
tickers_list = [str('US.' + ticker) for ticker in tickers_list]
# print(tickers_list)
# ['US.AGG', 'US.ASHR', 'US.EMB', 'US.EWA', 'US.EWC', 'US.EWG', 'US.EWH', 'US.EWJ', 'US.EWL', 'US.EWU', 'US.EWY', 'US.EWZ', 'US.GLD', 'US.GOVT', 'US.HYG', 'US.INDA', 'US.IWS', 'US.LQD', 'US.MCHI', 'US.MUB', 'US.SCHF', 'US.SHV', 'US.USO', 'US.TIP', 'US.VBR', 'US.VNQ', 'US.VTI', 'US.VTV', 'US.VWO', 'US.XLB', 'US.XLE', 'US.XLF', 'US.XLI', 'US.XLK', 'US.XLP', 'US.XLU', 'US.XLV', 'US.XLY', 'US.BNDX', 'US.SJNK', 'US.SHYG', 'US.FLOT', 'US.SHY', 'US.IEF', 'US.IEI', 'US.VTIP', 'US.QQQ', 'US.VB', 'US.ITOT', 'US.BND']


# k line
# function `get_multiple_history_kline` can also receive `tickers_list` directly.
for ticker in tickers_list:
    ret, data = quote_ctx.get_multiple_history_kline(ticker, '2017-02-01', '2018-11-30', KLType.K_1M, AuType.NONE)
    data[0].to_csv('./data/kline_1M/{}_kline_1M.csv'.format(ticker),)
    print("{} success".format(ticker))

quote_ctx.close()
