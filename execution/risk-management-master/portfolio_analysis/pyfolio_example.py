# -*- coding: utf-8 -*-
import pyfolio as pf
import pandas as pd

# 测试单一股票
# stock_return = pf.utils.get_symbol_rets('SPY')
# pf.create_returns_tear_sheet(stock_return, live_start_date='2015-12-1')

# 测试投资组合
# portfolio_bt = pd.read_csv('../data/pv_ts_sg.csv', index_col=0)
# portfolio_rets = portfolio_bt.pct_change().fillna(0.)
# pf.create_returns_tear_sheet(portfolio_rets)

import requests

_session = requests.session()
end_point = 'https://market.aqumon.com/v1/account/summary'
params = {
	'account_id': 2,
	'access_token': 'M3za0PTfaNZg59FVwg0jVJo2uwirzMwZ'
}

resp = _session.get(end_point, params=params)
data_json = resp.json()

df = pd.DataFrame()