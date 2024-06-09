# -*- coding: utf-8 -*-
# Python 2.7

from lib.commonalgo.data.etf_dist_parser import  ETFDistParse
import pandas as pd

parse = ETFDistParse()
region_data = {}
sector_data = {}
tickers =['VTV','VWO',"SCHF"]
for ticker in tickers:
	data = parse.handle_US(ticker,{})
	region_data[ticker] = data['Region']
	sector_data[ticker] = data['Sector']

region_df = pd.DataFrame(region_data)
sector_df = pd.DataFrame(sector_data)