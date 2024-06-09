from datetime import datetime

import pandas as pd
from datamaster import dm_client
import os
from .ccy_mapping import CCY_INTEREST_TICKER, CCY_TICKER, ndf_FP_SCALE_map, USD_RATE_TICKER


_this_file_path = os.path.abspath(__file__)
_backtest_engine_path = os.path.dirname(os.path.dirname(_this_file_path))

CALENDAR_PATH = os.path.join(_backtest_engine_path, 'data/constants/calendar_data.xlsx')
SPOT_FWD_SCALE_PATH = os.path.join(_backtest_engine_path, 'data/constants/FWD_SCALE.csv')


def tn_to_rate(ccys,start,end):
	df_calendar = pd.read_excel(CALENDAR_PATH).iloc[1:]
	df_spot_FWD_SCALE = pd.read_csv(SPOT_FWD_SCALE_PATH, index_col=0)


	list_buzdays = list(pd.bdate_range(start='1990-01-01', end='2025-12-31'))
	df = dm_client.get_historical_data(symbols = [USD_RATE_TICKER], start_date = str(start.date()),end_date = str(end.date()), fields = ['close'],
										to_dataframe = True,fill_method = 'ffill')
	df = df[USD_RATE_TICKER]
	df = df.set_index('date')
	df['PX_BID'] = df['close']
	df['PX_ASK'] = df['close']
	df['PX_LAST'] = df['close']
	df.index = pd.to_datetime(df.index, format='%Y-%m-%d')
	del df['close']
	df_USD_rate = df

	bbg_tickers = list( map(lambda x:CCY_INTEREST_TICKER[x],ccys))
	TN_result = dm_client.get_historical_data(symbols = bbg_tickers, start_date = str(start.date()),end_date = str(end.date()), fields = ['close'],
										to_dataframe = True,fill_method = 'ffill')

	bbg_tickers = list( map(lambda x:CCY_TICKER[x],ccys))
	SPOT_result = dm_client.get_historical_data(symbols = bbg_tickers, start_date = str(start.date()),end_date = str(end.date()), fields = ['close'],
										to_dataframe = True,fill_method = 'ffill')
	for ccy in ccys:
		rate_df = pd.DataFrame()
		bbg_tn_tickers = CCY_INTEREST_TICKER[ccy]
		bbg_spot_tickers = CCY_TICKER[ccy]

		df = TN_result[bbg_tn_tickers]
		df = df.set_index('date')
		df['TN_BID'] = df['close']
		df['TN_ASK'] = df['close']
		df['TN_LAST'] = df['close']
		df.index = pd.to_datetime(df.index, format='%Y-%m-%d')
		del df['close']
		ccp_tn = df

		df = SPOT_result[bbg_spot_tickers]
		df = df.set_index('date')
		df['PRICE_BID'] = df['close']
		df['PRICE_ASK'] = df['close']
		df['PRICE_LAST'] = df['close']
		df.index = pd.to_datetime(df.index, format='%Y-%m-%d')
		del df['close']
		ccp_fx = df

		ccp = CCY_TICKER[ccy].split(" ")[0]
		ccp_tn = ccp_tn.join(ccp_fx[['PRICE_LAST']].rename(columns={'PRICE_LAST': 'fx_last'}))
		ccp_tn['fx_last'].ffill(inplace=True)
		ccp_tn = ccp_tn.join(df_USD_rate[['PX_LAST']].rename(columns={'PX_LAST': 'USD_rate'}))
		ccp_tn['USD_rate'].ffill(inplace=True)
		other_ccy = ccp.replace('USD', '')
		other_ccy_calendar = df_calendar[f'{other_ccy} Curncy'].tolist()
		USD_calendar = df_calendar['USD Curncy'].tolist()
		list_holidays = list(set(other_ccy_calendar + USD_calendar))
		list_non_holidays = list(set(list_buzdays).difference(set(list_holidays)))
		list_delta_T = get_delta_T(ccp_tn.index.tolist(), list_non_holidays)
		ccp_tn['delta_T'] = list_delta_T
		is_USD_left_leg = ('USD' == ccp[:3])
		FWD_SCALE = df_spot_FWD_SCALE.loc[ccp, 'FWD_SCALE']
		for field in ['BID','ASK','LAST']:
			direction = (1 if is_USD_left_leg else -1)
			rate_df[f'{other_ccy}_{field}'] = (direction * 360 * ccp_tn[f'TN_{field}'] / 10 ** FWD_SCALE / ccp_tn['delta_T'] / ccp_tn['fx_last'] +
											ccp_tn['USD_rate'] / 100) * 100
		rate_df.index = ccp_tn.index
		# check bid ask

		if 'rate_spot' not in locals():
			rate_spot = rate_df
		else:
			rate_spot = rate_spot.join(rate_df, how='outer')
	rate_spot.ffill(inplace=True)
	return rate_spot


def fp_to_rate(ndf_fp,start,end):

	bbg_tickers = list( map(lambda x:CCY_INTEREST_TICKER[x],ndf_fp))
	FP_result = dm_client.get_historical_data(symbols = bbg_tickers, start_date = str(start.date()),end_date = str(end.date()), fields = ['close'],
										to_dataframe = True,fill_method = 'ffill')

	bbg_tickers = list( map(lambda x:CCY_TICKER[x],ndf_fp))
	SPOT_result = dm_client.get_historical_data(symbols = bbg_tickers, start_date = str(start.date()),end_date = str(end.date()), fields = ['close'],
										to_dataframe = True,fill_method = 'ffill')

	df = dm_client.get_historical_data(symbols = [USD_RATE_TICKER], start_date = str(start.date()),end_date = str(end.date()), fields = ['close'],
										to_dataframe = True,fill_method = 'ffill')
	df = df[USD_RATE_TICKER]
	df = df.set_index('date')
	df['PX_BID'] = df['close']
	df['PX_ASK'] = df['close']
	df['PX_LAST'] = df['close']
	df.index = pd.to_datetime(df.index, format='%Y-%m-%d')
	del df['close']
	df_USD_rate = df

	for fp in ndf_fp:
		rate_df = pd.DataFrame()
		bbg_fp_tickers = CCY_INTEREST_TICKER[fp]
		bbg_spot_tickers = CCY_TICKER[fp]

		df = FP_result[bbg_fp_tickers]
		df = df.set_index('date')
		df['FP_BID'] = df['close']
		df['FP_ASK'] = df['close']
		df['FP_LAST'] = df['close']
		df.index = pd.to_datetime(df.index, format='%Y-%m-%d')
		del df['close']
		ccp_fp = df

		df = SPOT_result[bbg_spot_tickers]
		df = df.set_index('date')
		df['PRICE_BID'] = df['close']
		df['PRICE_ASK'] = df['close']
		df['PRICE_LAST'] = df['close']
		df.index = pd.to_datetime(df.index, format='%Y-%m-%d')
		del df['close']
		ccp_fx = df

		ccp_fp = ccp_fp.join(ccp_fx[['PRICE_LAST']].rename(columns={'PRICE_LAST': 'fx_last'}))
		ccp_fp['fx_last'].ffill(inplace=True)
		ccp_fp = ccp_fp.join(df_USD_rate[['PX_LAST']].rename(columns={'PX_LAST': 'USD_rate'}))
		ccp_fp['USD_rate'].ffill(inplace=True)
		ccp_fp['delta_T'] = 30
		FWD_SCALE = ndf_FP_SCALE_map[fp]
		fp_point_last = ccp_fp[f'FP_LAST'] / 10 ** FWD_SCALE
		fp_point_ask = ccp_fp[f'FP_ASK'] / 10 ** FWD_SCALE
		fp_point_bid = ccp_fp[f'FP_BID'] / 10 ** FWD_SCALE
		fx_forward = ccp_fp['fx_last']
		other_ccy = fp.replace('USD', '')

		rate_df[other_ccy+'_LAST'] = (360 * fp_point_last / ccp_fp['delta_T'] / fx_forward + ccp_fp[
									 'USD_rate'] / 100) * 100
		rate_df[other_ccy+'_ASK'] = (360 * fp_point_bid / ccp_fp['delta_T'] / fx_forward + ccp_fp[
									 'USD_rate'] / 100) * 100
		rate_df[other_ccy+'_BID'] = (360 * fp_point_ask / ccp_fp['delta_T'] / fx_forward + ccp_fp[
									 'USD_rate'] / 100) * 100
		rate_df.index = ccp_fp.index
		if 'rate_fp' not in locals():
			rate_fp = rate_df
		else:
			rate_fp = rate_fp.join(rate_df, how='outer')
	rate_fp.ffill(inplace=True)
	
	return rate_fp

def usd_to_rate(start,end):
	df = dm_client.get_historical_data(symbols = [USD_RATE_TICKER], start_date = str(start.date()),end_date = str(end.date()), fields = ['close'],
										to_dataframe = True,fill_method = 'ffill')
	df = df[USD_RATE_TICKER]
	df = df.set_index('date')
	df.index = pd.to_datetime(df.index, format='%Y-%m-%d')
	df['USD_BID'] = df['close']
	df['USD_ASK'] = df['close']
	df['USD_LAST'] = df['close']
	del df['close']
	return df


def get_delta_T(list_calendar_dates, list_non_holidays):
	list_calendar_dates.sort()
	list_non_holidays.sort()
	list_delta_T = [search_next(x, list_non_holidays) for x in list_calendar_dates]
	return list_delta_T

def search_next(x, list_non_holidays):
	list_non_holidays_copy = list_non_holidays.copy()
	if x not in list_non_holidays:
		list_non_holidays_copy.append(x)
		list_non_holidays_copy.sort()
	next_index = list_non_holidays_copy.index(x) + 2
	if next_index < len(list_non_holidays_copy):
		return (list_non_holidays_copy[next_index] - list_non_holidays_copy[next_index - 1]).days
	else:
		return None
