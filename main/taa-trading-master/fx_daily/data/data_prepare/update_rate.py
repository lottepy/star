# 把TN和FP转化成利率
# 用时：43s
import pandas as pd
from datetime import datetime
import os


ndf_ccp_map = {
	'NTN1M': 'USDTWD',
	'KWN1M': 'USDKRW',
	'IRN1M': 'USDINR',
	'IHN1M': 'USDIDR',
}

ndf_FP_SCALE_map = {'IHN1M': 0,
					'IRN1M': 2,
					'KWN1M': 0,
					'NTN1M': 0}

ENV = os.getenv('DYMON_FX_ENV', 'dev_algo')
# data path
# root_path = "//192.168.9.170/share/alioss/0_DymonFx/daily_data/"
root_path = {
	'dev_algo': "//192.168.9.170/share/alioss/0_DymonFx/daily_data/",
	'dev_jeff': '../0_DymonFx/daily_data/',
	'live': '../0_DymonFx/daily_data/'
}[ENV]
# root_path = 'https://aqm-algo.oss-cn-hongkong.aliyuncs.com/0_DymonFx/daily_data/'
# target_path = "//192.168.9.170/share/alioss/0_DymonFx/parse_data/"
target_path = {
	'dev_algo': "//192.168.9.170/share/alioss/0_DymonFx/parse_data/",
	'dev_jeff': '../0_DymonFx/parse_data/',
	'live': '../0_DymonFx/parse_data/'
}[ENV]
spot_TN_BGNL_path = os.path.join(root_path, 'spot_TN')
spot_ccp_BGNL_path = os.path.join(root_path, 'spot_BGNL')
ndf_fp_BGNL_path = os.path.join(root_path, 'ndf_KWN1M')
ndf_ccp_BGNL_path = os.path.join(root_path, 'ndf_BGNL')
USD_rate_path = os.path.join(root_path, 'USDR1T/USDR1T CMP Curncy.csv')
calendar_path = os.path.join(root_path, 'lily/calendar_data.xlsx')
spot_FWD_SCALE_path = os.path.join(root_path, 'lily/FWD_SCALE.csv')
df_calendar = pd.read_excel(calendar_path).iloc[1:]
df_USD_rate = pd.read_csv(USD_rate_path, index_col=0, parse_dates=True)
df_spot_FWD_SCALE = pd.read_csv(spot_FWD_SCALE_path, index_col=0)

# dates
list_buzdays = list(pd.bdate_range(start='1990-01-01', end='2025-12-31'))


# functions
# given specific date, give delta day between next day and the day after next day
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


# give next delta day to a list of dates
def get_delta_T(list_calendar_dates, list_non_holidays):
	list_calendar_dates.sort()
	list_non_holidays.sort()
	list_delta_T = [search_next(x, list_non_holidays) for x in list_calendar_dates]
	return list_delta_T


def tn_to_rate(major_ccp):
	for ccp in major_ccp:
		rate_df = pd.DataFrame()
		ccp_name = ccp.replace('USD', '')
		ccp_tn_path = os.path.join(spot_TN_BGNL_path, f'{ccp_name}TN BGNL Curncy.csv')
		ccp_path = os.path.join(spot_ccp_BGNL_path, f'{ccp} BGNL Curncy.csv')
		ccp_tn = pd.read_csv(ccp_tn_path, index_col=0, parse_dates=True)
		ccp_fx = pd.read_csv(ccp_path, index_col=0, parse_dates=True)
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
		if is_USD_left_leg:
			rate_df[other_ccy + '_LAST'] = (360 * ccp_tn['TN_LAST'] / 10 ** FWD_SCALE / ccp_tn['delta_T'] / ccp_tn[
				'fx_last'] +
											ccp_tn['USD_rate'] / 100) * 100
			rate_df[other_ccy + '_ASK'] = (360 * ccp_tn['TN_BID'] / 10 ** FWD_SCALE / ccp_tn['delta_T'] / ccp_tn[
				'fx_last'] +
										   ccp_tn['USD_rate'] / 100) * 100
			rate_df[other_ccy + '_BID'] = (360 * ccp_tn['TN_ASK'] / 10 ** FWD_SCALE / ccp_tn['delta_T'] / ccp_tn[
				'fx_last'] +
										   ccp_tn['USD_rate'] / 100) * 100
		else:
			rate_df[other_ccy + '_LAST'] = (-360 * ccp_tn['TN_LAST'] / 10 ** FWD_SCALE / ccp_tn['delta_T'] / ccp_tn[
				'fx_last'] +
											ccp_tn['USD_rate'] / 100) * 100
			rate_df[other_ccy + '_ASK'] = (-360 * ccp_tn['TN_ASK'] / 10 ** FWD_SCALE / ccp_tn['delta_T'] / ccp_tn[
				'fx_last'] +
										   ccp_tn['USD_rate'] / 100) * 100
			rate_df[other_ccy + '_BID'] = (-360 * ccp_tn['TN_BID'] / 10 ** FWD_SCALE / ccp_tn['delta_T'] / ccp_tn[
				'fx_last'] +
										   ccp_tn['USD_rate'] / 100) * 100
		rate_df.index = ccp_tn.index
		# check bid ask

		if 'rate_spot' not in locals():
			rate_spot = rate_df
		else:
			rate_spot = rate_spot.join(rate_df, how='outer')
	rate_spot.ffill(inplace=True)
	return rate_spot


def fp_to_rate(ndf_fp):
	for fp in ndf_fp:
		rate_df = pd.DataFrame()
		ccp = ndf_ccp_map[fp]
		fp_path = os.path.join(ndf_fp_BGNL_path, f'{fp} BGNL Curncy.csv')
		ccp_path = os.path.join(ndf_ccp_BGNL_path, f'{fp[:3]}+1M BGNL Curncy.csv')
		ccp_fp = pd.read_csv(fp_path, index_col=0, parse_dates=True)
		ccp_fx = pd.read_csv(ccp_path, index_col=0, parse_dates=True)
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
		other_ccy = ccp.replace('USD', '')

		rate_df[other_ccy + '_LAST'] = (360 * fp_point_last / ccp_fp['delta_T'] / fx_forward + ccp_fp[
			'USD_rate'] / 100) * 100
		rate_df[other_ccy + '_ASK'] = (360 * fp_point_bid / ccp_fp['delta_T'] / fx_forward + ccp_fp[
			'USD_rate'] / 100) * 100
		rate_df[other_ccy + '_BID'] = (360 * fp_point_ask / ccp_fp['delta_T'] / fx_forward + ccp_fp[
			'USD_rate'] / 100) * 100
		rate_df.index = ccp_fp.index
		if 'rate_fp' not in locals():
			rate_fp = rate_df
		else:
			rate_fp = rate_fp.join(rate_df, how='outer')
	rate_fp.ffill(inplace=True)
	return rate_fp


def process_rate_update():
	# currency pair
	spot_ccp = ['NZDJPY', 'NOKSEK', 'GBPJPY', 'GBPCHF', 'GBPCAD',
				'EURJPY', 'EURGBP', 'EURCHF', 'EURCAD', 'CADJPY',
				'CADCHF', 'AUDNZD', 'AUDJPY', 'AUDCAD', 'USDSEK',
				'USDNOK', 'USDJPY', 'USDCHF', 'USDCAD', 'NZDUSD',
				'GBPUSD', 'EURUSD', 'AUDUSD', 'USDZAR', 'USDSGD',
				'USDTHB']

	major_ccp = [ccp for ccp in spot_ccp if 'USD' in ccp]
	ndf_fp = ['IHN1M', 'IRN1M', 'KWN1M', 'NTN1M']

	ts = datetime.now()
	rate_spot = tn_to_rate(major_ccp)
	rate_fp = fp_to_rate(ndf_fp)

	rate_all = rate_spot.join(rate_fp, how='outer')

	rate_all_last = rate_all[[column for column in rate_all.columns if 'LAST' in column]]
	rate_all_ask = rate_all[[column for column in rate_all.columns if 'ASK' in column]]
	rate_all_bid = rate_all[[column for column in rate_all.columns if 'BID' in column]]
	rate_all_last = rate_all_last.join(df_USD_rate[['PX_LAST']].rename(columns={'PX_LAST': 'USD_LAST'}))
	rate_all_ask = rate_all_ask.join(df_USD_rate[['PX_LAST']].rename(columns={'PX_LAST': 'USD_ASK'}))
	rate_all_bid = rate_all_bid.join(df_USD_rate[['PX_LAST']].rename(columns={'PX_LAST': 'USD_BID'}))

	te = datetime.now()
	print(f"用时 {te-ts}")

	rate_all_last.to_csv(os.path.join(target_path, "FX_ON_RATE_LAST.csv"))
	rate_all_ask.to_csv(os.path.join(target_path, "FX_ON_RATE_ASK.csv"))
	rate_all_bid.to_csv(os.path.join(target_path, "FX_ON_RATE_BID.csv"))

if __name__ == '__main__':
	process_rate_update()