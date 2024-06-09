import os
import pandas as pd

# 有一些T150缺失的数据用BGNL补
ENV = os.getenv('DYMON_FX_ENV', 'dev_algo')

# ROOT_PATH = "//192.168.9.170/share/alioss/0_DymonFx/daily_data/"
# TARGET_PATH = "//192.168.9.170/share/alioss/0_DymonFx/parse_data/"
ROOT_PATH = {
	'dev_algo': "//192.168.9.170/share/alioss/0_DymonFx/daily_data/",
	'dev_jeff': '../0_DymonFx/daily_data/',
	'live': '../0_DymonFx/daily_data/'
}[ENV]
TARGET_PATH = {
	'dev_algo': "//192.168.9.170/share/alioss/0_DymonFx/parse_data/",
	'dev_jeff': '../0_DymonFx/parse_data/',
	'live': '../0_DymonFx/parse_data/'
}[ENV]
def process_fill_ndf():

	ndf_ccp_map = {
		'NTN1M': 'USDTWD',
		'KWN1M': 'USDKRW',
		'IRN1M': 'USDINR',
		'IHN1M': 'USDIDR',
	}

	forward_ndf_ccp_map = {
		'NTN+1M': 'USDTWD',
		'KWN+1M': 'USDKRW',
		'IRN+1M': 'USDINR',
		'IHN+1M': 'USDIDR',
	}

	ndf_FP_SCALE_map = {'IHN1M': 0,
						'IRN1M': 2,
						'KWN1M': 0,
						'NTN1M': 0}

	ccp_ndf_map = dict(zip(ndf_ccp_map.values(), ndf_ccp_map.keys()))
	ccp_ndf_forward_map = dict(zip(forward_ndf_ccp_map.values(), forward_ndf_ccp_map.keys()))

	# ndf+1M t150缺失部分用 t150spot+fp来填补
	ndf_t150_spot_path = os.path.join(ROOT_PATH, "ndf_spot_T150")
	ndf_bgnl_fp_path = os.path.join(ROOT_PATH, "ndf_KWN1M")
	ndf_t150_forward_path = os.path.join(ROOT_PATH, "ndf_T150")

	ndf_t150_spot_files = os.listdir(ndf_t150_spot_path)

	for file in ndf_t150_spot_files:
		ccp = file.split(' ')[0]
		fp = ccp_ndf_map[ccp]
		forward = ccp_ndf_forward_map[ccp]
		ccp_file = os.path.join(ndf_t150_spot_path, file)
		fp_file = os.path.join(ndf_bgnl_fp_path, f"{fp} BGNL Curncy.csv")
		forward_file = os.path.join(ndf_t150_forward_path, f"{forward} T150 Curncy.csv")
		ccp_df = pd.read_csv(ccp_file, index_col=0, parse_dates=True)
		fp_df = pd.read_csv(fp_file, index_col=0, parse_dates=True)
		forward_df = pd.read_csv(forward_file, index_col=0, parse_dates=True)
		forward_df.columns = [col + '_forward' for col in forward_df.columns]
		ccp_df = ccp_df.join(forward_df, how='outer')
		ccp_df = ccp_df.join(fp_df, how='left')

		for field in ['LAST', 'BID', 'ASK']:
			ccp_field = f'PRICE_{field}'
			fp_field = f'FP_{field}'
			forward_field = f'PRICE_{field}_forward'
			scale = ndf_FP_SCALE_map[fp]
			ccp_df[ccp_field] = ccp_df[ccp_field] + (ccp_df[fp_field] / (10 ** scale))
			ccp_df.loc[forward_df.loc[forward_df[forward_field].notnull()].index, ccp_field] \
				= forward_df.loc[forward_df[forward_field].notnull(), forward_field]
		ccp_df = ccp_df.loc[:forward_df.index[-1]]
		ccp_df = ccp_df[[f'PRICE_{field}' for field in ['LAST', 'BID', 'ASK']]]
		ccp_df.ffill(inplace=True)

		ccp_df.to_csv(os.path.join(TARGET_PATH, f'T150_daily_data/{ccp}.csv'))


if __name__ == '__main__':
	process_fill_ndf()