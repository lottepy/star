# 把不同路径下daily的spot和ndf外汇数据放在一起
# 用时大概8s

import os
import pandas as pd
from datetime import datetime


def process_spot_ndf_data():
	start = datetime.now()

	ndf_ccp_map = {'IHN+1M': 'USDIDR',
				   'IRN+1M': 'USDINR',
				   'KWN+1M': 'USDKRW',
				   'NTN+1M': 'USDTWD'}
	
	ENV = os.getenv('DYMON_FX_ENV', 'dev_algo')
	# ROOT_PATH = "//192.168.9.170/share/alioss/0_DymonFx/"
	ROOT_PATH = {
		'dev_algo': "//192.168.9.170/share/alioss/0_DymonFx/",
		'dev_jeff': '../0_DymonFx/',
		'live': '../0_DymonFx/'
	}[ENV]

	DAILY_PATH = os.path.join(ROOT_PATH, 'daily_data')
	SPOT_T150_PATH = os.path.join(DAILY_PATH, 'spot_T150')
	SPOT_BGNL_PATH = os.path.join(DAILY_PATH, 'spot_BGNL')
	SPOT_CMPL_PATH = os.path.join(DAILY_PATH, 'spot_CMPL')
	NDF_T150_PATH = os.path.join(DAILY_PATH, 'ndf_T150')
	NDF_BGNL_PATH = os.path.join(DAILY_PATH, 'ndf_BGNL')
	NDF_CMPL_PATH = os.path.join(DAILY_PATH, 'ndf_CMPL')

	# TARGET_ROOT_PATH = "//192.168.9.170/share/alioss/0_DymonFx"
	TARGET_ROOT_PATH = {
		'dev_algo': "//192.168.9.170/share/alioss/0_DymonFx/",
		'dev_jeff': '../0_DymonFx/',
		'live': '../0_DymonFx/'
	}[ENV]

	TARGET_PATH = os.path.join(TARGET_ROOT_PATH, 'parse_data')
	T150_TARGET_PATH = os.path.join(TARGET_PATH, 'T150_daily_data')
	BGNL_TARGET_PATH = os.path.join(TARGET_PATH, 'BGNL_daily_data')
	CMPL_TARGET_PATH = os.path.join(TARGET_PATH, 'CMPL_daily_data')

	# 1、转化T150数据
	spot_T150_files = os.listdir(SPOT_T150_PATH)
	ndf_T150_files = os.listdir(NDF_T150_PATH)

	for file in spot_T150_files:
		file_path = os.path.join(SPOT_T150_PATH, file)
		file_df = pd.read_csv(file_path, index_col=0, parse_dates=True)
		ccp = file.split(' ')[0]
		target_path = os.path.join(T150_TARGET_PATH, f'{ccp}.csv')
		file_df.to_csv(target_path)

	for file in ndf_T150_files:
		file_path = os.path.join(NDF_T150_PATH, file)
		file_df = pd.read_csv(file_path, index_col=0, parse_dates=True)
		fp = file.split(' ')[0]
		target_path = os.path.join(T150_TARGET_PATH, f'{ndf_ccp_map[fp]}.csv')
		file_df.to_csv(target_path)

	# 2、转化BGNL数据
	spot_BGNL_files = os.listdir(SPOT_BGNL_PATH)
	ndf_BGNL_files = os.listdir(NDF_BGNL_PATH)

	for file in spot_BGNL_files:
		file_path = os.path.join(SPOT_BGNL_PATH, file)
		file_df = pd.read_csv(file_path, index_col=0, parse_dates=True)
		ccp = file.split(' ')[0]
		target_path = os.path.join(BGNL_TARGET_PATH, f'{ccp}.csv')
		file_df.to_csv(target_path)

	for file in ndf_BGNL_files:
		file_path = os.path.join(NDF_BGNL_PATH, file)
		file_df = pd.read_csv(file_path, index_col=0, parse_dates=True)
		fp = file.split(' ')[0]
		target_path = os.path.join(BGNL_TARGET_PATH, f'{ndf_ccp_map[fp]}.csv')
		file_df.to_csv(target_path)

	# 3、转化CMPL数据
	spot_CMPL_files = os.listdir(SPOT_CMPL_PATH)
	ndf_CMPL_files = os.listdir(NDF_CMPL_PATH)

	for file in spot_CMPL_files:
		file_path = os.path.join(SPOT_CMPL_PATH, file)
		file_df = pd.read_csv(file_path, index_col=0, parse_dates=True)
		ccp = file.split(' ')[0]
		target_path = os.path.join(CMPL_TARGET_PATH, f'{ccp}.csv')
		file_df.to_csv(target_path)

	for file in ndf_CMPL_files:
		file_path = os.path.join(NDF_CMPL_PATH, file)
		file_df = pd.read_csv(file_path, index_col=0, parse_dates=True)
		fp = file.split(' ')[0]
		target_path = os.path.join(CMPL_TARGET_PATH, f'{ndf_ccp_map[fp]}.csv')
		file_df.to_csv(target_path)



	end = datetime.now()

	print(end - start)


if __name__ == '__main__':
	process_spot_ndf_data()