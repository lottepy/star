from lib.commonalgo.database.algo_marketapi import post_localcsv, backtesting_update, post_dis, post_proj, MarketAlgoAPI
import pandas as pd
import time
from datetime import datetime
from algorithm import addpath
import os

if __name__ == "__main__":
	version = 'yl'
	result_backtesting_version_folder = os.path.join(addpath.result_path, 'todatabase Final version/')
	file_log = os.path.join(result_backtesting_version_folder, 'post2database.csv')
	projection_path = result_backtesting_version_folder + 'projection/' 
	algo_type = 223
	risk_ratio_list = [0.07, 0.13, 0.17, 0.23, 0.27, 0.33, 0.45, 0.55, 0.65, 0.75]
	industry_list = ['A1', 'A2', 'A3', 'A4', 'A5']
	# industry_list = ['A5']
	region_list = ['B1', 'B2', 'B3', 'B4', 'B5']
	# region_list = ['B4', 'B5']
	test_endpoint = 'https://test-market.aqumon.com/v2/algo/'
	production_endpoint = 'https://market.aqumon.com/v2/algo/'

	initial_date = datetime(2020, 1, 8)
	end_point = test_endpoint
	# end_point = production_endpoint

	if end_point == production_endpoint:
		print('*' * 40)
		print('即将推生产数据库！！！')
		print('*' * 40)
		time.sleep(10)
		print('*' * 40)
		print('即将推生产数据库！！！')
		print('*' * 40)
		time.sleep(5)

	# 策略升級不用這function, 這function是更新最新的backtest data, 策略升級不用這function
	# backtesting_update(file_log, endpoint=end_point)

	post_localcsv(file_log, endpoint=end_point)
	post_dis(filename=file_log, algo_type=algo_type, endpoint=end_point)

	for risk_ratio in risk_ratio_list:
		for industry in industry_list:
			for region in region_list: 
				print('pushing {}-{}-{} ...'.format(risk_ratio, industry, region))
				name = '{}_{}_{}'.format(str(risk_ratio), industry, region)
				file_name = '{}{}.csv'.format(projection_path, name)
				post_proj(file_name=file_name, name=name, initial_date=initial_date, algo_type=algo_type, endpoint=end_point)
