import pandas as pd
import numpy as np
import datetime
import statsmodels.api as sm
import math
from dateutil.relativedelta import relativedelta
import json
from os import makedirs
from os.path import exists, join, normpath
from algorithm.addpath import config_path, data_path
from constant import *
import logging
import configparser
import random


def generate_new_folder(folder):
	if not exists(folder):
		# print(f"Create new folder {folder}")
		makedirs(folder)
	else:
		# print(f"Folder {folder} already existed.")
		pass


def standardized_single_factors_selection(training_date):
	start_time = datetime.datetime.now()
	model_path = join(data_path, 'cs_model', 'single_factor_validation')
	return_path = join(data_path, 'returns')
	factor_path = join(data_path, 'factors', 'cs_factor_std')
	category_path = join(data_path, 'categorization', 'fund_category')
	category_list = ['Equity_US', 'Index_Global', 'Equity_EM', 'Equity_Global', 'Equity_APAC', 'Equity_DMexUS', 'Bond_Global', 'Balance_Global', 'Bond_US', 'Alternative_Gold_Global', 'Alternative_Futures_Global']

	save_path = join(model_path, 'factor_categorization_intermediate_data', training_date)
	generate_new_folder(save_path)
	logging.basicConfig(filename= normpath(save_path + '/regression.log'), level=logging.DEBUG, format='%(asctime)s %(message)s',
						datefmt='%d/%m/%Y %H:%M:%S')

	factors_cluster = pd.read_csv(join(model_path, 'Factor Categorization and Single Factor Significance.csv'), index_col='Cross-sectional Factor Name')
	factors_cluster_list = factors_cluster['Factor Category'].unique().tolist()

	config_file_path = join(config_path, "config.conf")
	config = configparser.ConfigParser()
	config.read(config_file_path)
	hrz = str(config['model_info']['prediction_horizon'])
	training_sample_period = int(config['model_info']['training_sample_period_for_single_factor_selection'])

	end_date = datetime.datetime.strptime(training_date, "%Y-%m-%d") - relativedelta(months=int(hrz))
	start_date = end_date - relativedelta(months=int(training_sample_period) - 1)
	dates = pd.date_range(start_date, end_date, freq='M').tolist()
	ret_tmp = pd.read_csv(join(return_path, '{}M_return.csv'.format(hrz)), parse_dates=[0], index_col='date')

	record_coefficient = dict()
	record_tvalue = dict()
	for cate in category_list:
		record_coefficient[cate] = dict()
		record_tvalue[cate] = dict()
		for cluster in factors_cluster_list:
			record_coefficient[cate][cluster] = dict()
			record_tvalue[cate][cluster] = dict()

	for date in dates:
		ret = pd.DataFrame(ret_tmp.loc[date, :])
		ret = ret.dropna()
		ret.columns = ['fwd_return']

		factors_all = pd.read_csv(join(factor_path, date.strftime("%Y-%m-%d") + '.csv'), index_col=0)
		factors_all = factors_all.dropna(axis=0, how='all')
		factors_all = factors_all.dropna(axis=1, how='all')

		category_df = pd.read_csv(join(category_path, date.strftime("%Y-%m-%d") + '.csv'), index_col=0)

		# Create masterfile
		masterfile = pd.concat([ret, category_df, factors_all], axis=1)
		masterfile = masterfile.dropna(subset=['fwd_return'])

		for cate in category_list:
			masterfile_cate = masterfile[masterfile['category'] == cate]
			for cluster in factors_cluster_list:
				record_coefficient[cate][cluster][date.strftime("%Y-%m-%d")] = dict()
				record_tvalue[cate][cluster][date.strftime("%Y-%m-%d")] = dict()

				factors_list = factors_cluster[factors_cluster['Factor Category'] == cluster].index.tolist()
				'''single factor regression'''
				for factor in factors_list:
					factor += '_zscore'
					sample = masterfile_cate.loc[:, ['fwd_return', factor]]
					sample = sample.replace([-np.inf, np.inf], np.nan)
					sample = sample.dropna()
					if sample.shape[0] == 0:
						logging.info('Regression no data1: {} {} {} {} {}'.format(hrz, cate, date.strftime("%Y-%m-%d"), cluster, factor))
						continue
					last_factorExposure = sm.add_constant(sample[factor])

					'''fit OLS model'''
					regResult = sm.OLS(sample['fwd_return'], last_factorExposure).fit()
					record_coefficient[cate][cluster][date.strftime("%Y-%m-%d")][factor] = regResult.params[factor]
					record_tvalue[cate][cluster][date.strftime("%Y-%m-%d")][factor] = regResult.tvalues[factor]

		print('{} regression finished'.format(date.strftime("%Y-%m-%d")))

	for cate in record_coefficient:
		coefficient_cluster = record_coefficient[cate]
		tvalue_cluster = record_tvalue[cate]
		generate_new_folder(join(save_path, hrz, cate, 'coefficient'))
		generate_new_folder(join(save_path, hrz, cate, 'tvalue'))

		for cluster in coefficient_cluster:
			coefficient_single = pd.DataFrame.from_dict(coefficient_cluster[cluster])
			tvalue_single = pd.DataFrame.from_dict(tvalue_cluster[cluster])

			try:
				coefficient_single['t_value'] = coefficient_single.mean(axis=1) / (
							coefficient_single.std(axis=1) / math.sqrt(coefficient_single.count(axis=1).values[0]))
			except IndexError:
				logging.error('t-stat: no coeff {} {}'.format(cate, cluster))
				coefficient_single['t_value'] = np.nan
			coefficient_single.to_csv(join(save_path, hrz, cate, 'coefficient', 'coef_{}.csv'.format(cluster)))

			tvalue_tmp = tvalue_single[tvalue_single.columns.tolist()]
			try:
				tvalue_single['mean'] = tvalue_tmp.mean(axis=1)
				tvalue_single['median'] = tvalue_tmp.median(axis=1)
				tvalue_single['t_value'] = tvalue_single['mean'] / (
							tvalue_tmp.std(axis=1) / math.sqrt(tvalue_tmp.count(axis=1).values[0]))
			except IndexError:
				logging.error('t-stat of t-stat: no tvalues {} {}'.format(cate, cluster))
				tvalue_single['mean'] = np.nan
				tvalue_single['median'] = np.nan
				tvalue_single['t_value'] = np.nan
			tvalue_single.to_csv(join(save_path, hrz, cate, 'tvalue', 'tvalue_{}.csv'.format(cluster)))

	end_time = datetime.datetime.now()
	logging.info('{} finished in time: {}'.format(training_date, str(end_time - start_time)))


def analyze_result(training_date):
	model_path = join(data_path, 'cs_model', 'single_factor_validation')
	category_list = ['Equity_US', 'Index_Global', 'Equity_EM', 'Equity_Global', 'Equity_APAC', 'Equity_DMexUS', 'Bond_Global', 'Balance_Global', 'Bond_US', 'Alternative_Gold_Global', 'Alternative_Futures_Global']

	config_file_path = join(config_path, "config.conf")
	config = configparser.ConfigParser()
	config.read(config_file_path)
	hrz = str(config['model_info']['prediction_horizon'])

	save_path = join(model_path, 'analysis', training_date)
	generate_new_folder(save_path)
	read_path = join(model_path, 'factor_categorization_intermediate_data', training_date)

	factors_cluster = pd.read_csv(join(model_path, 'Factor Categorization and Single Factor Significance.csv'), index_col='Cross-sectional Factor Name')
	factors_cluster_list = factors_cluster['Factor Category'].unique().tolist()
	output_header = ['Factor Category', 'FM Coef', 'FM t-stat', 'Mean of CS t-stat', 't-stat of CS t-stat',
					 '% of Periods with abs(CS t-stat)>2.5 and sign(CS t-stat)=sign(FM Coef)', 'num_obs']

	writer_dict = dict()
	for cate in category_list:
		selector_list = []
		for cluster in factors_cluster_list:
			tvalue_tmp = pd.read_csv(join(read_path, hrz, cate, 'tvalue', 'tvalue_{}.csv'.format(cluster)), index_col=0)
			coeff_tmp = pd.read_csv(join(read_path, hrz, cate, 'coefficient', 'coef_{}.csv'.format(cluster)), index_col=0)

			FM_coeff = coeff_tmp[coeff_tmp.columns.tolist()[:-1]].mean(axis=1)
			FM_coeff.columns = ['FM Coef']
			FM_tvalue = coeff_tmp[['t_value']]  # rename

			mean_of_cs_tvalue = tvalue_tmp[['mean']]  # rename
			tvalue_of_cs_tvalue = tvalue_tmp[['t_value']]

			sign_tvalue = tvalue_tmp[tvalue_tmp.columns.tolist()[:-3]]
			sign_coeff = pd.DataFrame(coeff_tmp[coeff_tmp.columns.tolist()[:-1]].mean(axis=1).apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0)))
			sign_coeff.columns = ['Sign(FM Coef)']

			sign_tvalue = sign_tvalue.fillna(0)
			sign_tvalue = np.sign(sign_tvalue)

			sign_matrix = sign_tvalue.multiply(sign_coeff['Sign(FM Coef)'], axis='index')
			sign_matrix[sign_matrix != 1] = 0

			magnitude_tvalue = tvalue_tmp[tvalue_tmp.columns.tolist()[:-3]]
			magnitude_tvalue[magnitude_tvalue > 2.5] = 1
			magnitude_tvalue[magnitude_tvalue < -2.5] = 1
			magnitude_tvalue[magnitude_tvalue != 1] = 0

			intersect = pd.DataFrame(sign_matrix.values * magnitude_tvalue.values, columns=sign_matrix.columns,
									 index=sign_matrix.index)

			percentage = intersect.sum(axis=1) / intersect.count(axis=1)  # rename
			num_obs = intersect.count(axis=1)

			cluster_col = pd.DataFrame(cluster, columns=['Factor Category'], index=percentage.index)
			cluster_tmp = pd.concat(
				[cluster_col, FM_coeff, FM_tvalue, mean_of_cs_tvalue, tvalue_of_cs_tvalue, percentage, num_obs], axis=1)
			cluster_tmp.columns = ['Factor Category', 'FM Coef', 'FM t-stat', 'Mean of CS t-stat', 't-stat of CS t-stat',
								   '% of Periods with abs(CS t-stat)>2.5 and sign(CS t-stat)=sign(FM Coef)', 'num_obs']
			cluster_tmp.index.name = 'Cross-sectional Factor Name'

			selector_list.append(cluster_tmp)
			del sign_tvalue, sign_matrix

		writer_dict['{} {}'.format(cate, hrz)] = pd.concat(selector_list, axis=0)

	writer = pd.ExcelWriter(join(save_path, 'Factor Categorization and Single Factor Significance.xlsx'), engine='xlsxwriter')
	for w in writer_dict:
		writer_dict[w].to_excel(writer, sheet_name=w)
	writer.save()


def select_factors(training_date):
	model_path = join(data_path, 'cs_model', 'single_factor_validation')
	category_list = ['Equity_US', 'Index_Global', 'Equity_EM', 'Equity_Global', 'Equity_APAC', 'Equity_DMexUS', 'Bond_Global', 'Balance_Global', 'Bond_US', 'Alternative_Gold_Global', 'Alternative_Futures_Global']

	select_path = join(model_path, 'selected')
	save_path = join(select_path, training_date)
	generate_new_folder(save_path)
	logging.basicConfig(filename=normpath(join(select_path, 'selection.log')), level=logging.DEBUG,
						format='%(asctime)s %(message)s',
						datefmt='%d/%m/%Y %H:%M:%S')
	config_file_path = join(config_path, "config.conf")
	config = configparser.ConfigParser()
	config.read(config_file_path)
	training_sample_period = int(config['model_info']['training_sample_period_for_single_factor_selection'])

	config_file_path = join(config_path, "config.conf")
	config = configparser.ConfigParser()
	config.read(config_file_path)
	hrz = str(config['model_info']['prediction_horizon'])
	min_num_obs = int(config['model_info']['min_num_periods_for_single_factor_selection'])

	percentage_header = '% of Periods with abs(CS t-stat)>2.5 and sign(CS t-stat)=sign(FM Coef)'
	tvalue_header = 'Mean of CS t-stat'
	fm_tvalue_header = 'FM t-stat'

	def calc_max(df, head):
		max_val = df[head].abs().max(axis=0)
		max_df = pd.DataFrame(df.loc[abs(df[head]) == max_val, :])
		max_rest = pd.DataFrame(df.loc[abs(df[head]) != max_val, :])
		return max_val, max_df, max_rest

	def select(datain):
		rows = datain.shape[0]
		if rows == 0:
			print('FAILED {} {} {}'.format(cate, hrz, cluster))
			pass
		elif rows == 1:
			datain.loc[:, 'Select'] = 1
		else:
			percMax1_val, percMax1_df, percMax1_rest = calc_max(datain, percentage_header)
			tvalueMax1_in_percMax1_val, tvalueMax1_in_percMax1_df, tvalueMax1_in_percMax1_rest = \
				calc_max(percMax1_df, tvalue_header)

			perc_threshold = max(0.05 * percMax1_val, 1/training_sample_period)
			filter1 = pd.DataFrame(datain.loc[percMax1_val - datain[percentage_header] <= perc_threshold, :])

			tvalue_threshold = max(tvalueMax1_in_percMax1_val * 2, 1.2)
			percMax1_val, percMax1_df, percMax1_rest = calc_max(filter1, percentage_header)
			filter2 = pd.DataFrame(percMax1_rest.loc[percMax1_rest[tvalue_header].abs() >= tvalue_threshold, :])
			filtered = pd.concat([percMax1_df, filter2], axis=0)

			fil_percMax1_val, fil_percMax1_df, fil_percMax1_rest = calc_max(filtered, percentage_header)
			fil_tvalueMax1_val, fil_tvalueMax1_df, fil_tvalueMax1_rest = calc_max(filtered, tvalue_header)
			fil_tvalueMax1_in_percMax1_val, fil_tvalueMax1_in_percMax1_df, fil_tvalueMax1_in_percMax1_rest = \
				calc_max(fil_percMax1_df, percentage_header)
			if fil_percMax1_df.shape[0] == 1:
				datain.loc[fil_percMax1_df.index.values[0], 'Select'] = 1
			else:
				not_choose_percMax1 = fil_tvalueMax1_val != fil_tvalueMax1_in_percMax1_val
				if not not_choose_percMax1:
					select_tmp = pd.DataFrame(filtered.loc[((filtered[tvalue_header].abs() ==
															fil_tvalueMax1_in_percMax1_val)
														   &(filtered[percentage_header]==fil_percMax1_val)), :])
					if select_tmp.shape[0] == 1:
						datain.loc[select_tmp.index.values[0], 'Select'] = 1
					else:
						FMtvalueMax1_val, FMtvalueMax1_df, FMtvalueMax1_rest = calc_max(select_tmp, fm_tvalue_header)
						if FMtvalueMax1_df.shape[0] == 1:
							datain.loc[FMtvalueMax1_df.index.values[0], 'Select'] = 1
						else:
							rd = random.randint(0, FMtvalueMax1_df.shape[0]-1)
							datain.loc[FMtvalueMax1_df.index.values[rd], 'Select'] = 1
							logging.info('Randomly Select: three cols equal for {}|{}|{}'.format(training_date, cate, cluster))
				else:
					select_tmp = pd.DataFrame(filtered.loc[filtered[tvalue_header].abs() == fil_tvalueMax1_val, :])
					if select_tmp.shape[0] == 1:
						datain.loc[select_tmp.index.values[0], 'Select'] = 1
					else:
						FMtvalueMax1_val, FMtvalueMax1_df, FMtvalueMax1_rest = calc_max(select_tmp, fm_tvalue_header)
						if FMtvalueMax1_df.shape[0] == 1:
							datain.loc[FMtvalueMax1_df.index.values[0], 'Select'] = 1
						else:
							rd = random.randint(0, FMtvalueMax1_df.shape[0]-1)
							datain.loc[FMtvalueMax1_df.index.values[rd], 'Select'] = 1
							logging.info('Randomly Select: three cols equal for {}|{}|{}'.format(training_date, cate, cluster))
		return datain

	for cate in category_list:
		flag = 0
		factors_res = pd.read_excel(join(model_path, 'analysis', training_date, 'Factor Categorization and Single Factor Significance.xlsx'),
									sheet_name='{} {}'.format(cate, hrz), index_col='Cross-sectional Factor Name')
		factors_res['Select'] = None
		selected_list = []
		factors_cluster_list = factors_res['Factor Category'].unique()
		count = 0
		for cluster in factors_cluster_list:
			cluster_tmp = factors_res[factors_res['Factor Category'] == cluster]
			cluster_tmp = cluster_tmp[cluster_tmp['num_obs'] >= min_num_obs]
			count += cluster_tmp.shape[0]
			if cluster == 'Return':
				cluster_tmp_1 = cluster_tmp[cluster_tmp['Mean of CS t-stat'] > 0]
				cluster_tmp_2 = cluster_tmp[cluster_tmp['Mean of CS t-stat'] <= 0]
				if not (cluster_tmp_1.empty or cluster_tmp_2.empty):
					flag += 1
					cluster_tmp_1 = select(cluster_tmp_1)
					cluster_tmp_2 = select(cluster_tmp_2)
					cluster_tmp = pd.concat([cluster_tmp_1, cluster_tmp_2])
				elif cluster_tmp_1.empty:
					cluster_tmp = select(cluster_tmp_2)
					logging.info('NO positive sign: {}|{}|{}'.format(training_date, cate, cluster))
				else:
					cluster_tmp = select(cluster_tmp_1)
					logging.info('NO negative sign: {}|{}|{}'.format(training_date, cate, cluster))
			elif cluster == 'Volatility':
				cluster_tmp_1 = cluster_tmp[cluster_tmp['Mean of CS t-stat'] > 0]
				cluster_tmp_2 = cluster_tmp[cluster_tmp['Mean of CS t-stat'] <= 0]
				cluster_tmp_2 = select(cluster_tmp_2)
				cluster_tmp = pd.concat([cluster_tmp_1, cluster_tmp_2])
				if cluster_tmp_2.empty:
					flag -= 1
					logging.info('NO negative sign: {}|{}|{}'.format(training_date, cate, cluster))
			elif cluster == 'Style Drift':
				cluster_tmp_1 = cluster_tmp[cluster_tmp['Mean of CS t-stat'] > 0]
				cluster_tmp_2 = cluster_tmp[cluster_tmp['Mean of CS t-stat'] <= 0]
				cluster_tmp_2 = select(cluster_tmp_2)
				cluster_tmp = pd.concat([cluster_tmp_1, cluster_tmp_2])
				if cluster_tmp_2.empty:
					flag -= 1
					logging.info('NO negative sign: {}|{}|{}'.format(training_date, cate, cluster))
			elif cluster == 'Portfolio Manager Experience':
				cluster_tmp_1 = cluster_tmp[cluster_tmp['Mean of CS t-stat'] > 0]
				cluster_tmp_1 = select(cluster_tmp_1)
				cluster_tmp_2 = cluster_tmp[cluster_tmp['Mean of CS t-stat'] <= 0]
				cluster_tmp = pd.concat([cluster_tmp_1, cluster_tmp_2])
				if cluster_tmp_1.empty:
					flag -= 1
					logging.info('NO positive sign: {}|{}|{}'.format(training_date, cate, cluster))
			else:
				cluster_tmp = select(cluster_tmp)

			selected_list.append(cluster_tmp)
		selected = pd.concat(selected_list, axis=0)
		output_path = join(save_path, cate + '_' + hrz + '.csv')
		selected.to_csv(output_path)
		# verification
		logging.info("{}|{}\nDimention:{}|SelectNo:{}".format(training_date, cate, selected.shape[0] == count,
															  selected['Select'].sum(axis=0)==len(factors_cluster_list)+flag))


if __name__=="__main__":
	start_year = 2013
	now_date = datetime.datetime.now().date()
	if now_date >= datetime.datetime(datetime.datetime.now().year, 5, 31).date():
		end_year = datetime.datetime.now().year
	else:
		end_year = datetime.datetime.now().year - 1

	dates = [datetime.datetime(i, 5, 31) for i in range(start_year, end_year + 1)]
	for date in dates:
		standardized_single_factors_selection(training_date=date.strftime(format='%Y-%m-%d'))
		analyze_result(training_date=date.strftime(format='%Y-%m-%d'))
		select_factors(training_date=date.strftime(format='%Y-%m-%d'))

	# standardized_single_factors_selection(training_date='2020-04-30')
	# analyze_result(training_date='2020-04-30')
	# select_factors(training_date='2020-04-30')