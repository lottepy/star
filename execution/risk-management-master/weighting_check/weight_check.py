# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

REGION_LIST = ["NON", "AUS", "DEU", "HKG", "JPN", "GBR", "CHN"]
SECTOR_LIST = ["NON", "TEC", "CNS", "UTL", "HEA"]
# RATIO_LIST = range(0, 110, 10)
RATIO_LIST = [str(r) for r in range(0, 110, 10)]
PREFERENCE_MAP = {
	'AUS': 'EWA',
	'DEU': 'EWG',
	'HKG': 'EWH',
	'JPN': 'EWJ',
	'CHN': 'ASHR',
	'GBR': 'EWU',
	'TEC': 'QQQ',
	'CNS': 'XLP',
	'UTL': 'XLU',
	'HEA': 'XLV'
}


def region_check(weight, map_):
	dist = weight.iloc[:, :-4].dot(map_)
	agg_data = pd.concat([dist, weight.iloc[:, -3:]], axis=1)

	us_non_mono = np.array([])
	dm_non_mono = np.array([])
	em_non_mono = np.array([])
	for r in REGION_LIST:
		for s in SECTOR_LIST:
			tmp = agg_data[agg_data['sector'] == s][agg_data['region'] == r]
			tmp.iloc[:, :3] = tmp.iloc[:, :3].diff()
			us_non_mono = np.append(us_non_mono, tmp[tmp['US'] <= 0.].index.values)
			dm_non_mono = np.append(dm_non_mono, tmp[tmp['DM'] <= 0.].index.values)
			em_non_mono = np.append(em_non_mono, tmp[tmp['EM'] <= 0.].index.values)
	non_mono_dict = {
		'US': us_non_mono,
		'DM': dm_non_mono,
		'EM': em_non_mono
	}
	# print non_mono_dict
	if len(non_mono_dict.keys()) == 0:
		result = '地区分布全部单调，通过测试'
	else:
		result = non_mono_dict
		error_plot(agg_data)
	print_result('地区分布', result)


def preference_check(weight):
	error_list = []
	for r in RATIO_LIST:
		no_prefer = weight[weight['ratio'] == r][(weight['region'] == 'NON') | (weight['sector'] == 'NON')]
		for reg in REGION_LIST:
			for s in SECTOR_LIST:
				tmp = weight[weight['ratio'] == r][(weight['region'] == reg) & (weight['sector'] == s)]
				if reg == 'NON' and s == 'NON':
					continue
				elif reg == 'NON' and s != 'NON':
					if tmp[PREFERENCE_MAP[s]].values[0] < no_prefer[PREFERENCE_MAP[s]].values[0]:
						error_list.append('_'.join([s, reg, r]))
				elif reg != 'NON' and s == 'NON':
					if tmp[PREFERENCE_MAP[reg]].values[0] < no_prefer[PREFERENCE_MAP[reg]].values[0]:
						error_list.append('_'.join([s, reg, r]))
				else:
					if tmp[PREFERENCE_MAP[s]].values[0] < no_prefer[PREFERENCE_MAP[s]].values[0] \
							or tmp[PREFERENCE_MAP[reg]].values[0] < no_prefer[PREFERENCE_MAP[reg]].values[0]:
						error_list.append('_'.join([s, reg, r]))

	if len(error_list) == 0:
		result = '偏好选择全部有效，通过测试'
	else:
		result = error_list
	print_result('偏好测试', result)


def error_plot(weight):
	error_df = weight[(weight['region'] == 'NON') & (weight['sector'] == 'NON')]
	error_df.plot()
	plt.show()


def print_result(case, result):
	print '\n--- 开始测试：{} ---\n'.format(case)
	print result
	print '\n--- 测试结束：{} ---\n'.format(case)


if __name__ == '__main__':
	target_weight = pd.read_csv('../data/weight_sg.csv', index_col=0).T
	underlying_map = pd.read_csv('../data/mapping_sg.csv', index_col=0)

	target_weight['risk type'] = target_weight.index
	risk_type_df = target_weight['risk type'].str.split('_', expand=True)
	risk_type_df.columns = ['sector', 'region', 'ratio']
	target_weight = pd.concat([
		target_weight,
		risk_type_df
	], axis=1)

	region_check(target_weight, underlying_map)
	# preference_check(target_weight)


