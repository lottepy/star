import pandas as pd
import json
import os
from fx_daily.utils.constants import *

file_path = os.path.join(WORKING_PATH, 'weight-v4.xlsx')

weight1 = pd.read_excel(file_path, sheet_name=0, index_col=0, parse_dates=True)
weight2 = pd.read_excel(file_path, sheet_name=1, index_col=0, parse_dates=True)
weight3 = pd.read_excel(file_path, sheet_name=2, index_col=0, parse_dates=True)


def weight_transform(df):
	df['weight'] = df['weight'].apply(lambda x: json.loads(x.replace("'", '"')))
	df['ccy'] = df['weight'].apply(lambda x: list(x.keys()))
	all_ccy = list(set(df['ccy'].sum()))
	for ccy in all_ccy:
		df[f'{ccy}'] = df['weight'].apply(lambda x: x.get(ccy))
	df = df.fillna(0)
	df.drop(['weight', 'ccy'], axis=1, inplace=True)
	return df


weight1 = weight_transform(weight1)
weight2 = weight_transform(weight2)
weight3 = weight_transform(weight3)

ccy1 = weight1.columns.tolist()
ccy2 = weight2.columns.tolist()
ccy3 = weight3.columns.tolist()
all_ccy = list(set(ccy1 + ccy2 + ccy3))


def fill_0(df, all_ccy):
	for ccy in all_ccy:
		if ccy not in df.columns:
			df[ccy] = 0
	df = df[all_ccy]
	return df


weight1 = fill_0(weight1, all_ccy)
weight2 = fill_0(weight2, all_ccy)
weight3 = fill_0(weight3, all_ccy)

weight_all = (1 / 3) * weight1 + (1 / 3) * weight2 + (1 / 3) * weight3

weight1.to_csv(os.path.join(WORKING_PATH, 'weight_df_20191120/weight_mean_reversion.csv'))
weight2.to_csv(os.path.join(WORKING_PATH, 'weight_df_20191120/weight_momentum.csv'))
weight3.to_csv(os.path.join(WORKING_PATH, 'weight_df_20191120/weight_macro.csv'))
weight_all.to_csv(os.path.join(WORKING_PATH, 'weight_df_20191120/weight_equal_weighted.csv'))
