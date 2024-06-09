# -*- coding: utf-8 -*-
import pyfolio as pf
import pandas as pd


def compute_weight_pv(position, price):
	value_df = position * price
	value_df['pv'] = value_df.sum(axis=1)
	weight_df = value_df.iloc[:, :-1].div(value_df['pv'], axis='index')
	fund_pv = value_df['pv'] / value_df['pv'].iloc[0]

	return weight_df, fund_pv


if __name__ == "__main__":
	position_df = pd.read_csv('../data/bocam_position.csv', index_col=0)
	price_df = pd.read_csv('../data/bocam_price.csv', index_col=0)
	weight_df, pv = compute_weight_pv(position_df, price_df)
	weight_df.to_csv('../data/bocam_weight.csv')
	pv.to_csv('../data/bocam_pv.csv')
