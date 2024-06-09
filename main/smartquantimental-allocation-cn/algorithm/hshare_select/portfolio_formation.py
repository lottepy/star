import numpy as np
import pandas as pd
import os
from algorithm import addpath
from cvxopt import solvers
import cvxopt
from datetime import datetime
import warnings
warnings.filterwarnings("ignore")

def portfolio_formation(criteria):
	cash=100000000
	upper_bound=0.1
	lower_bound=0.01
	weighting_approach='MARKET_CAP'
	number_assets=40

	formation_dates=pd.read_csv(os.path.join(addpath.config_path,'formation_date_hk.csv'),parse_dates=[0])['formation_date']
	formation_dates=formation_dates[formation_dates<datetime.today()]
	hk_path=os.path.join(addpath.data_path,'Hshare')
	investment_univ_path = os.path.join(hk_path, "investment_universe")
	CS_factors_path = os.path.join(hk_path, "CS_Factors")
	symbol_path = os.path.join(addpath.config_path, "Hshare_tradable_symbol_list.csv")
	symbol_industry=pd.read_csv(symbol_path,index_col=0).dropna()
	num_universe_list=pd.DataFrame(index=formation_dates,columns=['num_universe','num_selected'])

	for formation_date in formation_dates:
		print(formation_date)
		investment_univ = pd.read_csv(os.path.join(investment_univ_path, formation_date.strftime("%Y-%m-%d") + ".csv"))[
		'symbol'].tolist()
		factors = pd.read_csv(os.path.join(CS_factors_path, formation_date.strftime("%Y-%m-%d") + ".csv"),
							  index_col=['symbol'])
		factors=factors.loc[investment_univ,:]
		factors['LOTPRICE']=symbol_industry.loc[investment_univ,'LOTSIZE']*factors['PX_LAST_RAW']
		factors=factors[factors['LOTPRICE']<cash*upper_bound]
		investment_univ=list(set(investment_univ)&set(factors.index.tolist()))
		num_universe=len(investment_univ)

		# criteria = {
		# 	'MARKET_CAP': ['Positive',0.2],
		# 	'REVOAGROWTH': ['Positive',0.2],
		# 	'GPOE': ['Positive', 0.2],
		# 	'ROA': ['Positive', 0.2],
		# 	'ACCT_RCV_TO': ['Positive', 0.2],
		# 	'EBITDEVAR': ['Negative', 0.2],
		# 	'LEV': ['Negative', 0.2]
		# }
		selection_tmp = factors.copy()[list(criteria.keys())]
		selection_tmp = selection_tmp.loc[investment_univ, :]
		selection_tmp['num_na'] = selection_tmp.isnull().sum(axis=1)
		print(selection_tmp)

		selection_tmp = selection_tmp[selection_tmp['num_na'] <= 3]
		selection_tmp = selection_tmp.drop(['num_na'], axis=1)
		for col in selection_tmp.columns:
			selection_tmp[col] = selection_tmp[col].clip(lower=selection_tmp[col].quantile(0.025),
														 upper=selection_tmp[col].quantile(0.975))
			selection_tmp[col] = selection_tmp[col].fillna(selection_tmp[col].mean())

		symbol_industry = symbol_industry.loc[investment_univ, :]
		selection_tmp['industry'] = symbol_industry['SEHKENG']
		selection_tmp['HSTECH_LIST'] =symbol_industry['HSTECH_LIST']
		ind_list = list(set(symbol_industry['SEHKENG'].tolist()))
		ratio_ind={
			'Information Technology':0.35,
			'Industrials':0.2,
			'Healthcare':0.2,
			'Consumer Discretionary':0.2,
			'Financials':0.05
		}

		selection_list = []
		for ind in ind_list:
			temp = selection_tmp[selection_tmp['industry'] == ind]
			temp['ind_zscore'] = 0
			for col in list(criteria.keys()):
				if criteria[col][0] == 'Positive':
					temp[col + '_rank'] = temp[col].rank(ascending=False)
				else:
					temp[col + '_rank'] = temp[col].rank(ascending=True)
				# temp=temp[temp[col + '_rank']<0.5*len(temp)]
				factor_mean_tmp = temp[col + '_rank'].mean()
				factor_std_tmp = temp[col + '_rank'].std()
				temp[col + '_zscore'] = (temp[col + '_rank'] - factor_mean_tmp) / factor_std_tmp
				temp['ind_zscore'] = temp['ind_zscore'] + temp[col + '_zscore'] * criteria[col][1]

			temp['ind_rank'] = temp['ind_zscore'].rank(ascending=True)
			temp = temp[(temp['ind_rank'] <= number_assets*ratio_ind[ind])|(temp['HSTECH_LIST'] ==1)]
			print(ind)
			print(temp)
			selection_list.append(temp)
		selection_tmp = pd.concat(selection_list)

		selection_tmp['zscore'] = 0
		for col in list(criteria.keys()):

			if criteria[col][0] == 'Positive':
				selection_tmp['col_rank'] = selection_tmp[col].rank(ascending=False)
			else:
				selection_tmp['col_rank'] = selection_tmp[col].rank(ascending=True)
			factor_mean_tmp = selection_tmp['col_rank'].mean()
			factor_std_tmp = selection_tmp['col_rank'].std()
			selection_tmp['zscore'] = selection_tmp['zscore'] + (
						selection_tmp['col_rank'] - factor_mean_tmp) / factor_std_tmp * criteria[col][1]

		selection_tmp['rank'] = selection_tmp['zscore'].rank(ascending=True)

		rankings = selection_tmp
		rankings['MARKET_CAP'] = factors.loc[rankings.index, 'MARKET_CAP']
		rankings['CLOSE'] = factors.loc[rankings.index, 'PX_LAST_RAW']
		rankings['ADJ_CLOSE'] = factors.loc[rankings.index, 'PX_LAST']


		num_selected=len(rankings)

		portfolio_path = os.path.join(hk_path, "portfolio")
		if os.path.exists(portfolio_path):
			pass
		else:
			os.makedirs(portfolio_path)

		portfolio_symbol = rankings[rankings['rank'] <= number_assets]
		print(portfolio_symbol)

		if weighting_approach == "EQUALLY":
			portfolio_symbol['weight'] = 1 / portfolio_symbol.shape[0]
			#portfolio_symbol = portfolio_symbol.drop(columns=['rank', 'MARKET_CAP'])
			P = np.eye(portfolio_symbol.shape[0])
			q = -2 * np.array(portfolio_symbol['weight'].tolist()).reshape(portfolio_symbol.shape[0], 1)
		elif weighting_approach == "MARKET_CAP":
			portfolio_symbol['weight'] = portfolio_symbol['MARKET_CAP'] / sum(portfolio_symbol['MARKET_CAP'])
			#portfolio_symbol = portfolio_symbol.drop(columns=['rank', 'MARKET_CAP'])
			P = np.eye(portfolio_symbol.shape[0])
			q = -2 * np.array(portfolio_symbol['weight'].tolist()).reshape(portfolio_symbol.shape[0], 1)
		elif weighting_approach == 'Z_SCORE':
			portfolio_symbol['ini_weight'] = portfolio_symbol['zscore'] + 1
			temp = portfolio_symbol['zscore'][portfolio_symbol['zscore'] < 0]
			portfolio_symbol['ini_weight'][portfolio_symbol['zscore'] < 0] = 1 / (1 - temp)
			portfolio_symbol['ini_weight'] = portfolio_symbol['ini_weight'] / portfolio_symbol['ini_weight'].sum()

			portfolio_symbol['weight'] = portfolio_symbol['ini_weight']
			# portfolio_symbol = portfolio_symbol.drop(columns=['rank', 'MARKET_CAP'])
			P = np.eye(portfolio_symbol.shape[0])
			q = -2 * np.array(portfolio_symbol['weight'].tolist()).reshape(portfolio_symbol.shape[0], 1)

		lotprice = symbol_industry.loc[portfolio_symbol.index, 'LOTSIZE'] * portfolio_symbol['CLOSE']
		lotprice = lotprice.values.reshape(portfolio_symbol.shape[0], 1)
		lotprice=np.array(lotprice,dtype=np.float)

		G = np.vstack([np.eye(portfolio_symbol.shape[0]), -np.eye(portfolio_symbol.shape[0]), \
					   -np.eye(portfolio_symbol.shape[0]) * cash])
		h = np.vstack([np.ones(portfolio_symbol.shape[0]).reshape(portfolio_symbol.shape[0], 1) * upper_bound,
					   -np.ones(portfolio_symbol.shape[0]).reshape(portfolio_symbol.shape[0], 1) * lower_bound, \
					   -lotprice])
		if sum(cash*lower_bound-lotprice[lotprice<cash*lower_bound])>cash-sum(lotprice)[0]:
			G = np.vstack([np.eye(portfolio_symbol.shape[0]), \
						   -np.eye(portfolio_symbol.shape[0]) * cash])
			h = np.vstack([np.ones(portfolio_symbol.shape[0]).reshape(portfolio_symbol.shape[0], 1) * upper_bound,
						   -lotprice])

		A=np.vstack([np.ones(portfolio_symbol.shape[0]).reshape(1,portfolio_symbol.shape[0])])
		b=np.vstack([np.ones(1).reshape(1,1)])

		solvers.options['show_progress'] = False
		solvers.options['abstol'] = 1e-21
		optimized = solvers.qp(
			cvxopt.matrix(P),
			cvxopt.matrix(q),
			cvxopt.matrix(G),
			cvxopt.matrix(h),
			cvxopt.matrix(A),
			cvxopt.matrix(b)
		)

		portfolio_symbol['weight'] = optimized['x']
		num_universe_list.loc[formation_date, 'num_universe'] = num_universe
		num_universe_list.loc[formation_date, 'num_selected'] = num_selected
		print(portfolio_symbol)
		portfolio_symbol.to_csv(os.path.join(portfolio_path, formation_date.strftime("%Y-%m-%d") + ".csv"))
	num_universe_list.to_csv(os.path.join(hk_path, "num_universe.csv"))

if __name__ == "__main__":
	portfolio_formation(criteria)
