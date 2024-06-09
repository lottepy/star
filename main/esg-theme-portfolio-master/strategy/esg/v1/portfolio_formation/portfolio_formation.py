import numpy as np
import pandas as pd
import os
import datetime
from algorithm import addpath

from os.path import join 


from cvxopt import solvers
import cvxopt
import sys

def portfolio_formation(portfolio_name, number_assets, symbol_list,symbol_industry, rebalance_freq, weighting_approach,
						trading_data, financial_data, reference_data, formation_date, data_start, factors,
						underlying_type, market_cap_threshold, cash,upper_bound, lower_bound, factor_list):
	if underlying_type == "US_STOCK":
		input_path = os.path.join(addpath.data_path, "us_data")
	elif underlying_type == "HK_STOCK":
		input_path = os.path.join(addpath.data_path, "hk_data")
	elif underlying_type == "CN_STOCK":
		input_path = os.path.join(addpath.data_path, "cn_data")
	# if portfolio_name=='CN_Quality_Privilege':
	# 	investment_univ = investment_universe_Privilege(symbol_industry, trading_data, formation_date, underlying_type, \
	# 										  market_cap_threshold, cash, upper_bound)
	# else:
	# 	t_all_s=datetime.datetime.now()
	# 	investment_univ = investment_universe(symbol_industry, trading_data, formation_date, underlying_type, \
	# 									  market_cap_threshold,cash,upper_bound)
    #
	# investment_univ_df=pd.DataFrame(investment_univ,columns=['symbol'])
	# investment_univ_path=os.path.join(input_path, "investment_univ")
	# investment_univ_df.to_csv(os.path.join(investment_univ_path, formation_date.strftime("%Y-%m-%d") + ".csv"))
    #
	# t_all_e = datetime.datetime.now()
	# print('investment_universe calculating time' + f'{t_all_e - t_all_s}')
 
	theme_mapped_from_eikon = pd.read_csv(join(addpath.data_path, 'esg', 'theme_mapped_from_eikon.csv'))
	theme_mapped_from_eikon = theme_mapped_from_eikon[theme_mapped_from_eikon['cusip_mapped_RIC'].notna()]
	
	
	# grading_mapping = 
 
	refinitiv_esg = pd.read_csv(join(addpath.data_path, 'esg', 'refinitiv_esg.csv'))
	refinitiv_esg = refinitiv_esg[refinitiv_esg['ESG Score Grade'].notnull()]
	refinitiv_esg['Date'] = pd.DatetimeIndex(refinitiv_esg['Date']).tz_localize(None)
	refinitiv_esg = refinitiv_esg[refinitiv_esg['Date'] <= formation_date]
	refinitiv_esg = refinitiv_esg.set_index('Date').sort_index(ascending=False)
	refinitiv_esg['Date'] = refinitiv_esg.index
	refinitiv_esg = refinitiv_esg.groupby('Instrument').first() # Only need the latest ESG before formation date
 
	# This part is only used for counting number of tickers having esg
	have_esg_instruments = theme_mapped_from_eikon[theme_mapped_from_eikon['cusip_mapped_RIC'].isin(refinitiv_esg.index)]
	have_esg_instruments = have_esg_instruments['Theme Ticker'].unique().tolist()
	num_have_esg = len(have_esg_instruments)
 
	refinitiv_esg = refinitiv_esg[refinitiv_esg['ESG Combined Score'] >= 50] # Only need first 50 percentile stocks

	pass_esg_instruments = theme_mapped_from_eikon[theme_mapped_from_eikon['cusip_mapped_RIC'].isin(refinitiv_esg.index)]
	pass_esg_instruments = pass_esg_instruments['Theme Ticker'].unique().tolist()

	investment_univ_path = os.path.join(input_path, "investment_univ")
	investment_univ = pd.read_csv(os.path.join(investment_univ_path, formation_date.strftime("%Y-%m-%d") + ".csv"))[
		'symbol'].tolist()

	# rebalancing_dates_bwd = cal_rebalancing_dates_backward(data_start, formation_date, rebalance_freq)

	# last_period_factors_list = []
	# for symbol in investment_univ:
	# 	if factors[symbol].shape[0] > 0:
	# 		tmp = pd.DataFrame(factors[symbol].iloc[-1, :]).transpose()
	# 		last_period_factors_list.append(tmp)
	# last_period_factors = pd.concat(last_period_factors_list)

	investment_univ=list(set(investment_univ)&set(factors.index.tolist())&set(symbol_list)&set(pass_esg_instruments))
	num_universe=len(investment_univ)
 
	print("Number of stocks to be considered in calculation : {}".format(len(investment_univ)))
 
	last_period_factors=factors.loc[investment_univ,:]
	last_period_factors['LOTPRICE']=symbol_industry.loc[investment_univ,'LOTSIZE']*last_period_factors['CLOSE']
	last_period_factors=last_period_factors[last_period_factors['LOTPRICE']<cash*upper_bound]
	investment_univ=list(set(investment_univ)&set(last_period_factors.index.tolist())&set(symbol_list))

	input_path = os.path.join(addpath.strategy_path, "esg", "v1", "config", portfolio_name)
	sys.path.append(input_path)
	from theme_conditions import theme_conditions

	rankings = theme_conditions(last_period_factors,symbol_industry, investment_univ, factor_list)
	num_selected=len(rankings)

	portfolio_path = os.path.join(addpath.data_path, "strategy_temps", portfolio_name, "portfolios")





	# if portfolio_name == "HK_HSTECH":
	# 	rankings['LOTPRICE'] = rankings['CLOSE'] * rankings['LOTSIZE']
	# 	portfolio_symbol1=rankings[rankings['ind_rank']==1]
	# 	rankings1=rankings.drop(index=portfolio_symbol1.index)
	# 	rankings1=rankings1[rankings1['rank'].rank(ascending=True)<=(number_assets-len(portfolio_symbol1))]
	# 	rankings1=rankings1.sort_values(by = 'rank',axis = 0,ascending = True)
	# 	cash_left=cash-sum(portfolio_symbol1['LOTPRICE'])
	# 	rankings1['CUM_LOTPRICE']=rankings1['LOTPRICE'].cumsum()
	#
	# 	portfolio_symbol2 = rankings1[rankings1['CUM_LOTPRICE'] <= cash_left]
	# 	portfolio_symbol=pd.concat([portfolio_symbol1,portfolio_symbol2],axis=0)

	if (underlying_type == "US_STOCK") & (number_assets == 50):
		portfolio_symbol1 = rankings[rankings['ind_rank'] <= int(number_assets / 20+1)]
		# print(len(portfolio_symbol1))
		rankings1 = rankings.drop(index=portfolio_symbol1.index)
		rankings1 = rankings1[rankings1['rank'].rank(ascending=True) <= (number_assets - len(portfolio_symbol1))]
		# print(len(rankings))
		portfolio_symbol=pd.concat([portfolio_symbol1,rankings1],axis=0)

	elif (portfolio_name=="US_5G"):
		portfolio_symbol = rankings[rankings['rank'] <= number_assets]

	else:
		portfolio_symbol=rankings[rankings['rank'] <= number_assets]
		if portfolio_symbol['CLOSE'].sum()>cash:
			portfolio_symbol = rankings[rankings['rank'] <= number_assets*1.3]
			portfolio_symbol['trade_off']=rankings['CLOSE'].rank(ascending=True)*rankings['rank']
			portfolio_symbol['trade_off']=portfolio_symbol['trade_off'].rank(ascending=True)
			portfolio_symbol=portfolio_symbol[portfolio_symbol['trade_off']<=number_assets]
			print('the minimum cash needed:',portfolio_symbol['CLOSE'].sum())

	print('portfolio_symbol.shape[0]')
	print(portfolio_symbol.shape[0])
	print(portfolio_symbol)
	#portfolio_symbol.to_csv(os.path.join(portfolio_path, "factors" + formation_date.strftime("%Y-%m-%d") + ".csv"))
	# portfolio_symbol=portfolio_symbol.set_index('symbol')

	# if portfolio_symbol.shape[0]>number_assets:
	# 	maxrank=portfolio_symbol['rank'].max()
	# 	rank_list=portfolio_symbol[portfolio_symbol['rank']<maxrank].index
	# 	zscore_df=portfolio_symbol[portfolio_symbol['rank']==maxrank]
	# 	zscore_df['zscorerank']=zscore_df['zscore'].rank(ascending=False)
	# 	zscore_df=zscore_df[zscore_df['zscorerank']<=(number_assets-len(rank_list))]
	# 	zscore_list=zscore_df.index
	# 	rank_list=rank_list.append(zscore_list)
	# 	portfolio_symbol=portfolio_symbol.loc[rank_list,:]
	# 	print('rank_list')
	# 	print(len(rank_list))
	#
	# 	portfolio_symbol=rankings.loc[rank_list,:]

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
	elif weighting_approach == 'MIN_VARIANCE':
		windows=365
		start = formation_date - datetime.timedelta(windows)
		temp_trading = pd.DataFrame()
		for symbol in portfolio_symbol.index:
			temp = trading_data[symbol].loc[start:formation_date, ['ret_daily']]
			temp.columns = list([symbol])
			temp_trading = pd.concat([temp_trading, temp], axis=1)

		covariance = shrinkage_est(temp_trading.values)
		covariance = np.array(covariance)

		P = covariance
		q = np.zeros(portfolio_symbol.shape[0])
	elif weighting_approach == 'Z_SCORE':
		portfolio_symbol['weight'] = portfolio_symbol['ini_weight']
		# portfolio_symbol = portfolio_symbol.drop(columns=['rank', 'MARKET_CAP'])
		P = np.eye(portfolio_symbol.shape[0])
		q = -2 * np.array(portfolio_symbol['weight'].tolist()).reshape(portfolio_symbol.shape[0], 1)

	if underlying_type!="CN_STOCK":
		lotprice = symbol_industry.loc[portfolio_symbol.index, 'LOTSIZE'] * portfolio_symbol['CLOSE']
		lotprice = lotprice.values.reshape(portfolio_symbol.shape[0], 1)
		lotprice=np.array(lotprice,dtype=np.float)
	elif underlying_type=="CN_STOCK":
		lotprice = 100 * portfolio_symbol['CLOSE']
		lotprice = lotprice.values.reshape(portfolio_symbol.shape[0], 1)
		lotprice = np.array(lotprice, dtype=np.float)
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
	return portfolio_symbol,num_universe,num_selected,num_have_esg

def shrinkage_est(obs):
    """
    Generate the covariance matrix of the sample using shrinkage technique.

    :param obs: 2-d array
        The time series data of close prices

    :return: 2-d array
        The covariance matrix of the sample data
    """
    m = obs.shape[0]  # number of observations for each r.v.
    n = obs.shape[1]  # number of random variables

    cov = np.cov(obs.T)
    mean = np.mean(obs, axis=0)
    wij_bar = cov.dot(float(m - 1) / m)
    sq_cov = np.power(cov, 2.0)
    np.fill_diagonal(sq_cov, 0.0)
    beta = np.sum(sq_cov)

    mean_removed = obs - np.ones((m, n)).dot(np.diag(mean))

    alpha = 0
    for i in range(n):
        for j in range(i + 1, n):
            for t in range(m):
                alpha += (mean_removed[t - 1, i - 1] * mean_removed[t - 1, j - 1] - wij_bar[i - 1, j - 1]) ** 2
    alpha *= (float(m) / (m - 1) ** 3)
    lam = alpha / (beta / 2)
    cov_shrinkage = cov.dot(1.0 - lam) + np.diag(np.diag(cov)).dot(lam)

    return cov_shrinkage


def compute_realized_cov(data):
    tmp = data.copy()
    log_price = pd.np.log(tmp)
    ret = (log_price - log_price.shift(1)).dropna()
    square_ret = pd.np.multiply(ret, ret)

    tickers = tmp.columns.values
    cov = pd.DataFrame(None, index=tickers, columns=tickers)

    for x in tickers:
        for y in tickers:
            cov.loc[x, y] = pd.np.dot(ret[x], ret[y])

    return cov.values.astype(float)


