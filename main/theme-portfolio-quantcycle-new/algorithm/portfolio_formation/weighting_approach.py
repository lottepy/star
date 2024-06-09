import pandas as pd
import datetime
import numpy as np
from cvxopt import solvers
import cvxopt

def mean_variance(portfolio_symbol,trading_data,formation_date,windows,cash):
    symbol_list=portfolio_symbol.index
    start=formation_date-datetime.timedelta(windows)
    temp_trading=pd.DataFrame()
    for symbol in symbol_list:
        temp=trading_data[symbol].loc[start:formation_date,['ret_daily']]
        temp.columns = list([symbol])
        temp_trading=pd.concat([temp_trading,temp],axis=1)

    mean_return=np.mean(temp_trading,axis=0)
    mean_return = np.array(mean_return)
    covariance = np.cov(temp_trading, rowvar=0)
    covariance = np.array(covariance)

    P=covariance
    q=np.zeros(portfolio_symbol.shape[0])
    G = np.vstack([np.eye(portfolio_symbol.shape[0]), -np.eye(portfolio_symbol.shape[0]), \
                   -np.eye(portfolio_symbol.shape[0]) * cash])
    lotprice = 100 * portfolio_symbol['CLOSE'].values.reshape(portfolio_symbol.shape[0], 1)
    lotprice = np.array(lotprice, dtype=np.float)
    h = np.vstack([np.ones(portfolio_symbol.shape[0]).reshape(portfolio_symbol.shape[0], 1) * upper_bound,
                   -np.ones(portfolio_symbol.shape[0]).reshape(portfolio_symbol.shape[0], 1) * lower_bound, \
                   -lotprice])
    A = np.vstack([np.ones(portfolio_symbol.shape[0]).reshape(1, portfolio_symbol.shape[0])])
    b = np.vstack([np.ones(1).reshape(1, 1)])

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
    portfolio_symbol['weight_minvar'] = optimized['x']




















