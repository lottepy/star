import numpy as np
import pandas as pd
import os
from datetime import datetime, timedelta
from scipy.stats import skew
import calendar
import statsmodels.api as sm
from algorithm import addpath

def coskewness(insample):
	temp_coskewness = insample.loc[:, ['ret_daily', 'market_index']]
	temp_coskewness = temp_coskewness.dropna()
	temp_coskewness = temp_coskewness[~temp_coskewness.isin([np.nan, np.inf, -np.inf]).any(1)]
	temp_coskewness['m_sq'] = temp_coskewness['market_index'].map(lambda x: x ** 2)
	if temp_coskewness.shape[0] > 15:
		X = sm.add_constant(temp_coskewness.loc[:, ['market_index', 'm_sq']])
		Y = temp_coskewness['ret_daily']
		model = sm.OLS(endog=Y, exog=X, missing='drop').fit()
		coskew = model.params.iloc[2]

		return coskew


def idiosyn(insample):
    temp_idiosyn = insample.loc[:, ['ret_daily', 'market_index']]
    temp_idiosyn = temp_idiosyn.dropna()
    temp_idiosyn = temp_idiosyn[~temp_idiosyn.isin([np.nan, np.inf, -np.inf]).any(1)]
    if temp_idiosyn.shape[0] > 15:
        X = sm.add_constant(temp_idiosyn.loc[:, 'market_index'])
        Y = temp_idiosyn['ret_daily']
        Y_mean=temp_idiosyn['ret_daily'].mean()
        lows=Y[Y<Y_mean]
        semivar=np.sum((lows-Y_mean)**2)/len(lows)
        model = sm.OLS(endog=Y, exog=X, missing='drop').fit()
        beta = model.params.iloc[1]
        residual = model.resid
        ivol = np.std(residual)
        skewness = skew(residual)
        output = pd.DataFrame([ivol, skewness, beta,semivar]).transpose()
        output.columns = ['ivol', 'skew', 'beta','semivar']
        return output



def cal_factors_caixin(symbol, trading_data, financial_data, reference_data, rebalance_freq):
    # Mkt factor
    mkt_dt = reference_data["market_index"].copy()
    mkt_dt["market_index"] = mkt_dt["market_index"].pct_change()
    mkt_dt = mkt_dt.fillna(0)

    temp = trading_data[symbol].copy()
    if temp.shape[0] > 0:
        temp['symbol'] = symbol
        temp['Date'] = temp.index
        temp['YYYY'] = temp['Date'].map(lambda x: x.year)
        temp['MM'] = temp['Date'].map(lambda x: x.month)
        temp = temp.fillna(method='ffill')
        temp['ret_daily'] = temp['PX_LAST'].pct_change()
        temp['TURN'] = temp['PX_VOLUME'] / temp['EQY_SH_OUT']

        # Liquidity Measures
        # Turnovers
        result = temp.loc[:, ['symbol', 'YYYY', 'MM']].resample('1m').last()
        result['TO_1M'] = temp['TURN'].resample('1m').mean()

        # Monthly Price, Marketcap, and Volume
        ADJCLOSE = temp['PX_LAST'].fillna(method='ffill')
        result['EQY_SH_OUT'] = temp['EQY_SH_OUT'].resample('1m').last()
        result['EQY_SH_OUT'] = result['EQY_SH_OUT'].ffill()
        result['VOLUME'] = temp['PX_VOLUME'].resample('1m').sum()
        result['VOLUME'] = result['VOLUME'].ffill()
        result['CLOSE'] = temp['PX_LAST_RAW'].resample('1m').last()
        result['CLOSE'] = result['CLOSE'].ffill()
        result['ADJCLOSE'] = ADJCLOSE.resample('1m').last()
        result['ADJCLOSE'] = result['ADJCLOSE'].ffill()
        result['MARKET_CAP'] = result['CLOSE'] * result['EQY_SH_OUT']

        # Volatility and Skewness Factors
        # Realized Volatility
        daily_std = temp['ret_daily'].rolling(window=23).std(ddof=0)
        # result['RealizedVol_1M'] = daily_std.resample('1m').last().ffill()
        # daily_std = temp['ret_daily'].rolling(window=67).std(ddof=0)
        # result['RealizedVol_3M'] = daily_std.resample('1m').last().ffill()
        # daily_std = temp['ret_daily'].rolling(window=125).std(ddof=0)
        # result['RealizedVol_6M'] = daily_std.resample('1m').last().ffill()
        # daily_std = temp['ret_daily'].rolling(window=252).std(ddof=0)
        result['RealizedVol_12M'] = daily_std.resample('1m').last().ffill()
        # Idiosyncratic Volatility and Skewness
        idio_raw = pd.merge(temp, mkt_dt.loc[:, ['market_index']], left_index=True,
                            right_index=True)
        idio_dt = idio_raw.groupby(['YYYY', 'MM']).apply(idiosyn)
        if idio_dt.shape[0] > 0:
            idio_dt = idio_dt.reset_index()
            idio_dt = idio_dt.loc[:, ['YYYY', 'MM', 'ivol', 'skew', 'beta','semivar']]
            idio_dt['YYYYMM'] = idio_dt['YYYY'] * 100 + idio_dt['MM']
            idio_dt['Date'] = idio_dt['YYYYMM'].map(lambda x: datetime(int(x / 100), x - int(x / 100) * 100, 1))
            idio_dt = idio_dt.set_index('Date')
            idio_dt = idio_dt.resample('1m').last()
            idio_dt = idio_dt.loc[:, ['ivol', 'skew', 'beta','semivar']]
            result = pd.merge(left=result, right=idio_dt, left_index=True, right_index=True, how='outer')
        else:
            result['ivol'] = np.nan
            result['skew'] = np.nan
            result['beta'] = np.nan
            result['semivar'] = np.nan


        # Coskewness
        # csk_raw = pd.merge(temp, mkt_dt.loc[:, ['market_index']], left_index=True, right_index=True)
        # coskew_dt = csk_raw.groupby(['YYYY', 'MM']).apply(coskewness)
        # if coskew_dt.shape[0] > 0:
        #     coskew_dt = pd.DataFrame(coskew_dt, columns=['coskew'])
        #     coskew_dt = coskew_dt.reset_index()
        #     coskew_dt['YYYYMM'] = coskew_dt['YYYY'] * 100 + coskew_dt['MM']
        #     coskew_dt['Date'] = coskew_dt['YYYYMM'].map(lambda x: datetime(int(x / 100), x - int(x / 100) * 100, 1))
        #     coskew_dt = coskew_dt.set_index('Date')
        #     coskew_dt = coskew_dt.resample('1m').last()
        #     coskew_dt = coskew_dt.loc[:, ['coskew']]
        #     result = pd.merge(left=result, right=coskew_dt, left_index=True, right_index=True, how='outer')
        # else:
        #     result['coskew'] = np.nan

        temp2 = pd.DataFrame(index=pd.date_range(temp.index[0], temp.index[-1], freq='D'))
        temp2['EQY_SH_OUT'] = temp['EQY_SH_OUT']
        temp2['PX_LAST_RAW'] = temp['PX_LAST_RAW']
        temp2 = temp2.ffill()
    else:
        result = pd.DataFrame()



    temp = financial_data[symbol].copy()

    if temp.shape[0] > 0:
        temp['symbol'] = symbol
        temp['ANNOUNCEMENT_DT'] = temp['ANNOUNCEMENT_DT'].map(lambda x: np.nan if pd.isna(x) else datetime.strptime(x, "%Y-%m-%d"))
        if temp[['ANNOUNCEMENT_DT']].dropna().shape[0] > 0:
            temp['effective_date'] = np.where(pd.isna(temp['ANNOUNCEMENT_DT']), temp.index + timedelta(days=90),
                                              temp['ANNOUNCEMENT_DT'])
        else:
            temp['effective_date'] = temp.index + timedelta(days=90)
        temp.index.name = 'date'
        temp = temp.reset_index()
        temp = temp.set_index('effective_date')
        if result.shape[0] > 0:
            temp['EQY_SH_OUT'] = temp2['EQY_SH_OUT']
            temp['PX_LAST_RAW'] = temp2['PX_LAST_RAW']
        else:
            temp['EQY_SH_OUT'] = np.nan
            temp['PX_LAST_RAW'] = np.nan
        temp = temp.reset_index()
        temp = temp.set_index('date')
        temp = temp.resample('1M').last().ffill()

        # Profitability
        temp['ROE'] = temp['NET_INCOME'] *2 / (temp['TOT_EQUITY'] +temp['BS_TOT_ASSET'].shift())
        temp['ROA'] = temp['NET_INCOME'] *2 / (temp['BS_TOT_ASSET']+temp['BS_TOT_ASSET'].shift())
        temp['GPRatio']=temp['GP_SALEREV_Ratio'] * temp['SALES_REV_TURN'] *2 \
                        / (temp['BS_TOT_ASSET']+temp['BS_TOT_ASSET'].shift())
        temp['EBTRatio']=temp['EBT']*2/ (temp['TOT_EQUITY'] +temp['BS_TOT_ASSET'].shift())
        temp['DEBT'] = temp['SHORTTERM_BORROW']+temp['LONGTERM_BORROW']
        temp['WoringQuality']=temp['OPPROFIT']*2/(temp['DEBT']+temp['TOT_EQUITY']+\
                                                  temp['DEBT'].shift()+temp['TOT_EQUITY'].shift())
        temp['EarningGrowth_exInv']=(temp['SALES_REV_TURN']-temp['INVENTORY'])/(\
                    temp['SALES_REV_TURN'].shift()-temp['INVENTORY'].shift()) - 1
        temp['CashClass']=temp['CASH']+temp['DEPOSIT_RESERVATION']

        temp['ShortTermFinAsset']=temp['FUND_LENDED']+temp['FUND_PROVIDED']+temp['TRANS_FIN_ASSETS']+\
            temp['FAIR_VALUE_PROFITS_EQUITY']+temp['DERIV_FIN_ASSETS']+temp['BUY_SOLD_FIN_ASSETS']+\
            temp['AMORTIZED_COST_FIN_EQUITY']+temp['FAIR_VALUE_OTHERPROFITS_FIN_EQUITY']+temp['CONTRACT_EQUITY']+\
            temp['ONSALE_EQUITY']+temp['NONCURRENT_ASSETS_1Y']
        temp['LongTermFinAsset'] =temp['ISSUE_LOANSNADVANCES']+temp['CREDITOR_INVEST']+temp['CREDITOR_INVEST_OTHER']+\
            temp['AMORTIZED_COST_NONCURRENT_FIN_EQUITY']+temp['FAIR_VALUE_OTHERPROFITS_NONCURRENCT_FIN_EQUITY']+\
            temp['ONSALE_FIN_ASSETS']+temp['HOLD_TO_MATURITY_INVEST']+temp['LONGTERM_EQUITY_INVEST']+\
            temp['REAL_ESTATE_INVEST']+temp['EQUITY_INVEST_OTHER']+temp['NONCURRENT_EQUITY_OTHER']
        temp['NET_OPERATEASSET']=temp['BS_TOT_ASSET']-temp['BS_TOT_LIAB2']-temp['CashClass']-temp['ShortTermFinAsset']- \
                             temp['LongTermFinAsset']+temp['DEBT']

        temp['ReturnofOperAsset']=temp['SALES_REV_TURN']*2/(temp['NET_OPERATEASSET']+temp['NET_OPERATEASSET'].shift())
        temp['RevenueofOperAsset']=temp['NET_INCOME'] *2/(temp['NET_OPERATEASSET']+temp['NET_OPERATEASSET'].shift())

        temp = temp.set_index('effective_date')
        temp = temp.resample('1M').last().ffill()

        temp = temp.drop(columns=['symbol', 'EQY_SH_OUT'])

        if result.shape[0] > 0:
            result = pd.merge(left=result, right=temp, left_index=True, right_index=True, how='outer')
        else:
            result = temp

    if result.shape[0] > 0:
        result = result.ffill()

    return result