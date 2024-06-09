import pandas as pd
from algorithm import addpath
import os

def portfolio_combined(portfolio_name_list, rotated_etf, market, percent, ETF_mode):
    # Calculate Portfolio Value
    performance_list = []
    if market == 'HK':
        performance_list.append(
            pd.read_csv(os.path.join(addpath.result_path, "smart_beta", portfolio_name_list[0] + '_12_13', 'portfolio value.csv'),
                        index_col=[0], parse_dates=[0]))
        performance_list.append(
            pd.read_csv(os.path.join(addpath.result_path, "smart_beta", portfolio_name_list[0] + '_13_16', 'portfolio value.csv'),
        
                        index_col=[0], parse_dates=[0]))
        performance_list.append(
            pd.read_csv(os.path.join(addpath.result_path, "smart_beta", portfolio_name_list[0] + '_16_20', 'portfolio value.csv'),
                        index_col=[0], parse_dates=[0]))
        performance_list[1] = performance_list[1] * performance_list[0].iloc[-1]
        performance_list[2] = performance_list[2] * performance_list[1].iloc[-1]
        
        performance_list = [pd.concat([performance_list[0].iloc[:-1],performance_list[1].iloc[:-1],performance_list[2]])]
    else:
        for portfolio in portfolio_name_list:
            performance_list.append(
                pd.read_csv(os.path.join(addpath.result_path, "smart_beta", portfolio, 'portfolio value.csv'),
                            index_col=[0], parse_dates=[0]))
            
    

    tmp = pd.read_csv(os.path.join(addpath.result_path, "fer_1", rotated_etf, ETF_mode, 'portfolio value.csv'),
                    index_col=[0], parse_dates=[0])
    
    tmp.columns = [rotated_etf]
    performance_list.append(tmp)

    performance_df = pd.concat(performance_list, axis=1).ffill()
    
    fx_rate = pd.read_csv(os.path.join(addpath.strategy_path, 'smartbeta', 'fxrate.csv'), index_col=[0], parse_dates=[0])
    fx_rate = fx_rate.merge(performance_df.iloc[:,0], how='right', left_index=True, right_index=True).iloc[:,:-1].ffill()
    
    if market != 'US':
        if market == 'CN':
            performance_df[portfolio_name_list] = performance_df[portfolio_name_list].mul(fx_rate['CNY'], axis=0)
        else:
            performance_df[portfolio_name_list] = performance_df[portfolio_name_list].mul(fx_rate['HKD'], axis=0)
    
    performance_pct_change_df = performance_df.pct_change()

    # Calculate mktcap
    mktcap_list = []
    for portfolio in portfolio_name_list:
        portfolio_file_list = os.listdir(
            os.path.join(addpath.data_path, "smart_beta", "strategy_temps", portfolio, "portfolios"))[:-1]
        tmp_mktcap_list = []
        for portfolio_file in portfolio_file_list:
            tmp = pd.read_csv(os.path.join(addpath.data_path, "smart_beta", "strategy_temps", portfolio, "portfolios",
                                           portfolio_file), usecols=['symbol', 'MARKET_CAP'], index_col=[0])
            tmp_mktcap_list.append(tmp.sum().iloc[0])

        tmp_mktcap_ser = pd.Series(tmp_mktcap_list, index=[ele[:-4] for ele in portfolio_file_list])
        tmp_mktcap_ser.index = pd.to_datetime(tmp_mktcap_ser.index)
        tmp_mktcap_ser.name = portfolio
        mktcap_list.append(tmp_mktcap_ser)

    mktcap_df = pd.concat(mktcap_list, axis=1)

    # Read SP1 MKT cap
    tmp = pd.read_csv(os.path.join(addpath.strategy_path, 'smartbeta', 'mktcap.csv'), index_col=[0], parse_dates=[0])[rotated_etf]

    # Match date to 2012-12-31 to 2020-09-30
    performance_df = performance_df.loc[tmp.index[0]:tmp.index[-1]]
    performance_pct_change_df = performance_pct_change_df.loc[tmp.index[0]:tmp.index[-1]]
    mktcap_df = mktcap_df.loc[tmp.index[0]:tmp.index[-1]]

    # Match SP1 mktcap with other mkt cap
    tmp = mktcap_df.merge(tmp.to_frame(), how='outer', left_index=True, right_index=True).iloc[:, -1].ffill()
    sp_mktcap = tmp.loc[mktcap_df.index]
    sp_mktcap = sp_mktcap * 1000000
    mktcap_df = pd.concat([mktcap_df, sp_mktcap], axis=1)

    # Calculate weighting
    weighting_df = mktcap_df[portfolio_name_list].div(mktcap_df[portfolio_name_list].sum(axis=1), axis=0) * percent
    weighting_df[rotated_etf] = (1 - percent)

    weighting_df = weighting_df.merge(performance_df, how='right', left_index=True, right_index=True).iloc[:,
                   :weighting_df.shape[1]].ffill()
    weighting_df.columns = [ele[:-2] for ele in weighting_df.columns]

    portfolio_return = ((weighting_df * performance_pct_change_df).sum(axis=1) + 1).cumprod()

    strategy_result_path = os.path.join(addpath.result_path, 'smart_beta', market+'_combined')
    if os.path.exists(strategy_result_path):
        pass
    else:
        os.makedirs(strategy_result_path)

    portfolio_return.to_csv(os.path.join(strategy_result_path, 'portfolio value.csv'))
    weighting_df.to_csv(os.path.join(strategy_result_path, 'weighting.csv'))

if __name__ == "__main__":
    portfolio_name_list = ['US_Safety', 'US_Profit', 'US_Tech']
    # portfolio_combined(portfolio_name_list, "SP1", "US", percent = 23.0/26.0, ETF_mode="All ETF")
    portfolio_combined(portfolio_name_list, "SP1", "US", percent = 1, ETF_mode="All ETF")

    portfolio_name_list = ['CN']
    # portfolio_combined(portfolio_name_list, "ACC1", "CN", percent = 8.0/11.0, ETF_mode="All ETF")
    portfolio_combined(portfolio_name_list, "ACC1", "CN", percent = 1, ETF_mode="All ETF")

    portfolio_name_list = ['HK_HSTECH']
    portfolio_combined(portfolio_name_list, "HI1", "HK", percent = 1, ETF_mode="All ETF")
