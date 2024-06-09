from os import makedirs, listdir
from os.path import exists, join
import pandas as pd
import numpy as np
import cvxopt
from algorithm import addpath
from datetime import datetime
from dateutil.relativedelta import relativedelta
from cvxopt import solvers

# 协方差矩阵计算
def assets_meanvar(daily_returns):
    expreturns = np.array([])
    (rows, cols) = daily_returns.shape
    for r in range(rows):
        expreturns = np.append(expreturns, np.mean(daily_returns[r]))
    covars = shrinkage_est(daily_returns.T)
    return expreturns, covars
def shrinkage_est(obs):
    m = obs.shape[0]  # number of observations for each r.v.
    n = obs.shape[1]  # number of random variables

    cov = np.cov(obs.T)
    Mean = np.mean(obs, axis=0)
    wij_bar = cov.dot(float(m - 1) / m)
    sq_cov = np.power(cov, 2.0)
    np.fill_diagonal(sq_cov, 0.0)
    beta = np.sum(sq_cov)
    mean_removed = obs - np.ones((m, n)).dot(np.diag(Mean))

    alpha = 0
    for i in range(n):
        for j in range(i + 1, n):
            for t in range(m):
                alpha += (mean_removed[t - 1, i - 1] * mean_removed[t - 1, j - 1] - wij_bar[i - 1, j - 1]) ** 2
    alpha *= (float(m) / (m - 1) ** 3)
    lam = alpha / (beta / 2)
    cov_shrinkage = cov.dot(1.0 - lam) + np.diag(np.diag(cov)).dot(lam)
    return cov_shrinkage

# 债券 Risk Parity 计算
def compute_weights_of_bond_funds(bond_fund_price, method):
    if method=='risk_parity':
        # calulate bond funds returns
        fund_weekly_prices = bond_fund_price.asfreq('W-MON', method='pad')
        fund_weekly_prices = fund_weekly_prices.fillna(method='ffill')
        fund_weekly_returns = fund_weekly_prices.pct_change().dropna()

        std = fund_weekly_returns.std() * np.sqrt(52)
        inverse_std = std.apply(lambda x: 1./x)
        weights_of_bond_funds = inverse_std / inverse_std.sum()
        weights_of_bond_funds_df = pd.DataFrame(weights_of_bond_funds, columns=['Weight'])
        weights_of_bond_funds_df = _very_important_function(weights_of_bond_funds_df)
    elif method=='equal_weight':
        weights_of_bond_funds_df = pd.DataFrame(index=bond_fund_price.columns)
        equal_weight = 1./len(bond_fund_price.columns)
        weights_of_bond_funds_df.loc[:,'Weight'] = equal_weight

    return weights_of_bond_funds_df
def _very_important_function(weights_of_bond_funds_df:pd.DataFrame)->pd.DataFrame:
    number_of_funds = weights_of_bond_funds_df.shape[0]
    min_we_want = (1./number_of_funds) * 0.318
    max_we_want = (1./number_of_funds) * 1.406
    current_weights = weights_of_bond_funds_df.Weight.values
    adjusted_weights = current_weights[:]
    while adjusted_weights.max()>max_we_want:
        w = adjusted_weights.max()
        n = adjusted_weights.argmax()
        adjusted_weights[n] = max_we_want
        equal_split = (w-max_we_want) / (number_of_funds-1.)
        for k in range(len(adjusted_weights)):
            if k!=n:
                adjusted_weights[k] += equal_split
    new_weights_of_bond_funds_df = weights_of_bond_funds_df.copy()
    new_weights_of_bond_funds_df.Weight = adjusted_weights

    return new_weights_of_bond_funds_df


def optimal_weighting(weights_before_cap_df, funds_price_df_tmp, index_volume_df, date, lower_bound):
    cap_list = ['大市值股票', '中小市值股票']
    lamda = 2.5 # risk aversion coeff

    market_cap_df = index_volume_df[np.logical_and(index_volume_df.index >= date - relativedelta(months=12),
                                                         index_volume_df.index < date)]
    market_cap_df = market_cap_df.ffill().last('1d').transpose()
    # 将中小市值股票市值调整到1.5倍
    market_cap_df.loc['中小市值股票'] = market_cap_df.loc['中小市值股票'] * 2.0

    # 所有基金
    funds_list = weights_before_cap_df['Funds'].tolist()
    weights_of_cap_df = weights_before_cap_df[weights_before_cap_df.index.isin(cap_list)]
    # 大市值，中小市值基金
    funds_cap_list = weights_of_cap_df['Funds'].tolist()

    # 股票型基金所处位置=1，其他类型基金所处位置=0
    stock_array = []
    for cate in weights_before_cap_df.index.tolist():
        if cate not in ['利率债', '信用债', '货币']:
            stock_array.append(1.0)
        else:
            stock_array.append(0.0)

    # 大市值基金和中小市值基金初始权重，同时拼接到已有权重的基金
    cap_wts = market_cap_df.loc[cap_list] / market_cap_df.loc[cap_list].sum()
    if len(funds_cap_list) > 2:
        for fund in weights_before_cap_df.loc[['大市值股票'], 'Funds'].values:
            cap_wts.loc[fund] = cap_wts.loc['大市值股票'][0] / len(weights_before_cap_df.loc['大市值股票', 'Funds'])
        for fund in weights_before_cap_df.loc[['中小市值股票'], 'Funds'].values:
            cap_wts.loc[fund] = cap_wts.loc['中小市值股票'][0] / len(weights_before_cap_df.loc['中小市值股票', 'Funds'])
        cap_wts.drop(['大市值股票', '中小市值股票'], inplace=True)
    else:
        cap_wts.rename(index={'大市值股票':weights_before_cap_df.loc['大市值股票','Funds'],
                              '中小市值股票':weights_before_cap_df.loc['中小市值股票','Funds']}, inplace=True)
    all_cap_wts = pd.DataFrame(index=weights_before_cap_df['Funds'].tolist())
    all_cap_wts['all_cap_wts'] = cap_wts * (1 - weights_before_cap_df['Weight'].sum())
    for idx in all_cap_wts.index.tolist():
        if idx in funds_cap_list:
            continue
        try:
            weights_tmp = weights_before_cap_df.set_index('Funds')
            all_cap_wts.loc[idx, 'all_cap_wts'] = weights_tmp.loc[idx, 'Weight']
        except:
            all_cap_wts.loc[idx, 'all_cap_wts'] = 0.0
    all_cap_wts['lower'] = all_cap_wts['all_cap_wts'] - 0.05
    all_cap_wts['upper'] = all_cap_wts['all_cap_wts'] + 0.05

    for index, row in all_cap_wts.iterrows():
        if all_cap_wts.loc[index, 'lower'] < lower_bound:
            all_cap_wts.loc[index, 'lower'] = lower_bound
        if all_cap_wts.loc[index, 'upper'] < lower_bound:
            all_cap_wts.loc[index, 'upper'] = lower_bound + 0.05

    # the price should be cleaned before plugging in
    funds_price_df_tmp = funds_price_df_tmp[funds_list]

    funds_weekly_prices = funds_price_df_tmp.asfreq('W-MON', method='pad')
    funds_weekly_prices = funds_weekly_prices.fillna(method='ffill')
    funds_weekly_returns = funds_weekly_prices.pct_change().dropna().values.T

    index_expreturns, index_covars = assets_meanvar(funds_weekly_returns)
    cov = pd.DataFrame(index_covars, index=funds_price_df_tmp.columns, columns=funds_price_df_tmp.columns)
    cov_equity = cov.loc[funds_cap_list, funds_cap_list]
    cap_wts = np.array(all_cap_wts.loc[funds_cap_list, 'all_cap_wts'].values.T)

    # BL Model Exp Ret
    Pi = lamda * np.dot(cov_equity, cap_wts)
    Pi = pd.Series(Pi, index=funds_cap_list)
    Pi_all = pd.DataFrame(None, index=funds_list)
    Pi_all['Pi_all'] = Pi
    Pi_all['Pi_all'] = Pi_all['Pi_all'].fillna(0)
    Pi_all = Pi_all['Pi_all'].values

    n = len(Pi_all)
    P = np.dot(lamda, cov)
    q = Pi_all * (-1)
    G = np.vstack([-np.eye(n), np.eye(n)]).astype(float)

    h = np.hstack([-all_cap_wts['lower'], all_cap_wts['upper']]).astype(float).reshape(all_cap_wts.shape[0] * 2, 1)

    A_raw = []
    b_raw = []
    fixed_weight_list = [i for i in funds_list if i not in funds_cap_list]
    for bd in fixed_weight_list:
        raw_constrain = np.zeros(n)
        raw_constrain[funds_list.index(bd)] = 1
        A_raw.append(raw_constrain)
        b_raw.append([all_cap_wts.loc[bd, 'all_cap_wts']])

    A_raw.append(np.array(stock_array))
    b_raw.append([equity_pct])
    A_new = np.array([A_raw[i] for i in range(0, len(A_raw), 1)])
    b_new = np.array([b_raw[i] for i in range(0, len(b_raw), 1)])


    solvers.options['show_progress'] = False
    solvers.options['abstol'] = 1e-21
    optimized = solvers.qp(
        cvxopt.matrix(P),
        cvxopt.matrix(q),
        cvxopt.matrix(G),
        cvxopt.matrix(h),
        cvxopt.matrix(A_new),
        cvxopt.matrix(b_new)
    )
    return optimized['x'], all_cap_wts


def compute_weights(start_date, end_date, equity_pct, sector_preference, invest_length, lower_bound, price_df):
    bl_weights_path = join(addpath.data_path, 'bl_weights', 'JD')
    pool_path = join(addpath.data_path, 'pool', 'JD')
    foropt_path = join(addpath.data_path, 'foropt')
    # categorization_path = join(data_path, 'categorization')
    # score_path = join(data_path, 'predictions', 'expected_returns')

    # index_volume_df = pd.read_excel(join(foropt_path, 'index_market_cap.xlsx'), parse_dates=[0], index_col=0)
    index_volume_df = pd.read_csv(join(foropt_path, 'index_mktcap.csv'), parse_dates=[0], index_col=0)
    # index_price_df = pd.read_excel(join(foropt_path, 'index_price.xlsx'),parse_dates=[0], index_col=0)

    info = str(int(equity_pct * 100)) + '-' + sector_preference + '-' + invest_length
    savepath = join(bl_weights_path, info)
    if exists(savepath):
        pass
    else:
        makedirs(savepath)

    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    rebalance_dates_df = pd.read_csv(join(addpath.config_path, 'JD', 'rebalance_dates.csv'), parse_dates=[0])
    dates_list = rebalance_dates_df[np.logical_and(rebalance_dates_df['rebalance_dates'] >= start_dt,
                                                   rebalance_dates_df['rebalance_dates'] <= end_dt)]['rebalance_dates'].tolist()

    fundsfile_list = listdir(pool_path)

    # 删去.DS_Store文件
    try:
        fundsfile_list.remove('.DS_Store')
    except ValueError:
        print('No .DS_Store file')

    fundsbook_list = []
    for file in fundsfile_list:
        book = pd.read_csv(join(pool_path, file))
        book = book.rename(columns = {'ms_secid': 'Funds'})
        fundsbook_list.append(book)

    invest_length_df = pd.read_csv(join(addpath.config_path, 'JD', 'invest_length.csv'), index_col=0)
    # basic_invest_length = invest_length_df[invest_length_df['delta_equity_pct'] == 0].index[0]
    delta_equity_pct = invest_length_df.loc[invest_length, 'delta_equity_pct']
    equity_pct_adj = equity_pct + delta_equity_pct

    cap_list = ['大市值股票', '中小市值股票']
    style_list = ['成长', '价值']
    sector_list = ['消费', '医疗', '科技', '工业', '金融']
    money_list = ['货币']
    bond_list = ['利率债', '信用债']
    alternative_list = ['另类资产']

    weight_overall_df_list = []
    for date in dates_list:
        filename = date.strftime('%Y-%m-%d') + '.csv'
        index_p = fundsfile_list.index(filename)
        funds_df_tmp = fundsbook_list[index_p]
        funds_df_tmp = funds_df_tmp.dropna()
        # funds_list_tmp = funds_df_tmp['Funds'].tolist()

        index_volume_df_tmp = index_volume_df[np.logical_and(index_volume_df.index >= date - relativedelta(months=12),
                                                             index_volume_df.index < date)]
        index_volume_df_tmp = index_volume_df_tmp.ffill().last('1d').transpose()

        sector_pct = 0.275 * equity_pct_adj
        style_pct = 0.125 * equity_pct_adj


        sector_list_tmp = [sec for sec in sector_list if sec in funds_df_tmp['class'].tolist()]
        weights_of_sector_df_tmp = index_volume_df_tmp.copy().loc[sector_list_tmp, :]
        weights_of_sector_df_tmp.columns = ['Weight']
        weights_of_sector_df_tmp['Weight'] = weights_of_sector_df_tmp['Weight']/weights_of_sector_df_tmp['Weight'].sum() * sector_pct

        weights_of_sector_df_tmp['Funds'] = funds_df_tmp.set_index('class').copy().loc[weights_of_sector_df_tmp.index, 'Funds']

        number_of_funds = weights_of_sector_df_tmp.shape[0]
        current_weights = weights_of_sector_df_tmp.copy().Weight.values
        adjusted_weights = current_weights[:]
        count = 0
        adjust_list = [0.001, 0.003, 0.0005, 0.002]
        while adjusted_weights.min() < lower_bound:
            count = count % 4
            w = adjusted_weights.min()
            n = adjusted_weights.argmin()
            adjusted_weights[n] = lower_bound + adjust_list[count]
            equal_split = (lower_bound + adjust_list[count] - w) / (number_of_funds - 1.)
            for k in range(len(adjusted_weights)):
                if k != n:
                    adjusted_weights[k] -= equal_split
            count += count
        # weights_of_sector_df_tmp.Weight = adjusted_weights
        # weights_of_sector_df_tmp['Weight'] = weights_of_sector_df_tmp['Weight'] /weights_of_sector_df_tmp['Weight'].sum() * sector_pct
        #
        # # number_of_funds = weights_of_sector_df_tmp.shape[0]
        # current_weights = weights_of_sector_df_tmp.copy().Weight.values
        # adjusted_weights = current_weights[:]
        # count = 0
        # adjust_list = [0.001, 0.003, 0.0005, 0.002]
        # while adjusted_weights.min() < lower_bound:
        #     count = count%4
        #     w = adjusted_weights.min()
        #     n = adjusted_weights.argmin()
        #     adjusted_weights[n] = lower_bound + adjust_list[count]
        #     equal_split = (lower_bound + adjust_list[count] - w) / (number_of_funds - 1.)
        #     for k in range(len(adjusted_weights)):
        #         if k != n:
        #             adjusted_weights[k] -= equal_split
        #     count += count
        weights_of_sector_df = weights_of_sector_df_tmp.copy()
        weights_of_sector_df.Weight = adjusted_weights
        weights_of_sector_df['Weight'] = weights_of_sector_df['Weight'] / weights_of_sector_df['Weight'].sum() * sector_pct



        if equity_pct >= 0.5:
            style_list_tmp = [sec for sec in style_list if sec in funds_df_tmp['class'].tolist()]
            weights_of_style_df_tmp = index_volume_df_tmp.copy().loc[style_list_tmp, :]
            weights_of_style_df_tmp.columns = ['Weight']
            weights_of_style_df_tmp['Weight'] = weights_of_style_df_tmp['Weight'] / weights_of_style_df_tmp[
                'Weight'].sum() * style_pct

            weights_of_style_df_tmp['Funds'] = funds_df_tmp.set_index('class').copy().loc[weights_of_style_df_tmp.index, 'Funds']

            weights_of_style_df = weights_of_style_df_tmp.copy()
            weights_of_style_df['Weight'] = weights_of_style_df['Weight'] / weights_of_style_df['Weight'].sum() * style_pct



        weights_of_money_df = funds_df_tmp.set_index('class').copy().loc[money_list, ['Funds']]
        money_weight =  (0.0831 - 0.0412) / (0.1 - 0.8) * (equity_pct_adj - 0.8) + 0.0212
        weights_of_money_df['Weight'] = money_weight

        weights_of_alternative_df = funds_df_tmp.set_index('class').copy().loc[alternative_list, ['Funds']]
        alternative_weight = (0.0831 - 0.0412) / (0.1 - 0.8) * (equity_pct_adj - 0.8) + 0.0382
        weights_of_alternative_df['Weight'] = alternative_weight


        funds_price_df_tmp = price_df[np.logical_and(price_df.index>=date - relativedelta(months = 12),
                                                    price_df.index<date)][funds_df_tmp['Funds'].tolist()]
        # clean the data if necessary
        funds_price_df_tmp = funds_price_df_tmp.fillna(method='ffill').fillna(method='bfill')
        # There might be duplicated indices since more than 1 funds match an index
        funds_price_df_tmp = funds_price_df_tmp.T.drop_duplicates().T
        for col in funds_price_df_tmp.columns:
            funds_price_df_tmp[col] = funds_price_df_tmp[col].replace(to_replace=0, method='ffill').replace(
                to_replace=0, method='bfill')

        # load funds data
        bond_list_tmp = [bond for bond in bond_list if bond in funds_df_tmp['class'].tolist()]
        funds_bonds_list = funds_df_tmp.set_index('class').copy().loc[bond_list_tmp, 'Funds'].dropna().tolist()
        fund_bond_price_df_tmp = funds_price_df_tmp[funds_bonds_list]
        weights_of_bond_funds_df_tmp = compute_weights_of_bond_funds(bond_fund_price=fund_bond_price_df_tmp,
                                                              method='risk_parity')

        weights_of_bond_funds_df_tmp['Weight'] = weights_of_bond_funds_df_tmp['Weight'] * (1 - equity_pct_adj - money_weight)
        weights_of_bond_funds_df_tmp['class'] = funds_df_tmp.set_index('Funds').copy().loc[weights_of_bond_funds_df_tmp.index, 'class']
        weights_of_bond_funds_df_tmp.index.name = 'Funds'
        weights_of_bond_funds_df_tmp = weights_of_bond_funds_df_tmp.reset_index().set_index('class')


        weights_of_bond_funds_df = weights_of_bond_funds_df_tmp
        weights_cap_df_tmp = funds_df_tmp.set_index('class').loc[cap_list, ['Funds']]
        if equity_pct >= 0.5:
            weights_before_cap_df = pd.concat([weights_of_sector_df, weights_of_style_df, weights_of_money_df, weights_of_bond_funds_df,
                                               weights_of_alternative_df, weights_cap_df_tmp], axis =0)
        else:
            weights_before_cap_df = pd.concat([weights_of_sector_df, weights_of_money_df, weights_of_bond_funds_df,
                                               weights_of_alternative_df, weights_cap_df_tmp], axis=0)
        raw_weights, all_cap_wts = optimal_weighting(weights_before_cap_df, funds_price_df_tmp, index_volume_df, date, lower_bound)
        normalized_weights = raw_weights / np.sum(np.fabs(raw_weights))
        opt_weight = np.array(normalized_weights.T).flatten()

        all_cap_wts[date.strftime('%Y-%m-%d')] = opt_weight

        # weights_df_tmp = optimal_weighting(weights_before_cap_df, funds_price_df_tmp, index_volume_df, date)
        weights_df_tmp = all_cap_wts[[date.strftime('%Y-%m-%d')]]
        weights_df_tmp.to_csv(join(savepath, date.strftime('%Y-%m-%d') + '.csv'))
        print('Succeed on ', date.date(), ' with ', len(weights_df_tmp), ' Funds.')
        weight_overall_df_list.append(weights_df_tmp)

    weight_overall_df = pd.concat(weight_overall_df_list, axis = 1)

    dates_list2 = weight_overall_df.columns

    portfolio_feature_df_list = []
    for date in dates_list2:
        weight_df_tmp = weight_overall_df.loc[:, [date]].dropna()
        weight_df_tmp.columns = ['Weight']
        weight_df_tmp.index.name = 'Funds'
        # weight_df_tmp.to_csv(join(savepath, date.strftime('%Y-%m-%d') + '.csv'))
        # print('Succeed on ', date.date(), ' with ', len(weight_df_tmp), ' Funds.')
        # beta_tmp = (weight_df_tmp['Weight'] * beta_df.loc[date, weight_df_tmp.index]).sum()
        portfolio_feature_df_tmp = pd.DataFrame()
        portfolio_feature_df_tmp['sector_preference'] = [sector_preference]
        portfolio_feature_df_tmp['invest_length'] = [invest_length]
        portfolio_feature_df_tmp['funds_number'] = [len(weight_df_tmp)]
        portfolio_feature_df_tmp['lowest_weight'] = [weight_df_tmp['Weight'].min()]
        portfolio_feature_df_tmp['highest_weight'] = [weight_df_tmp['Weight'].max()]
        # portfolio_feature_df_tmp['real_beta'] = [beta_tmp]

        if date != dates_list2[0]:
            turnover_df = pd.merge(weight_his_forturnover_df, weight_df_tmp, left_index=True, right_index=True,
                                   how='outer').fillna(0)
            turnover = abs(turnover_df['Weight_x'] - turnover_df['Weight_y']).sum() / 2
            portfolio_feature_df_tmp['turnover'] = [turnover]
            # print('turnover is ', turnover)
        portfolio_feature_df_tmp.index = [date]
        # print(portfolio_feature_df_tmp)
        portfolio_feature_df_list.append(portfolio_feature_df_tmp)

        weight_his_forturnover_df = weight_df_tmp
    portfolio_feature_df = pd.concat(portfolio_feature_df_list, axis=0)

    # print(weight_overall_df)

    return weight_overall_df, portfolio_feature_df


if __name__ == '__main__':
    foropt_path = join(addpath.data_path, 'foropt')
    price_df = pd.read_csv(join(foropt_path, 'price.csv'), parse_dates=[0], index_col=0)
    for equity_pct in [0.6, 0.7, 0.8]:
        compute_weights(start_date = '2015-11-30', end_date = '2020-12-15', equity_pct = equity_pct, sector_preference = 'A1',
                        invest_length = 'B3', lower_bound = 0.02, price_df = price_df)