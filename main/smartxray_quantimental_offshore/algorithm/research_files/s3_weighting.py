from os import makedirs, listdir
from os.path import exists, join
import pandas as pd
import numpy as np
from algorithm.addpath import config_path, data_path, result_path
from datetime import datetime
from dateutil.relativedelta import relativedelta

# def assets_meanvar(daily_returns):
#     expreturns = np.array([])
#     (rows, cols) = daily_returns.shape
#     for r in range(rows):
#         expreturns = np.append(expreturns, np.mean(daily_returns[r]))
#     covars = shrinkage_est(daily_returns.T)
#     return expreturns, covars
#
#
# def shrinkage_est(obs):
#     m = obs.shape[0]  # number of observations for each r.v.
#     n = obs.shape[1]  # number of random variables
#
#     cov = np.cov(obs.T)
#     Mean = np.mean(obs, axis=0)
#     wij_bar = cov.dot(float(m - 1) / m)
#     sq_cov = np.power(cov, 2.0)
#     np.fill_diagonal(sq_cov, 0.0)
#     beta = np.sum(sq_cov)
#     mean_removed = obs - np.ones((m, n)).dot(np.diag(Mean))
#
#     alpha = 0
#     for i in range(n):
#         for j in range(i + 1, n):
#             for t in range(m):
#                 alpha += (mean_removed[t - 1, i - 1] * mean_removed[t - 1, j - 1] - wij_bar[i - 1, j - 1]) ** 2
#     alpha *= (float(m) / (m - 1) ** 3)
#     lam = alpha / (beta / 2)
#     cov_shrinkage = cov.dot(1.0 - lam) + np.diag(np.diag(cov)).dot(lam)
#     return cov_shrinkage


def compute_weights_of_bond_funds(bond_fund_price,
                                  method):
    if method=='risk_parity':
        # calulate bond funds returns
        fund_weekly_prices = bond_fund_price.asfreq('W-MON', method='pad')
        fund_weekly_prices = fund_weekly_prices.fillna(method='ffill')
        fund_weekly_returns = fund_weekly_prices.pct_change().dropna()

        std = fund_weekly_returns.std() * np.sqrt(52)
        inverse_std = std.apply(lambda x: 1./x)
        weights_of_bond_funds = inverse_std / inverse_std.sum()
        weights_of_bond_funds_df = pd.DataFrame(weights_of_bond_funds, columns=['Weight'])
        upper_bound = 1./weights_of_bond_funds_df.shape[0] * 1.406
        weights_of_bond_funds_df = _very_important_function(weights_of_bond_funds_df,
                                                            type = 'adjust_max', bound = upper_bound)
    elif method=='equal_weight':
        weights_of_bond_funds_df = pd.DataFrame(index=bond_fund_price.columns)
        equal_weight = 1./len(bond_fund_price.columns)
        weights_of_bond_funds_df.loc[:,'Weight'] = equal_weight

    return weights_of_bond_funds_df


def _very_important_function(weights_of_funds_df, type, bound):
    number_of_funds = weights_of_funds_df.shape[0]
    # min_we_want = (1./number_of_funds) * 0.318
    # max_we_want = (1./number_of_funds) * 1.406
    current_weights = weights_of_funds_df.Weight.values
    adjusted_weights = current_weights[:]
    count = 0
    adjust_list = [0.001, 0.003, 0.0005, 0.002]

    if type == 'adjust_min':
        min_we_want = bound
        while adjusted_weights.min() < min_we_want:
            count = count % 4
            w = adjusted_weights.min()
            n = adjusted_weights.argmin()
            adjusted_weights[n] = min_we_want + adjust_list[count]
            equal_split = (min_we_want + adjust_list[count] - w) / (number_of_funds-1.)
            for k in range(len(adjusted_weights)):
                if k!=n:
                    adjusted_weights[k] -= equal_split
            count += count
    elif type == 'adjust_max':
        max_we_want = bound
        while adjusted_weights.max() > max_we_want:
            count = count % 4
            w = adjusted_weights.max()
            n = adjusted_weights.argmax()
            adjusted_weights[n] = max_we_want - adjust_list[count]
            equal_split = (w - max_we_want + adjust_list[count]) / (number_of_funds-1.)
            for k in range(len(adjusted_weights)):
                if k!=n:
                    adjusted_weights[k] += equal_split
            count += count
    else:
        print('Unidentifiable type!')
    new_weights_of_funds_df = weights_of_funds_df.copy()
    new_weights_of_funds_df.Weight = adjusted_weights

    return new_weights_of_funds_df

# def compute_cap_funds_weights(weights_before_cap_df, funds_price_df_tmp):
#     cap_list = ['大市值股票', '中小市值股票']
#     funds_list = weights_before_cap_df['Funds'].tolist()
#     weights_of_cap_df = weights_before_cap_df[weights_before_cap_df.index.isin(cap_list)]
#     funds_cap_list = weights_of_cap_df['Funds'].tolist()
#     # the price should be cleaned before plugging in
#     funds_price_df_tmp = funds_price_df_tmp[funds_list]
#
#     funds_weekly_prices = funds_price_df_tmp.asfreq('W-MON', method='pad')
#     funds_weekly_prices = funds_weekly_prices.fillna(method='ffill')
#     funds_weekly_returns = funds_weekly_prices.pct_change().dropna().values.T
#
#     index_expreturns, index_covars = assets_meanvar(funds_weekly_returns)
#
#     weights_before_cap_df.index.name = 'class'
#     weights_before_cap_df = weights_before_cap_df.reset_index().set_index('Funds')
#     w1 = weights_before_cap_df.dropna()[['Weight']].values
#     variable_index = [funds_list.index(fund) for fund in funds_list if weights_before_cap_df.loc[fund, 'class'] in cap_list]
#
#     n = len(variable_index)
#     cov_22 = index_covars[variable_index, :][:,variable_index]
#     cov_21 = np.delete(index_covars[variable_index, :], variable_index, axis = 1)
#     ret = index_expreturns[variable_index]
#     P = cov_22
#     q = 2 * cov_21.dot(w1) - 0.1 * ret.reshape((n,1))
#     weight_left  = 1-w1.sum()
#     large_cap_index = [funds_cap_list.index(fund) for fund in funds_cap_list if weights_before_cap_df.loc[fund, 'class'] == '大市值股票']
#     small_cap_index = [funds_cap_list.index(fund) for fund in funds_cap_list if weights_before_cap_df.loc[fund, 'class'] == '中小市值股票']
#     # id_v = np.ones((n, 1))
#     lower_bound = np.ones((n, 1))
#     upper_bound = np.ones((n, 1))
#     lower_bound[large_cap_index, :] = 3/5 * weight_left
#     lower_bound[small_cap_index, :] = 2/7 * weight_left
#     upper_bound[large_cap_index, :] = 5/7 * weight_left
#     upper_bound[small_cap_index, :] = 2/5 * weight_left
#
#     G = np.vstack([-np.ones((1,n)), - np.ones((1,n)), -np.eye(n),  np.eye(n)]).astype(float)
#     h = np.vstack([-weight_left, weight_left, -lower_bound, upper_bound]).astype(float)
#
#     cvxopt.solvers.options['show_progress'] = False
#     cvxopt.solvers.options['abstol'] = 1e-21
#
#     optimized = cvxopt.solvers.qp(cvxopt.matrix(P), cvxopt.matrix(q),
#                                   cvxopt.matrix(G), cvxopt.matrix(h),)
#                                   # cvxopt.matrix(A), cvxopt.matrix(b))
#     x = optimized['x']/sum(optimized['x']) * weight_left
#     weights_after_cap_df = weights_before_cap_df.copy()
#     weights_after_cap_df.loc[funds_cap_list, 'Weight'] = x
#     weights_after_cap_df = weights_after_cap_df.reset_index().set_index('class')
#     return weights_after_cap_df

#
# start_date = '2016-02-29'
# end_date = '2020-04-30'
start_date = '2020-07-31'
end_date = '2020-07-31'
sector_preference = 'A2'
region_preference = 'B2'
equity_pct = 0.45
lower_bound = 0.03

def compute_weights(start_date, end_date, equity_pct, sector_preference, region_preference, lower_bound, price_df):
    # 金融 'A2'
    # 健康 'A3'
    # 消费 'A4'
    # 科技 'A5'
    sector_preference_dict = {'A1': 'No preference',
                              'A2': 'Finance',
                              'A3': 'Healthcare',
                              'A4': 'Consumption',
                              'A5': 'Technology'
    }

    sector_clas = sector_preference_dict[sector_preference]
    # 均衡 'B1'
    # 美国 'B2'
    # 欧洲 'B3'
    # 亚太 'B4'
    # 新兴市场  'B5'

    region_preference_dict = {'B1': 'No preference',
                              'B2': 'US',
                              'B3': 'European DM',
                              'B4': 'APAC',
                              'B5': 'Emerging Market'
    }

    region_clas = region_preference_dict[region_preference]

    weights_path = join(data_path, 'weights')
    pool_path = join(data_path, 'pool')
    foropt_path = join(data_path, 'foropt')

    info = str(int(equity_pct * 100)) + '-' + sector_preference + '-' + region_preference
    savepath = join(weights_path, '6M-12M', info)
    if exists(savepath):
        pass
    else:
        makedirs(savepath)

    region_index_mc_df = pd.read_csv(join(foropt_path, 'index_market_cap.csv'),parse_dates=[0], index_col=0)


    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    rebalance_dates_df = pd.read_csv(join(config_path, 'rebalance_dates.csv'), parse_dates=[0])
    dates_list = rebalance_dates_df[np.logical_and(rebalance_dates_df['rebalance_dates'] >= start_dt,
                                                   rebalance_dates_df['rebalance_dates'] <= end_dt)][
                                    'rebalance_dates'].tolist()
    fundsfile_path = join(pool_path, 'hrz_6')
    fundsfile_list = listdir(fundsfile_path)
    # 删去.DS_Store文件
    try:
        fundsfile_list.remove('.DS_Store')
    except ValueError:
        print('No .DS_Store file')

    fundsbook_list = []
    for file in fundsfile_list:
        book = pd.read_csv(join(fundsfile_path, file))
        book = book.rename(columns = {'ms_secid': 'Funds'})
        fundsbook_list.append(book)

    # base_list = ['Global', 'US', 'European DM', 'APAC', 'Emerging Market']
    if equity_pct < 0.15:
        region_list = ['US']
    elif equity_pct >= 0.15 and equity_pct < 0.25:
        region_list = ['US', 'European DM']
    elif equity_pct >= 0.25:
        region_list = ['US', 'European DM', 'APAC', 'Emerging Market']

    if equity_pct < 0.25:
        bond_list = ['Global IG', 'US IG', 'Other IG']
    elif equity_pct >=0.25 and equity_pct < 0.65:
        bond_list = ['Global IG', 'US IG', 'Other IG', 'High Yield']
    elif equity_pct >= 0.65 and equity_pct < 0.8:
        bond_list = ['Global IG', 'US IG','High Yield']
    elif equity_pct >= 0.8 and equity_pct < 0.9:
        bond_list = ['Global IG','High Yield']
    elif equity_pct >= 0.9:
        bond_list = ['Global IG']


    # base_list = ['Global'] + list(region_preference_dict.values())
    # sector_list = ['Finance', 'Healthcare', 'Consumption', 'Technology']
    # money_list = ['Money Market']
    # bond_list = ['Global IG', 'US IG', 'Other IG', 'High Yield']

    preference_adj_pct = max(equity_pct * 0.05, lower_bound)
    if sector_preference == 'A1' and region_preference == 'B1':
        base_weight = equity_pct
    elif sector_preference != 'A1' and region_preference != 'B1':
        base_weight = equity_pct - 2 * preference_adj_pct
    else:
        base_weight = equity_pct - preference_adj_pct

    if equity_pct >= 0.15:
        # global_weight = base_weight * 0.2
        global_weight = max(base_weight * 0.2, lower_bound)
    elif equity_pct < 0.15:
        global_weight = max(base_weight / 2, lower_bound)

    if equity_pct >= 0.25:
        china_weight = (0.05 - 0.03)/(0.8 - 0.2) * (equity_pct - 0.2) + 0.03
    elif equity_pct < 0.25:
        china_weight = 0

    region_weight = base_weight - global_weight - china_weight
    weight_overall_df_list = []
    for date in dates_list:
        filename = date.strftime('%Y-%m-%d') + '.csv'
        index_p = fundsfile_list.index(filename)
        funds_df_tmp = fundsbook_list[index_p]
        funds_df_tmp = funds_df_tmp.dropna()
        funds_df_tmp = funds_df_tmp.set_index('class')

        index_mc_df_tmp = region_index_mc_df[np.logical_and(region_index_mc_df.index >= date - relativedelta(months=12),
                                                             region_index_mc_df.index < date)]
        index_mc_df_tmp = index_mc_df_tmp.ffill().last('1d').transpose()

        # ======================
        # Equity-bonds weighting
        # ======================
        weights_of_global_df = funds_df_tmp.copy().loc[['Global'], ['Funds']]
        weights_of_global_df['Weight'] = global_weight

        if china_weight != 0:
            weights_of_china_df = funds_df_tmp.copy().loc[['China'], ['Funds']]
            weights_of_china_df['Weight'] = china_weight
            weights_of_global_df = pd.concat([weights_of_global_df, weights_of_china_df], axis = 0)
        else:
            pass

        if equity_pct < 0.15:
            if 'US' in funds_df_tmp.index.tolist():
                weights_of_region_df = funds_df_tmp.loc[['US'], ['Funds']]
                weights_of_region_df['Weight'] = region_weight
            else:
                # indices_pct = equity_pct_adj
                weights_of_region_df = pd.DataFrame()
            weights_of_equity_funds_df = pd.concat([weights_of_global_df, weights_of_region_df], axis = 0)
        else:
            region_list_tmp = [clas for clas in region_list if clas in funds_df_tmp.index.tolist()]
            weights_of_region_df_tmp = index_mc_df_tmp.copy().loc[region_list_tmp, :]
            weights_of_region_df_tmp.columns = ['Weight']
            weights_of_region_df_tmp['Weight'] = weights_of_region_df_tmp['Weight']/\
                                                 weights_of_region_df_tmp['Weight'].sum() * region_weight

            weights_of_region_df_tmp['Funds'] = funds_df_tmp.copy().loc[weights_of_region_df_tmp.index, 'Funds']

            weights_of_region_df = _very_important_function(weights_of_funds_df = weights_of_region_df_tmp,
                                                            type = 'adjust_min', bound = lower_bound)

            if region_preference != 'B1':
                if region_clas in weights_of_region_df.index.tolist():
                    weights_of_region_df.loc[region_clas, 'Weight'] = \
                        weights_of_region_df.loc[region_clas, 'Weight'] + preference_adj_pct
                elif region_clas in funds_df_tmp.index.tolist():
                    weights_of_region_add_df =  funds_df_tmp.copy().loc[[region_clas], ['Funds']]
                    weights_of_region_add_df['Weight'] = preference_adj_pct
                    weights_of_region_df = pd.concat([weights_of_region_df, weights_of_region_add_df], axis = 0)

            if sector_preference != 'A1' and sector_clas in funds_df_tmp.index.tolist():
                weights_of_sector_df = funds_df_tmp.loc[[sector_clas], ['Funds']]
                weights_of_sector_df['Weight'] = preference_adj_pct
            else:
                weights_of_sector_df = pd.DataFrame()
            weights_of_equity_funds_df = pd.concat([weights_of_global_df, weights_of_region_df, weights_of_sector_df], axis=0)
            weights_of_equity_funds_df['Weight'] = weights_of_equity_funds_df['Weight']/ \
                                           weights_of_equity_funds_df['Weight'].sum() * equity_pct
        # ====================
        # Bonds-fund weighting
        # ====================
        bond_list_tmp = [clas for clas in bond_list if clas in funds_df_tmp.index.tolist()]
        funds_bonds_list = funds_df_tmp.loc[bond_list_tmp, 'Funds'].dropna().tolist()
        fund_bond_price_df_tmp = price_df[np.logical_and(price_df.index>=date - relativedelta(months = 12),
                                                    price_df.index<date)][funds_bonds_list]
        # clean the data if necessary
        fund_bond_price_df_tmp = fund_bond_price_df_tmp.fillna(method='ffill').fillna(method='bfill')
        # There might be duplicated indices since more than 1 funds match an index
        fund_bond_price_df_tmp = fund_bond_price_df_tmp.T.drop_duplicates().T
        for col in fund_bond_price_df_tmp.columns:
            fund_bond_price_df_tmp[col] = fund_bond_price_df_tmp[col].replace(to_replace=0, method='ffill').replace(
                to_replace=0, method='bfill')

        # load funds data
        weights_of_bond_funds_df_tmp = compute_weights_of_bond_funds(bond_fund_price=fund_bond_price_df_tmp,
                                                              method='risk_parity')
        weights_of_bond_funds_df_tmp['Weight'] = weights_of_bond_funds_df_tmp['Weight'] * (1 - equity_pct)
        weights_of_bond_funds_df_tmp['class'] = funds_df_tmp.reset_index().set_index('Funds').copy()\
                                                .loc[weights_of_bond_funds_df_tmp.index, 'class']
        weights_of_bond_funds_df_tmp.index.name = 'Funds'
        weights_of_bond_funds_df_tmp = weights_of_bond_funds_df_tmp.reset_index().set_index('class')

        weights_of_bond_funds_df = _very_important_function(weights_of_funds_df=weights_of_bond_funds_df_tmp,
                                                        type='adjust_min', bound=lower_bound)

        # make sure special_clas is the lowest one
        if 'High Yield' in bond_list_tmp:
            special_clas = 'High Yield'
            other_clas = [c for c in weights_of_bond_funds_df.index if c != special_clas]
            other_bond_min_weight = weights_of_bond_funds_df.loc[other_clas, 'Weight'].min()
            special_weight_tmp = weights_of_bond_funds_df.loc[special_clas, 'Weight']
            if other_bond_min_weight < special_weight_tmp:
                weights_of_bond_funds_df.loc[special_clas, 'Weight'] = other_bond_min_weight
                weights_of_bond_funds_df.loc[other_clas, 'Weight'] = \
                    weights_of_bond_funds_df.loc[other_clas, 'Weight'] + (special_weight_tmp-other_bond_min_weight)/len(other_clas)

            # control the upper_bound of special_clas
            bond_funds_num = len(weights_of_bond_funds_df)
            # upper_bound_for_spe = ((0.0549 - 0.1151) / (0.7 - 0.2) * (equity_pct - 0.2) + 0.1151) * 3/bond_funds_num

            upper_bound_for_spe = ((0.2 - 0.12) / (0.8 - 0.3) * (equity_pct - 0.3) + 0.12) * (1 - equity_pct)
            special_weight_tmp = weights_of_bond_funds_df.loc[special_clas, 'Weight']

            if special_weight_tmp > upper_bound_for_spe:
                weights_of_bond_funds_df.loc[special_clas, 'Weight'] = upper_bound_for_spe
                weights_of_bond_funds_df.loc[other_clas, 'Weight']\
                        =  weights_of_bond_funds_df.loc[other_clas, 'Weight'] \
                           + (special_weight_tmp - upper_bound_for_spe) /(bond_funds_num - 1)
            else:
                pass

        weights_df_tmp = pd.concat([weights_of_equity_funds_df, weights_of_bond_funds_df], axis = 0)
        weights_df_tmp['Weight'] = weights_df_tmp['Weight']/weights_df_tmp['Weight'].sum()
        weights_df_tmp.to_csv(join(savepath, date.strftime('%Y-%m-%d') + '.csv'))
        print('Succeed on ', date.date(), ' with ', len(weights_df_tmp), ' Funds.')
        weight_overall_df_list.append(weights_df_tmp.set_index('Funds').rename(columns = {'Weight': date}))

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
        portfolio_feature_df_tmp['region_preference'] = [region_preference]
        portfolio_feature_df_tmp['funds_number'] = [len(weight_df_tmp)]
        portfolio_feature_df_tmp['min_weight'] = [weight_df_tmp['Weight'].min()]
        portfolio_feature_df_tmp['max_weight'] = [weight_df_tmp['Weight'].max()]
        # portfolio_feature_df_tmp['real_beta'] = [beta_tmp]

        if date != dates_list2[0]:
            turnover_df = pd.merge(weight_his_forturnover_df, weight_df_tmp, left_index=True, right_index=True,
                                   how='outer').fillna(0)
            turnover = abs(turnover_df['Weight_x'] - turnover_df['Weight_y']).sum() / 2
            portfolio_feature_df_tmp['turnover'] = [turnover]
            print('turnover is ', turnover)
        portfolio_feature_df_tmp.index = [str(date.date())]
        print(portfolio_feature_df_tmp)
        portfolio_feature_df_list.append(portfolio_feature_df_tmp)

        weight_his_forturnover_df = weight_df_tmp
    portfolio_feature_df = pd.concat(portfolio_feature_df_list, axis=0)
    return weight_overall_df, portfolio_feature_df


if __name__ == '__main__':
    foropt_path = join(data_path, 'foropt')
    # return_df = pd.read_csv(join(foropt_path, 'return.csv'), parse_dates=[0], index_col=0)
    # beta_df = pd.read_csv(join(foropt_path, 'SHSZ300 Index_growth_beta.csv'), parse_dates=[0], index_col=0)
    # type_df = pd.read_csv(join(foropt_path, 'type.csv'), parse_dates=[0], index_col=0, low_memory=False)
    price_df = pd.read_csv(join(foropt_path, 'price.csv'), parse_dates=[0], index_col=0)
    equity_pct_df = pd.read_csv(join(config_path, 'equity_pct.csv'))
    #
    # for equity_pct in equity_pct_df['equity_pct'].tolist():
    #     compute_weights(start_date = '2014-04-30', end_date = '2020-04-30', equity_pct = equity_pct, sector_preference = 'A1',
    #                     region_preference = 'B1', lower_bound = 0.03, price_df = price_df)

    weight_overall_df, _= compute_weights(start_date='2016-02-29', end_date='2020-07-31', equity_pct=0.07, sector_preference='A1',
                            region_preference='B1', lower_bound=0.03, price_df=price_df)