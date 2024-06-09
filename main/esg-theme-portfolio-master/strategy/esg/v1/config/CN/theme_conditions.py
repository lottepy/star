import pandas as pd

def theme_conditions(last_period_factors,symbol_industry, investment_univ, factor_list):
    criteria = {
        factor_list[0]: ['Positive', 0.1], # 'MARKET_CAP'
        factor_list[1]: ['Positive', 0.1], # 'ETP_TTM'
        factor_list[2]: ['Positive', 0.1], # 'REVOAGROWTH'
        factor_list[3]: ['Positive', 0.3], # 'ROA_SEMI'
        factor_list[4]: ['Positive', 0.1], # 'OPCFOE_SEMI'
        factor_list[5]: ['Negative', 0.1], # 'EBITOAVAR'
        factor_list[6]: ['Positive', 0.1], # 'REVOE_SEMI'
        factor_list[7]: ['Positive', 0.1], # 'OPCF_MARGIN'
        factor_list[8]: ['Negative', 0.1], # 'LEV'
    }

    selection_tmp = last_period_factors.copy()[list(criteria.keys())]
    selection_tmp = selection_tmp.loc[investment_univ, :]
    selection_tmp = selection_tmp.dropna()
    for col in selection_tmp.columns:
        selection_tmp[col] = selection_tmp[col].clip(lower=selection_tmp[col].quantile(0.025), upper=selection_tmp[col].quantile(0.975))


    symbol_industry=symbol_industry.loc[investment_univ,:]
    selection_tmp['industry']=symbol_industry['SW_industry1']
    selection_tmp=selection_tmp.dropna(subset=['industry'])
    ind_list=list(set(selection_tmp['industry'].tolist()))

    selection_list = []
    for ind in ind_list:
        temp = selection_tmp[selection_tmp['industry'] == ind]
        temp['ind_zscore'] = 0
        for col in list(criteria.keys()):
            if criteria[col][0] == 'Positive':
                temp[col + '_rank'] = temp[col].rank(ascending=False)
            else:
                temp[col + '_rank']=temp[col].rank(ascending=True)
            # temp=temp[temp[col + '_rank']<0.5*len(temp)]
            factor_mean_tmp = temp[col + '_rank'].mean()
            factor_std_tmp = temp[col + '_rank'].std()
            temp[col + '_zscore'] = (temp[col + '_rank'] - factor_mean_tmp) / factor_std_tmp
            temp['ind_zscore'] = temp['ind_zscore']+temp[col + '_zscore']*criteria[col][1]

        temp['ind_rank']=temp['ind_zscore'].rank(ascending=True)
        temp = temp[temp['ind_rank']<=5]
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
        selection_tmp['zscore'] = selection_tmp['zscore'] + (selection_tmp['col_rank'] - factor_mean_tmp) / factor_std_tmp * criteria[col][1]

    selection_tmp['rank'] = selection_tmp['zscore'].rank(ascending=True)

    rankings = selection_tmp
    rankings['MARKET_CAP'] = last_period_factors.loc[rankings.index,'MARKET_CAP']
    rankings['CLOSE'] = last_period_factors.loc[rankings.index,'CLOSE']

    return rankings






'''
    for i in range(len(criteria)):

        selection_tmp[list(criteria.keys())[i] + '_rank_pct'] = selection_tmp[list(criteria.keys())[i]].rank(pct=True)
        selection_tmp[list(criteria.keys())[i]][selection_tmp[list(criteria.keys())[i]] > selection_tmp[list(criteria.keys())[i]]\
            .quantile(0.975)] = selection_tmp[list(criteria.keys())[i]].quantile(0.975)
        selection_tmp[list(criteria.keys())[i]][selection_tmp[list(criteria.keys())[i]] < selection_tmp[list(criteria.keys())[i]]\
            .quantile(0.025)] = selection_tmp[list(criteria.keys())[i]].quantile(0.025)

        if list(criteria.values())[i][0] == 'Positive':
            selection_tmp = selection_tmp[selection_tmp[list(criteria.keys())[i] + '_rank_pct'] >= (1 - list(criteria.values())[i][1])]
        else:
            selection_tmp = selection_tmp[selection_tmp[list(criteria.keys())[i] + '_rank_pct'] <= list(criteria.values())[i][1]]

        #selection_tmp.loc[:, list(criteria.keys())[i]] = selection_tmp.loc[:, list(criteria.keys())[0]].rank(pct=True)
    if list(criteria.values())[j][0] == 'Positive':
        selection_tmp['rank'] = selection_tmp[rank_factor].rank(ascending=False)
    else:
        selection_tmp['rank'] = selection_tmp[rank_factor].rank(ascending=True)
    rankings = selection_tmp.loc[:, ['symbol', 'rank', 'MARKET_CAP','CLOSE',list(criteria.keys())[0],\
                                     list(criteria.keys())[1], list(criteria.keys())[2],\
                                     list(criteria.keys())[3], list(criteria.keys())[4],\
                                     list(criteria.keys())[0] + '_rank_pct', list(criteria.keys())[1] + '_rank_pct',\
                                     list(criteria.keys())[2] + '_rank_pct',list(criteria.keys())[3] + '_rank_pct',\
                                     list(criteria.keys())[4] + '_rank_pct']]

    rankings = rankings.set_index('symbol')

    return rankings
'''