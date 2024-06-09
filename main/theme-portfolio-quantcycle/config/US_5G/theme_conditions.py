import pandas as pd

def theme_conditions(last_period_factors,symbol_industry, investment_univ,factor1, factor2, factor3,factor4, factor5, factor6):
    criteria = {
        factor1: ['Positive',0.2],
        factor2: ['Positive',0.2],
        factor3: ['Negative', 0.2],
        factor4: ['Positive', 0.2],
        # factor5: ['Negative', 0.2],
        factor6: ['Positive', 0.2],

    }
    sum_weight=0
    for value in criteria.values():
        sum_weight=sum_weight+value[1]
    if sum_weight!=1:
        print('the sum of factor weights is not 1!')

    num_asset=60
    white_list=symbol_industry[symbol_industry['white_list']==1].index.tolist()
    white_list=list(set(white_list)&set(investment_univ))
    num_whitelist=len(white_list)
    num_selected=num_asset-num_whitelist
    investment_univ=[symbol for symbol in investment_univ if symbol not in white_list]

    selection_tmp = last_period_factors.copy()[list(criteria.keys())]
    # selection_tmp = selection_tmp.loc[white_list, :]
    selection_tmp = selection_tmp.loc[investment_univ, :]

    # selection_tmp = selection_tmp.dropna()
    for col in selection_tmp.columns:
        selection_tmp[col] = selection_tmp[col].clip(lower=selection_tmp[col].quantile(0.025), upper=selection_tmp[col].quantile(0.975))

    selection_tmp['industry']=symbol_industry['gicsubind']


    selection_tmp['zscore'] = 0
    for col in list(criteria.keys()):

        if criteria[col][0] == 'Positive':
            selection_tmp[col+'_rank'] = selection_tmp[col].rank(ascending=False)
        else:
            selection_tmp[col+'_rank'] = selection_tmp[col].rank(ascending=True)
        factor_mean_tmp = selection_tmp[col+'_rank'].mean()
        factor_std_tmp = selection_tmp[col+'_rank'].std()
        selection_tmp['zscore'] = selection_tmp['zscore'] + (
                    factor_mean_tmp-selection_tmp[col+'_rank']) / factor_std_tmp * criteria[col][1]

    selection_tmp['rank'] = selection_tmp['zscore'].rank(ascending=False)
    selection_tmp=selection_tmp[selection_tmp['rank']<=num_selected]
    investment_univ=selection_tmp.index.tolist()

    investment_univ.extend(white_list)
    selection_tmp = last_period_factors.copy()[list(criteria.keys())]
    selection_tmp = selection_tmp.loc[investment_univ, :]
    for col in list(criteria.keys()):
        selection_tmp[col] = selection_tmp[col].clip(lower=selection_tmp[col].quantile(0.025), upper=selection_tmp[col].quantile(0.975))
    selection_tmp['industry']=symbol_industry['gicsubind']
    selection_tmp['zscore'] = 0
    for col in list(criteria.keys()):

        if criteria[col][0] == 'Positive':
            selection_tmp[col+'_rank'] = selection_tmp[col].rank(ascending=False)
        else:
            selection_tmp[col+'_rank'] = selection_tmp[col].rank(ascending=True)
        factor_mean_tmp = selection_tmp[col+'_rank'].mean()
        factor_std_tmp = selection_tmp[col+'_rank'].std()
        selection_tmp['zscore'] = selection_tmp['zscore'] + (
                    factor_mean_tmp-selection_tmp[col+'_rank']) / factor_std_tmp * criteria[col][1]

    selection_tmp['rank'] = selection_tmp['zscore'].rank(ascending=False)
    selection_tmp['ind_rank'] = selection_tmp['zscore'].rank(ascending=False)


    selection_tmp['ini_weight'] = selection_tmp['zscore']+1
    temp=selection_tmp['zscore'][selection_tmp['zscore']<0]
    selection_tmp['ini_weight'][selection_tmp['zscore']<0]=1/(1-temp)
    selection_tmp['ini_weight']=selection_tmp['ini_weight']/selection_tmp['ini_weight'].sum()
    selection_tmp=selection_tmp.dropna()
    print(selection_tmp)

    rankings = selection_tmp
    rankings['MARKET_CAP'] = last_period_factors.loc[rankings.index, 'MARKET_CAP']
    rankings['CLOSE'] = last_period_factors.loc[rankings.index, 'CLOSE']
    return rankings
