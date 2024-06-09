import pandas as pd

def theme_conditions(last_period_factors,symbol_industry, investment_univ,factor1, factor2, factor3,factor4, factor5, factor6):
    criteria = {
        factor1: ['Positive',0.2],
        factor2: ['Positive',0.2],
        factor3: ['Positive', 0.2],
        factor4: ['Positive', 0.2],
        factor5: ['Positive', 0.2],
        factor6: ['Negative', 0.2],
        'LEV': ['Negative', 0.2],
    }

    selection_tmp = last_period_factors.copy()[list(criteria.keys())]
    selection_tmp = selection_tmp.loc[investment_univ, :]
    selection_tmp['num_na']=selection_tmp.isnull().sum(axis=1)

    selection_tmp=selection_tmp[selection_tmp['num_na']<=3]
    selection_tmp=selection_tmp.drop(['num_na'],axis=1)
    # selection_tmp = selection_tmp.dropna()
    for col in selection_tmp.columns:
        selection_tmp[col] = selection_tmp[col].clip(lower=selection_tmp[col].quantile(0.025), upper=selection_tmp[col].quantile(0.975))
        selection_tmp[col]=selection_tmp[col].fillna(selection_tmp[col].mean())

    symbol_industry=symbol_industry.loc[investment_univ,:]
    selection_tmp['industry']=symbol_industry['SEHKENG']

    # selection_tmp=selection_tmp[selection_tmp['industry'].isin(['Information Technology'])]  #'Financials','Financials','Industrials','Consumer Discretionary','Healthcare'
    ind_list=list(set(selection_tmp['industry'].tolist()))

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
    rankings['MARKET_CAP'] = last_period_factors.loc[rankings.index, 'MARKET_CAP']
    rankings['CLOSE'] = last_period_factors.loc[rankings.index, 'CLOSE']
    rankings['LOTSIZE'] = symbol_industry.loc[rankings.index, 'LOTSIZE']

    return rankings
