import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def convert_cross_target_to_major(cross_pair_target, T150_price_df):
    '''
    将 cross_pair 的 target holding 转为 major pair 的 target holding
    例如  ccp      holding   last_price
         AUDJPY    1000       70.365
         USDJPY    1000       107.89
    转为  ccp      holding   last_price  usd_amt   TargetRatio
         AUDUSD    1000       0.6515     651.5       0.2828
         USDJPY    1652.2     107.89     1652.2      0.7172
    只取 ccp、TargetRatio
    '''
    cross_pair_target = cross_pair_target.merge(T150_price_df, on='ccp', how='left')
    cross_pair_target['CCY1'] = cross_pair_target['ccp'].str[:3]
    cross_pair_target['CCY2'] = cross_pair_target['ccp'].str[3:]
    cross_pair_target['CCY2_holding'] = - cross_pair_target['holding'] * cross_pair_target['T150_price']

    major_pair_target1 = cross_pair_target[['CCY1', 'holding']].rename(columns={'CCY1':'ccy'})
    major_pair_target2 = cross_pair_target[['CCY2', 'CCY2_holding']].rename(columns={'CCY2': 'ccy','CCY2_holding':'holding'})

    major_pair_target = pd.concat([major_pair_target1, major_pair_target2],axis=0)
    major_pair_target = major_pair_target.groupby('ccy')['holding'].sum().reset_index()
    major_pair_target = major_pair_target[major_pair_target['ccy'] != 'USD'].reset_index()

    major_last_price_df = T150_price_df.loc[T150_price_df['ccp'].str.contains('USD')].reset_index()
    major_last_price_df['ccy'] = major_last_price_df['ccp'].apply(lambda x: x.replace('USD',''))

    major_pair_target = major_pair_target.merge(major_last_price_df, on='ccy', how='left')
    major_pair_target['CCY1'] = major_pair_target['ccp'].str[:3]
    major_pair_target['usd_amt'] = np.where(major_pair_target['CCY1'] == 'USD',
                                            -major_pair_target['holding'] / major_pair_target['T150_price'] ,
                                             major_pair_target['holding'] * major_pair_target['T150_price'])
    portfolio_value = major_pair_target['usd_amt'].abs().sum()
    major_pair_target['TargetRatio'] = major_pair_target['usd_amt'] / portfolio_value

    return major_pair_target[['ccp', 'TargetRatio']], portfolio_value

def get_holding_from_csv(result_df):
    '''
    从回测结果中取最后一行，得到cross pair target holding
    '''
    position_columns = [col for col in result_df.columns if 'position' in col]
    cross_pair_target = result_df.iloc[-1][position_columns]
    cross_pair_target.index = [idx.replace('_position', '') for idx in cross_pair_target.index]
    cross_pair_target = pd.DataFrame(cross_pair_target).reset_index()
    cross_pair_target.columns = ['ccp', 'holding']

    return cross_pair_target

def weight_to_holding(target_df, last_price_df, portfolio_value):

    target_df = target_df.merge(last_price_df, on='ccp', how='left')

    target_df['usd_amt'] = target_df['TargetRatio'] * portfolio_value

    target_df['CCY1'] = target_df['ccp'].str[:3]

    target_df['target_holding'] = np.where(target_df['CCY1'] == 'USD',
                                           target_df['usd_amt'],
                                           target_df['usd_amt'] / target_df['last_price'])
    return target_df[['ccp', 'target_holding']]

def get_fx_yesterday(date):
    if date.weekday() == 0:
        return date - timedelta(days=3)
    elif 1<=date.weekday()<=4:
        return date - timedelta(days=1)
    else:
        raise ValueError("today is not a trade day!")


