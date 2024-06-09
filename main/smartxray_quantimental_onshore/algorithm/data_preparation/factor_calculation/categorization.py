from algorithm import addpath
import pandas as pd
import numpy as np
import os, warnings
from datetime import datetime
from constant import *
warnings.filterwarnings('ignore')


def categorization_stock_core(fund_tmp, date):
    mktcap_data = pd.read_csv(
        os.path.join(addpath.historical_path, 'categorization', 'MarketCapitalBreakdown', date + '.csv'), index_col=0)

    mktcap_data['Sum'] = mktcap_data.sum(axis=1)
    mktcap_data['Large_Capital'] = mktcap_data['Giant'] + mktcap_data['Large']
    mktcap_data['Medium_Capital'] = mktcap_data['Medium'] + mktcap_data['Small'] + mktcap_data['Micro']
    mktcap_data['Large_Capital_Scaled'] = mktcap_data['Large_Capital'] / mktcap_data['Sum'] * 100.0
    mktcap_data['Medium_Capital_Scaled'] = mktcap_data['Medium_Capital'] / mktcap_data['Sum'] * 100.0

    fund_tmp['category_capital'] = np.nan
    for index, row in fund_tmp.iterrows():
        if row['category'] == 'Equity':
            for key, value in CATEGORIZATION_CAPITAL_MAPPING.items():
                if key in row['info_name']:
                    fund_tmp.loc[index, 'category_capital'] = value
                    break

            if pd.isna(fund_tmp.loc[index, 'category_capital']) and index in mktcap_data.index and mktcap_data.loc[index, 'Sum'] >= 70:
                if mktcap_data.loc[index, 'Large_Capital'] >= 60 and mktcap_data.loc[index, 'Large_Capital_Scaled'] > mktcap_data.loc[index, 'Medium_Capital_Scaled']:
                    fund_tmp.loc[index, 'category_capital'] = 'Large-Cap Equity'
                elif mktcap_data.loc[index, 'Medium_Capital_Scaled'] >= 40 and mktcap_data.loc[index, 'Large_Capital_Scaled'] < mktcap_data.loc[index, 'Medium_Capital_Scaled']:
                    fund_tmp.loc[index, 'category_capital'] = 'Mid/Small-Cap Equity'
        else:
            continue

    print(date, "capital category finished.")
    # fund_tmp.to_csv(os.path.join(addpath.historical_path, 'categorization', date+'.csv'), encoding='utf-8-sig')
    return fund_tmp


def categorization_stock_sector(fund_tmp, date):
    sector_data = pd.read_csv(os.path.join(addpath.historical_path, 'categorization', 'GlobalStockSectorBreakdown', date+'.csv'), index_col=0)
    sector_data['Consumption'] = sector_data['Consumer Cyclical'] + sector_data['Consumer Defensive']
    sector_data.drop(columns=['Consumer Cyclical', 'Consumer Defensive'], inplace=True)
    sector_data['Sum'] = sector_data.sum(axis=1)

    fund_tmp['category_sector'] = np.nan
    for index, row in fund_tmp.iterrows():
        if (row['category'] == 'Equity'):

            for key, value in CATEGORIZATION_SECTOR_MAPPING.items():
                if key in row['info_name']:
                    fund_tmp.loc[index, 'category_sector'] = value
                    break

            if pd.isna(fund_tmp.loc[index, 'category_sector']) and (index in sector_data.index) and sector_data.loc[index, 'Sum'] >= 70:
                sector_tmp = sector_data.loc[index, :].drop('Sum')
                sector_tmp = sector_tmp.sort_values(ascending=False)
                selected_sector = sector_tmp[sector_tmp >= 50].index.tolist()
                if len(selected_sector) != 0:
                    fund_tmp.loc[index, 'category_sector'] = selected_sector[0]
        else:
            continue
    print(date, "sector category finished.")
    # fund_tmp.to_csv(os.path.join(addpath.historical_path, 'categorization', date + '.csv'), encoding='utf-8-sig')
    return fund_tmp


def categorization_stock_style(fund_tmp, date):
    style_data = pd.read_csv(
        os.path.join(addpath.historical_path, 'categorization', 'StyleBoxBreakdown', date + '.csv'), index_col=0)

    style_data['Sum'] = style_data.sum(axis=1)
    style_data['Large_Capital'] = style_data['Large Value'] + style_data['Large Blend'] + style_data['Large Growth']
    style_data['Medium_Capital'] = style_data['Mid Value'] + style_data['Mid Blend'] + style_data['Mid Growth'] + \
                                   style_data['Small Value'] + style_data['Small Blend'] + style_data['Small Growth']

    style_data['Large_Capital_Scaled'] = style_data['Large_Capital'] / style_data['Sum'] * 100.0
    style_data['Medium_Capital_Scaled'] = style_data['Medium_Capital'] / style_data['Sum'] * 100.0

    style_data['Value'] = (style_data['Large Value'] + style_data['Mid Value'] + style_data['Small Value']) / style_data['Sum'] * 100.0
    style_data['Blend'] = (style_data['Large Blend'] + style_data['Mid Blend'] + style_data['Small Blend']) / style_data['Sum'] * 100.0
    style_data['Growth'] = (style_data['Large Growth'] + style_data['Mid Growth'] + style_data['Small Growth']) / style_data['Sum'] * 100.0

    fund_tmp['category_style'] = np.nan
    for index, row in fund_tmp.iterrows():
        if row['category'] == 'Equity':

            for key, value in CATEGORIZATION_STYLE_MAPPING.items():
                if key in row['info_name']:
                    fund_tmp.loc[index, 'category_style'] = value
                    break


            if index in style_data.index and style_data.loc[index, 'Sum'] >= 70:
                if pd.isna(fund_tmp.loc[index, 'category_capital']):
                    if style_data.loc[index, 'Large_Capital'] >= 60 and style_data.loc[index, 'Large_Capital_Scaled'] > style_data.loc[index, 'Medium_Capital_Scaled']:
                        fund_tmp.loc[index, 'category_capital'] = 'Large-Cap Equity'
                    elif style_data.loc[index, 'Medium_Capital_Scaled'] >= 40 and style_data.loc[index, 'Large_Capital_Scaled'] < style_data.loc[index, 'Medium_Capital_Scaled']:
                        fund_tmp.loc[index, 'category_capital'] = 'Mid/Small-Cap Equity'

                if pd.isna(fund_tmp.loc[index, 'category_style']):
                    if style_data.loc[index, 'Value'] >= 60:
                        fund_tmp.loc[index, 'category_style'] = 'Value'
                    elif style_data.loc[index, 'Blend'] >= 60:
                        fund_tmp.loc[index, 'category_style'] = 'Blend'
                    elif style_data.loc[index, 'Growth'] >= 60:
                        fund_tmp.loc[index, 'category_style'] = 'Growth'
        else:
            continue

    print(date, "style category finished.")
    # fund_tmp.to_csv(os.path.join(addpath.historical_path, 'categorization', date + '.csv'), encoding='utf-8-sig')
    return fund_tmp


def categorization_bond(fund_tmp, date):
    bondsector_data = pd.read_csv(
        os.path.join(addpath.historical_path, 'categorization', 'GlobalBondSectorBreakdown', date + '.csv'), index_col=0)


    bondsector_data['Sum'] = bondsector_data['Government'] + bondsector_data['Municipal'] + bondsector_data['Corporate'] \
                             + bondsector_data['Securitized'] + bondsector_data['Derivative']
    bondsector_data['Interest Rate Bond'] = bondsector_data['Government'] + bondsector_data['Municipal']
    bondsector_data['Credit Rate Bond'] = bondsector_data['Corporate'] + bondsector_data['Securitized'] + bondsector_data['Derivative']
    bondsector_data['IR Bond Scaled'] = bondsector_data['Interest Rate Bond'] / bondsector_data['Sum']
    bondsector_data['CR Bond Scaled'] = bondsector_data['Credit Rate Bond'] / bondsector_data['Sum']

    fund_tmp['category_bond'] = np.nan
    for index, row in fund_tmp.iterrows():
        if row['category'] == 'Bond' and index in bondsector_data.index:
            if bondsector_data.loc[index, 'IR Bond Scaled'] > 60 and bondsector_data.loc[index, 'Interest Rate Bond'] > 40:
                fund_tmp.loc[index, 'category_bond'] = 'Interest Rate Bond'
            elif bondsector_data.loc[index, 'CR Bond Scaled'] > 60 and bondsector_data.loc[index, 'Credit Rate Bond'] > 40:
                fund_tmp.loc[index, 'category_bond'] = 'Credit Rate Bond'
        else:
            continue

    return fund_tmp


def categorization_others(fund_tmp):
    for index, row in fund_tmp.iterrows():
        if row['category'] == 'Bond':
            if not pd.isna(fund_tmp.loc[index, 'category_bond']):
                fund_tmp.loc[index, 'category_result'] = fund_tmp.loc[index, 'category_bond']
            else:
                fund_tmp.loc[index, 'category_result'] = 'Bond-Other'
        elif row['category'] == 'Money':
            fund_tmp.loc[index, 'category_result'] = 'Money'
        elif row['category'] == 'Equity':
            if not pd.isna(fund_tmp.loc[index, 'category_sector']):
                # sector_list = fund_tmp.loc[index, 'category_sector'].split(',')
                fund_tmp.loc[index, 'category_result'] = 'Equity-' + fund_tmp.loc[index, 'category_sector']
            elif not pd.isna(fund_tmp.loc[index, 'category_capital']):
                fund_tmp.loc[index, 'category_result'] = fund_tmp.loc[index, 'category_capital']
            elif not pd.isna(fund_tmp.loc[index, 'category_style']):
                fund_tmp.loc[index, 'category_result'] = 'Equity-' + fund_tmp.loc[index, 'category_style']
            else:
                fund_tmp.loc[index, 'category_result'] = 'Equity-Other'

        elif row['category'] == 'Others':
            fund_tmp.loc[index, 'category_result'] = 'Others'
        elif row['category'] == 'Alternative':
            fund_tmp.loc[index, 'category_result'] = 'Alternative'


    return fund_tmp


if __name__ == '__main__':
    start_date = '2014-12-31'
    end_date = '2020-12-31'

    dates = pd.date_range(start_date, end_date, freq='M')
    dates = [datetime.strftime(date, "%Y-%m-%d") for date in dates]
    cn_fund_full = pd.read_csv(addpath.ref_path, index_col=0)

    for date in dates:
        fund_tmp = cn_fund_full[np.logical_and(cn_fund_full['inception_date'] <= date, cn_fund_full['obsolete_date'].isna())]
        fund_tmp = fund_tmp[['info_name', 'ms_category', 'category']]
        print("Number of Funds:", len(fund_tmp))
        fund_core = categorization_stock_core(fund_tmp, date)
        fund_sector = categorization_stock_sector(fund_core, date)
        fund_style = categorization_stock_style(fund_sector, date)
        fund_bond = categorization_bond(fund_style, date)
        fund_result = categorization_others(fund_bond)

        fund_result.to_csv(os.path.join(addpath.historical_path, 'categorization', 'fund_category', date + '.csv'), encoding='utf-8-sig')
