# from choice_client import c
from algorithm import addpath
import pandas as pd
import numpy as np
import os, time
import math
from datetime import datetime
from datamaster import dm_client
dm_client.refresh_config()
dm_client.start()


def allocationDownloader(start_date, end_date, field, folder='raw'):
    # option = 'Level=1,_SalePosition=L,nonmonthendportfolio=all'
    secid_list = pd.read_csv(addpath.ref_path, index_col=0)['ms_secid'].tolist()

    if field in ['GlobalStockSectorBreakdown', 'AssetAllocation', 'StyleBoxBreakdown', 'MarketCapitalBreakdown',
                 'GlobalBondSectorBreakdown']:
        option = 'Level=1,_SalePosition=L'
    elif field in ['MaturityRange', 'CreditQualityBreakdown', 'FixedIncomeStyleBoxNineCellBreakdown']:
        option = '_SalePosition=L'

    save_path = os.path.join(addpath.historical_path, 'categorization', field, folder)
    if not os.path.exists(save_path):
        os.makedirs(save_path)

    error_list = []
    batch_size = 10
    for n in range(math.ceil((len(secid_list) / batch_size))):
        start_time = datetime.now()
        time.sleep(0.05)
        sub_secid_list = secid_list[n * batch_size:n * batch_size + batch_size]
        result = dm_client.instrument_fund_portfolio(symbols=sub_secid_list, start_date=start_date, end_date=end_date,
                                                     fields=field, options=option)
        factor = pd.DataFrame()
        for secid in sub_secid_list:
            try:
                factor = pd.DataFrame(result['values'][secid]).T
                factor.set_index(pd.DatetimeIndex(factor.index), inplace=True)
                factor = factor.resample('1M').last().ffill()
                first_date = factor.index.tolist()[0]
                first_to_end_date = list(pd.date_range(first_date, end_date, freq='M').tolist())
                factor = factor.reindex(first_to_end_date, fill_value=np.nan)
                factor.fillna(method='ffill', inplace=True)
                factor.to_csv(os.path.join(save_path, '{}.csv'.format(secid)), encoding='utf-8')
            except:
                error_list.append(secid)
                continue
        end_time = datetime.now()
        if factor.empty:
            print('Failed: {} in {} sec'.format((n+1)*batch_size, str(end_time - start_time)))
        else:
            print('Downloaded: {} in {} sec'.format((n + 1) * batch_size, str(end_time - start_time)))

    error_df = pd.DataFrame(error_list)
    error_df.to_csv(os.path.join(addpath.historical_path, 'categorization', field, '{}_missing.csv'.format(field)), encoding='utf-8')

def monthly_factor_file_prepare(field, start, end):
    print('transforming {} ...'.format(field))
    # summary_path = addpath.bundle_path
    rawpath = os.path.join(addpath.historical_path, 'categorization', field, 'raw')
    savepath = os.path.join(addpath.historical_path, 'categorization', field)
    summary_header = 'ms_secid'

    if os.path.exists(savepath):
        pass
    else:
        os.makedirs(savepath)

    # ticker_path = os.path.join(addpath.reference_data_path, "cn_fund_full.csv")
    ms_secids = pd.read_csv(addpath.ref_path)
    ms_secids = ms_secids[summary_header].tolist()

    data_list = []
    missing_data_list = []
    for ms_secid in ms_secids:
        try:
            tmp_path = os.path.join(rawpath, ms_secid + ".csv")
            tmp = pd.read_csv(tmp_path, parse_dates=[0], index_col=0)
            tmp.fillna(0, inplace=True)
            tmp = tmp.resample('M').last().ffill()
            if field == 'AssetAllocation':
                tmp.rename(columns={'1':'Stock', '3':'Bond', '5':'Preferred', '6':'Convertible',
                                    '7':'Cash', '8':'Other'},
                           inplace=True)
            elif field == 'GlobalStockSectorBreakdown':
                tmp.rename(columns={'101': 'Basic Materials', '102': 'Consumer Cyclical',
                                    '103': 'Financial Services', '104': 'Real Estate', '205': 'Consumer Defensive',
                                    '206': 'Healthcare', '207': 'Utilities', '308': 'Communication Services',
                                    '309': 'Energy', '310': 'Industrials', '311': 'Technology'},
                           inplace=True)
            elif field == 'StyleBoxBreakdown':
                tmp.rename(columns={'1':'Large Value', '2':'Large Blend', '3':'Large Growth',
                                    '4':'Mid Value', '5':'Mid Blend', '6':'Mid Growth',
                                    '7':'Small Value', '8':'Small Blend', '9':'Small Growth'},
                           inplace=True)
            elif field == 'MarketCapitalBreakdown':
                tmp.rename(columns={'1':'Giant', '2':'Large', '3':'Medium',
                                    '4':'Small', '5':'Micro'},
                           inplace=True)
            elif field == 'GlobalBondSectorBreakdown':
                tmp.rename(columns={'10':'Government', '20':'Municipal', '30':'Corporate',
                                    '40':'Securitized', '50':'Cash & Equivalents', '60':'Derivative'},
                           inplace=True)
            elif field == 'MaturityRange':
                tmp.rename(columns={'1':'1-3Y', '2':'3-5Y', '3':'5-7Y', '4':'7-10Y', '5':'10-15Y',
                                    '6':'15-20Y', '7':'20-30Y', '8':'30+Y', '9':'1-7D', '10':'8-30D',
                                    '11':'31-90D', '12':'91-182D', '13':'183-364D'},
                           inplace=True)
            elif field == 'CreditQualityBreakdown':
                tmp.rename(columns={'1':'Government', '2':'AAA', '3':'AA', '4':'A', '5':'BBB',
                                    '6':'BB', '7':'B', '8':'Below_B', '9':'Not Rated'},
                           inplace=True)
            elif field == 'FixedIncomeStyleBoxNineCellBreakdown':
                tmp.rename(columns={'1':'Short Term High Quality', '2':'Intermediate Term High Quality', '3':'Long Term High Quality',
                                    '4':'Short Term Medium Quality', '5':'Intermediate Term Medium Quality', '6':'Long Term Medium Quality',
                                    '7':'Short Term Low Quality', '8':'Intermediate Term Low Quality', '9':'Long Term Low Quality'},
                           inplace=True)
            tmp['ms_secid'] = ms_secid
            data_list.append(tmp)
        except Exception as e:
            missing_data_list.append(ms_secid)
            # print(e)
            # print("No data for " + ms_secid)
            continue

    data_raw = pd.concat(data_list)
    if 'date' in data_raw.columns:
        data_raw = data_raw.set_index('date')

    dates = pd.date_range(start, end, freq='M').tolist()
    for date in dates:
        factor_monthly = data_raw[data_raw.index == date.strftime("%Y-%m-%d")]
        factor_monthly = factor_monthly.set_index('ms_secid')
        factor_monthly = factor_monthly.dropna(how='all', axis=0)
        output_path = os.path.join(savepath, date.strftime("%Y-%m-%d") + '.csv')
        factor_monthly.to_csv(output_path)
    missing_data = pd.DataFrame(missing_data_list)
    missing_data.columns = ['ms_secid']
    missing_data.set_index('ms_secid', inplace=True)
    missing_data.to_csv(os.path.join(addpath.historical_path, 'categorization', field, '{}_missing.csv'.format(field)))



if __name__ == '__main__':
    allocationDownloader(start_date='2014-12-31', end_date='2020-12-31', field='GlobalStockSectorBreakdown')
    allocationDownloader(start_date='2014-12-31', end_date='2020-12-31', field='AssetAllocation')
    allocationDownloader(start_date='2014-12-31', end_date='2020-12-31', field='StyleBoxBreakdown')
    allocationDownloader(start_date='2014-12-31', end_date='2020-12-31', field='MarketCapitalBreakdown')
    allocationDownloader(start_date='2014-12-31', end_date='2020-12-31', field='GlobalBondSectorBreakdown')
    # allocationDownloader(start_date='2013-12-31', end_date='2020-12-31', field='MaturityRange')


    monthly_factor_file_prepare('GlobalStockSectorBreakdown', '2014-12-31', '2020-12-31')
    monthly_factor_file_prepare('AssetAllocation', '2014-12-31', '2020-12-31')
    monthly_factor_file_prepare('StyleBoxBreakdown', '2014-12-31', '2020-12-31')
    monthly_factor_file_prepare('MarketCapitalBreakdown', '2014-12-31', '2020-12-31')
    monthly_factor_file_prepare('GlobalBondSectorBreakdown', '2014-12-31', '2020-12-31')
    # monthly_factor_file_prepare('MaturityRange', '2013-12-31', '2020-12-31')
