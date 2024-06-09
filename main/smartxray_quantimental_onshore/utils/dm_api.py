import pandas as pd
import math, os
from algorithm import addpath
from datetime import datetime
from datetime import timedelta
from datamaster import dm_client
import time
import numpy as np
dm_client.refresh_config()
dm_client.start()

def cn_fund_full_download():
    secid_list = dm_client.filter_id_list(region='cn', type='fund')
    secid_list = secid_list.dropna(subset=['ms_secid'], how='all')
    secid_list = list(secid_list['ms_secid'])
    fund_info_all = pd.DataFrame()
    for n in range(math.ceil((len(secid_list) / 50))):
        time.sleep(0.5)
        sub_secid_list = secid_list[n * 50:n * 50 + 50]
        result = dm_client.info(symbols=sub_secid_list)
        for secid in sub_secid_list:
            print(secid)
            try:
                fund_info = pd.DataFrame.from_dict(result['values'][secid][1:]).T
                fund_info.columns = result['fields'][1:]
                fund_info.index = [secid]
            except:
                fund_info = pd.DataFrame(columns=result['fields'][1:], index=[secid])
            fund_info_all = pd.concat([fund_info_all, fund_info])

    return fund_info_all

def offshore_fund_full_download():
    secid_list1 = dm_client.filter_id_list(region='hk', type='fund')
    secid_list2 = dm_client.filter_id_list(region='us', type='fund')
    secid_list1 = list(secid_list1['ms_secid'])
    secid_list2 = list(secid_list2['ms_secid'])
    secid_list = list(set(secid_list1 + secid_list2))
    fund_info_all = pd.DataFrame()
    for n in range(math.ceil((len(secid_list) / 50))):
        sub_secid_list = secid_list[n * 50:n * 50 + 50]
        result = dm_client.info(symbols=sub_secid_list)
        for secid in sub_secid_list:
            # print(secid)
            try:
                fund_info = pd.DataFrame.from_dict(result['values'][secid][1:]).T
                fund_info.columns = result['fields'][1:]
                fund_info.index = [secid]
            except:
                fund_info= pd.DataFrame(columns=result['fields'][1:], index=[secid])
            fund_info_all = pd.concat([fund_info_all, fund_info])

    return fund_info_all

def historical_price_download(secid):
    result = dm_client.historical(symbols=secid, start_date='2000-01-04', fields='fund_fqnav')
    return result

def historical_factor_download(secid_list, end_date, fields):
    factor_all = pd.DataFrame()
    adjusted_start_date = datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=5)
    adjusted_start_date = adjusted_start_date.strftime("%Y-%m-%d")
    adjusted_end_date = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=5)
    adjusted_end_date =  adjusted_end_date.strftime("%Y-%m-%d")

    for n in range(math.ceil((len(secid_list) / 50))):
        sub_secid_list = secid_list[n * 50:n * 50 + 50]
        result = dm_client.historical(symbols=sub_secid_list, start_date=adjusted_start_date, end_date=adjusted_end_date,
                                      fields=fields)
        for secid in sub_secid_list:
            # print(secid)
            try:
                factor = pd.DataFrame(result['values'][secid])
                factor.columns = result['fields']
                factor = factor.set_index('date')
                factor.index = pd.to_datetime(factor.index)
                factor = factor.asfreq('D', method='ffill')
                factor = pd.DataFrame(factor.loc[end_date]).T
                factor.index = [secid]
            except:
                factor= pd.DataFrame(columns=result['fields'][1:], index=[secid])
            factor_all = pd.concat([factor_all, factor])

    return factor_all

def fundamental_factor_download(secid_list, end_date, fields, options):
    factor_all = pd.DataFrame()
    for n in range(math.ceil((len(secid_list) / 50))):
        sub_secid_list = secid_list[n * 50:n * 50 + 50]
        result = dm_client.instrument_fund_portfolio(symbols=sub_secid_list, start_date=end_date, end_date=end_date,
                                                     fields=fields, options=options)
        for secid in sub_secid_list:
            # print(secid)
            try:
                factor = pd.DataFrame(result['values'][secid]).T
                factor.index = [secid]

            except:
                if fields == 'StyleBoxBreakdown':
                    factor = pd.DataFrame(columns=['1', '2', '3', '4', '5', '6', '7', '8', '9'],
                                          index=[secid])
                elif fields == 'AssetAllocation':
                    factor = pd.DataFrame(columns=['1', '3', '5', '6', '7', '8'],
                                          index=[secid])
                elif fields == 'MarketCapitalBreakdown':
                    factor = pd.DataFrame(columns=['1', '2', '3', '4', '5'],
                                          index=[secid])
                elif fields == 'GlobalStockSectorBreakdown':
                    factor = pd.DataFrame(columns=['101', '102', '103', '104', '205', '206', '207', '308', '309', '310',
                                                   '311'], index=[secid])
                elif fields == 'GlobalBondSectorBreakdown' and options == 'Level=1,_SalePosition=L':
                    factor = pd.DataFrame(columns=['10', '20', '30', '40', '50', '60'],
                                          index=[secid])
                elif fields == 'GlobalBondSectorBreakdown' and options == 'Level=2,_SalePosition=L':
                    factor = pd.DataFrame(
                        columns=['1020', '2010', '2020', '3010', '3020', '3030', '3040', '4010', '4020',
                                 '4030', '4040', '4050', '5010', '6010', '6020', '6030'],
                        index=[secid])
                elif fields == 'CountryExposure':
                    factor = pd.DataFrame(columns=[],
                                          index=[secid])
                elif fields == 'RegionalExposure':
                    factor = pd.DataFrame(columns=['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12',
                                                   '13', '14', '15', '16'],
                                              index=[secid])
                elif fields == 'BondRegionalExposure':
                    factor = pd.DataFrame(columns=['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13',
                                                   '14', '15', '16'],
                                          index=[secid])
                elif fields == 'MaturityRange':
                    factor = pd.DataFrame(columns=['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13'],
                                          index=[secid])
                elif fields == 'CreditQualityBreakdown':
                    factor = pd.DataFrame(columns=['1', '2', '3', '4', '5', '6', '7', '8', '9'],
                                          index=[secid])
                elif fields == 'FixedIncomeStyleBoxNineCellBreakdown':
                    factor = pd.DataFrame(columns=['1', '2', '3', '4', '5', '6', '7', '8', '9'],
                                          index=[secid])
            factor_all = pd.concat([factor_all, factor])

    return factor_all

def equity_statistics_download(secid_list, end_date, fields, options):
    factor_all = pd.DataFrame()
    for n in range(math.ceil((len(secid_list) / 50))):
        print('Downloading {} {}'.format(fields, (n+1)*50))
        sub_secid_list = secid_list[n * 50:n * 50 + 50]
        result = dm_client.instrument_fund_portfolio(symbols=sub_secid_list, start_date=end_date, end_date=end_date,
                                                     fields=fields, options=options)
        for secid in sub_secid_list:
            try:
                result_df = pd.DataFrame(result['values'])
                result_df = result_df.set_index([0]).iloc[:, 1:]
                factor = pd.DataFrame(result_df.loc[secid]).T
                factor.columns = result['fields'][2:]
                factor.index = [secid]
            except:
                if fields == 'equity_statistics':
                    factor = pd.DataFrame(columns=result['fields'][2:], index=[secid])
                elif fields == 'BondStatistics' or fields == 'BondStatisticsCalculated':
                    try:
                        factor = pd.DataFrame(columns=result['fields'][2:], index=[secid])
                    except:
                        continue
            factor_all = pd.concat([factor_all, factor])

    return factor_all

if __name__ == '__main__':
    cn_fund_full = cn_fund_full_download()
    cn_fund_full = cn_fund_full[cn_fund_full['status'] == 1]
    cn_fund_full.to_csv(os.path.join(addpath.config_path, 'cn_fund_full_2.csv'), encoding='utf-8-sig')