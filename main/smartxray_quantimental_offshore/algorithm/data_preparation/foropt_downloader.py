import pandas as pd
import math, os
from datamaster import dm_client
from algorithm import addpath
dm_client.refresh_config()
dm_client.start()

def fundamental_factor_download(secid_list, end_date, fields, options):
    factor_all = pd.DataFrame()
    for n in range(math.ceil((len(secid_list) / 50))):
        sub_secid_list = secid_list[n * 50:n * 50 + 50]
        result = dm_client.instrument_fund_portfolio(symbols=sub_secid_list, start_date='2018-01-31', end_date=end_date,
                                                     fields=fields, options=options)
        for secid in sub_secid_list:
            print(secid)
            try:
                factor = pd.DataFrame(result['values'][secid]).T
                factor = factor.iloc[[-1]]
                factor.index = [secid + '_' + end_date]
            except:
                if fields == 'StyleBoxBreakdown':
                    factor = pd.DataFrame(columns=['1', '2', '3', '4', '5', '6', '7', '8', '9'],
                                          index=[secid + '_' + end_date])
                elif fields == 'AssetAllocation':
                    factor = pd.DataFrame(columns=['1', '3', '5', '6', '7', '8'],
                                          index=[secid + '_' + end_date])
                elif fields == 'MarketCapitalBreakdown':
                    factor = pd.DataFrame(columns=['1', '2', '3', '4', '5'],
                                          index=[secid + '_' + end_date])
                elif fields == 'GlobalStockSectorBreakdown':
                    factor = pd.DataFrame(columns=['101', '102', '103', '104', '205', '206', '207', '308', '309', '310',
                                                   '311'],
                                          index=[secid + '_' + end_date])
                elif fields == 'GlobalBondSectorBreakdown' and options == 'Level=1,_SalePosition=L':
                    factor = pd.DataFrame(columns=['10', '20', '30', '40', '50', '60'],
                                          index=[secid + '_' + end_date])
                elif fields == 'GlobalBondSectorBreakdown' and options == 'Level=2,_SalePosition=L':
                    factor = pd.DataFrame(columns=['1020', '2010', '2020', '3010', '3020', '3030', '3040', '4010', '4020',
                                                   '4030', '4040', '4050', '5010', '6010', '6020', '6030'],
                                          index=[secid + '_' + end_date])
                elif fields == 'CountryExposure':
                    factor = pd.DataFrame(columns=[],
                                          index=[secid + '_' + end_date])
                elif fields == 'RegionalExposure':
                    factor = pd.DataFrame(columns=['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13',
                                                   '14', '15', '16'],
                                          index=[secid + '_' + end_date])
                elif fields == 'BondRegionalExposure':
                    factor = pd.DataFrame(columns=['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13',
                                                   '14', '15', '16'],
                                          index=[secid + '_' + end_date])
                elif fields == 'MaturityRange':
                    factor = pd.DataFrame(columns=['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13'],
                                          index=[secid + '_' + end_date])
                elif fields == 'CreditQualityBreakdown':
                    factor = pd.DataFrame(columns=['1', '2', '3', '4', '5', '6', '7', '8', '9'],
                                          index=[secid + '_' + end_date])
                elif fields == 'FixedIncomeStyleBoxNineCellBreakdown':
                    factor = pd.DataFrame(columns=['1', '2', '3', '4', '5', '6', '7', '8', '9'],
                                          index=[secid + '_' + end_date])
            factor_all = pd.concat([factor_all, factor])

    return factor_all


def get_country_allocation_factor(secid_list, end_date):
    print('Downloading country allocation factor!')
    country_allocation = fundamental_factor_download(secid_list=secid_list, end_date=end_date,
                                                     fields='CountryExposure',
                                                     options='Type=2,_SalePosition=L,nonmonthendportfolio=all')
    country_allocation.to_csv(os.path.join(addpath.data_path, 'categorization', 'country_allocation', end_date+'.csv'))


def get_sector_allocation_factor(secid_list, end_date):
    print('Downloading sector allocation factor!')
    sector_allocation = fundamental_factor_download(secid_list=secid_list, end_date=end_date,
                                                    fields='GlobalStockSectorBreakdown',
                                                    options='Level=1,_SalePosition=L,nonmonthendportfolio=all')
    sector_allocation.to_csv(os.path.join(addpath.data_path, 'categorization', 'sector_allocation', end_date+'.csv'))


def get_credit_quality_factor(secid_list, end_date):
    print('Downloading credit quality factor!')
    credit_quality = fundamental_factor_download(secid_list=secid_list, end_date=end_date,
                                                 fields='CreditQualityBreakdown',
                                                 options='_SalePosition=L,nonmonthendportfolio=all')
    CREDIT_QUALITY_FACTOR_COLUMNS = ['1', '2', '3', '4', '5', '6', '7', '8', '9']
    credit_quality = credit_quality[CREDIT_QUALITY_FACTOR_COLUMNS]
    credit_quality.to_csv(os.path.join(addpath.data_path, 'categorization', 'credit_quality', end_date+'.csv'))


if __name__ == '__main__':
    secid_list =['F0000002PO','F00000JP5X','F00000Y909','F0GBR04JV5','F0GBR04MRF','F0GBR04MRU','F0GBR06IGW',
                 'F0HKG05BUU','F0HKG05BVF','F0HKG070CK','F00000MC3V']
    # secid_list = ['F0000002PO']
    get_credit_quality_factor(secid_list, end_date='2020-11-30')
    get_sector_allocation_factor(secid_list, end_date='2020-11-30')
    get_country_allocation_factor(secid_list, end_date='2020-11-30')