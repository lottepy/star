import urllib.parse
import requests
import json
import pandas as pd
import xml.etree.ElementTree as tree

from base_api import BaseAPI
from base_api import MasterID
from config import FUNDAMENTAL_BASE_URL
from config import HISTORICAL_BASE_URL
from config import XPATH
from xml.dom.minidom import parseString

from site_packages.xml2dict import XML2Dict
xml = XML2Dict()

class HistoricalAPI(BaseAPI):

    FUNDAMENTAL_BASE_URL = FUNDAMENTAL_BASE_URL
    HISTORICAL_BASE_URL = HISTORICAL_BASE_URL

    def __init__(self):
        super().__init__()
        self.FUNDAMENTAL_URL = urllib.parse.urljoin(self.BASE_URL, self.FUNDAMENTAL_BASE_URL)
        self.HISTORICAL_URL = urllib.parse.urljoin(self.BASE_URL, self.HISTORICAL_BASE_URL)

    def get_portfolio_holding(self, info_dict):

        try:
            portfolio_holding = info_dict['Holding']['HoldingDetail']
        except KeyError:
            return None

        return portfolio_holding


    def get_asset_allocation(self, info_dict):

        try:
            asset_allocation = info_dict['AssetAllocation']['BreakdownValue']
        except KeyError:
            return None

        return asset_allocation


    def get_region_allocation(self, info_dict):

        regional_exposure = {}
        try:
            regional_exposure['stock'] = info_dict['RegionalExposure']['BreakdownValue']
        except KeyError:
            regional_exposure['stock'] = None

        try:
            regional_exposure['bond'] = info_dict['BondRegionalExposure']['BreakdownValue']
        except KeyError:
            regional_exposure['bond'] = None

        return regional_exposure


    def get_sector_allocation(self, info_dict):

        sector_allocation = {}

        try:
            sector_allocation['stock'] = info_dict['GlobalStockSectorBreakdown']['BreakdownValue']
        except KeyError:
            sector_allocation['stock'] = None

        try:
            sector_allocation['bond'] = info_dict['GlobalBondSector']['GlobalBondSectorBreakdown'][0]['BreakdownValue']
        except KeyError:
            sector_allocation['bond'] = None

        return sector_allocation


    def get_credit_allocation(self, info_dict):

        try:
            credit_allocation = info_dict['CreditQualityBreakdown']['BreakdownValue']

        except KeyError:
            return None

        return credit_allocation


    def get_maturity_allocation(self, info_dict):

        try:
            maturity_allocation = info_dict['MaturityRange']['BreakdownValue']

        except KeyError:
            return None

        return maturity_allocation


    def get_currency_allocation(self, info_dict):

        try:
            currency_allocation = info_dict['RiskCurrencyExposure']['RiskCurrencyExposureValue']

        except KeyError:
            return None

        return currency_allocation


    def get_master_portid(self, tree=None, secid=None):

        try:
            master_portid = tree['FundShareClass']['Fund']['_MasterPortfolioId']['value']
        except:
            return None

        return master_portid


    def get_categoryid(self, tree=None, secid=None):

        try:
            master_portid = tree['FundShareClass']['Fund']['FundBasics']['_CategoryId']['value']
        except:
            return None

        return master_portid


    def get_investment_object(self, tree=None, secid=None):

        try:
            obj_1 = tree['FundShareClass']['Fund']['MultilingualVariation']['LanguageVariation']['RegionVariation'][1]['FundNarratives']['InvestmentCriteria']['InvestmentStrategy']['value']
        except :
            obj_1 = ''

        try:
            obj_2 = tree['FundShareClass']['Fund']['MultilingualVariation']['LanguageVariation']['RegionVariation'][1]['FundNarratives']['InvestmentCriteria']['InvestmentRange']['value']
        except:
            obj_2 = ''

        obj = obj_1 + obj_2

        return obj


    def get_daily_data(self, performanceid=None, start_date=None, end_date=None, DataType=None):
        headers = {'Content-type': self.CONTENT_TYPE}
        params = {'ClientId': self.CLIENT_ID,
                  'DataType': DataType,
                  'PerformanceId': performanceid,
                  'StartDate': start_date,
                  'EndDate': end_date,
                  'Obsolete': '1'
                  }
        url = self.HISTORICAL_URL

        with requests.Session() as session:
            r = session.post(url, params=params, headers=headers)
            content = r.content

            with open('temp_files/daily.txt', 'wb+') as f:
                f.write(content)
            daily_data = pd.read_csv('temp_files/daily.txt', sep=";", encoding='utf-8-sig')
            try:
                daily_data.index = daily_data['Date']
            except:
                return None

            return daily_data


    def get_fundamental_info(self, secid=None):

        fundamental_info = {}
        headers = {'Content-type': self.CONTENT_TYPE}
        params = {'ClientId': self.CLIENT_ID,
                  'Package': self.PACKAGE,
                  'Id': secid,
                  'IDTYpe': 'FundShareClassId',
                  'Content': 1471,
                  'Currencies': 'BAS',
                  'Obsolete': '1'
                  }
        url = self.FUNDAMENTAL_URL

        with requests.Session() as session:
            r = session.post(url, params=params, headers=headers)
            content = parseString(r.content)

            with open('data/warehouse_xml/{}.xml'.format(secid), 'wb+') as f:
                f.write(content.toprettyxml(indent='\t', encoding='utf-8'))

            r = xml.parse('data/warehouse_xml/{}.xml'.format(secid))

            fundamental_info['master_portid'] = api.get_master_portid(secid=secid, tree=r)
            fundamental_info['category_id'] = api.get_categoryid(secid=secid, tree=r)
            fundamental_info['investment_obj'] = api.get_investment_object(secid=secid, tree=r)

            data_df = pd.DataFrame.from_dict(fundamental_info, orient='index')
            data_df.columns = ['info']

        return data_df
    
    
    def download_xml(self, secid=None):

        headers = {'Content-type': self.CONTENT_TYPE}
        params = {'ClientId': self.CLIENT_ID,
                  'Package': self.PACKAGE,
                  'Id': secid,
                  'IDTYpe': 'FundShareClassId',
                  'Content': 1471,
                  'Currencies': 'BAS',
                  'Obsolete': '1'
                  }
        url = self.FUNDAMENTAL_URL

        with requests.Session() as session:
            r = session.post(url, params=params, headers=headers)
            content = parseString(r.content)

            with open('data/warehouse_xml/{}.xml'.format(secid), 'wb+') as f:
                f.write(content.toprettyxml(indent='\t', encoding='utf-8'))



    def get_portfolio_info(self, masterportfolioid=None, date='2018-06-30'):

        portfolio_info = {}

        headers = {'Content-type': self.CONTENT_TYPE}
        params = {'ClientId': self.CLIENT_ID,
                  'Package': self.PACKAGE,
                  'Id': masterportfolioid,
                  'IDTYpe': 'MasterPortfolioId',
                  'Date': date,
                  'Obsolete': '1'
                  }
        url = self.FUNDAMENTAL_URL
        with requests.Session() as session:
            r = session.post(url, params=params, headers=headers)

            try:
                content = parseString(r.content)

                with open('temp_files/portfolio.xml', 'wb+') as f:
                    f.write(content.toprettyxml(indent='\t', encoding='utf-8'))

                r = xml.parse('temp_files/portfolio.xml')

                portfolio_info['full_holding'] = api.get_portfolio_holding(info_dict=r)

                r = r['Portfolio']['PortfolioBreakdown']
                if len(r) < 5: r = r[0]

                portfolio_info['asset_allocation'] = api.get_asset_allocation(info_dict=r)
                portfolio_info['region_allocation_stock'] = api.get_region_allocation(info_dict=r)['stock']
                portfolio_info['region_allocation_bond'] = api.get_region_allocation(info_dict=r)['bond']
                portfolio_info['sector_allocation_stock'] = api.get_sector_allocation(info_dict=r)['stock']
                portfolio_info['sector_allocation_bond'] = api.get_sector_allocation(info_dict=r)['bond']
                portfolio_info['credit_allocation'] = api.get_credit_allocation(info_dict=r)
                portfolio_info['maturity_allocation'] = api.get_maturity_allocation(info_dict=r)
                portfolio_info['currency_allocation'] = api.get_currency_allocation(info_dict=r)

                full_dict = {}
                for key in portfolio_info:
                    sub_dict = {}

                    if key != 'full_holding':
                        if portfolio_info[key]:

                            if (len(portfolio_info[key]) > 2):
                                for mini_dict in portfolio_info[key]:
                                    sub_dict[mini_dict['Type']['value']] = mini_dict['value']
                            else:
                                sub_dict = portfolio_info[key]
                        else:
                            sub_dict = {}

                    else:
                        for mini_dict in portfolio_info[key]:
                            third_dict = {}
                            key_list = [key for key in mini_dict if key != 'value']
                            for sub_key in key_list:
                                third_dict[sub_key] = mini_dict[sub_key]['value']
                            sub_dict[third_dict['SecurityName']] = third_dict
                    full_dict[key] = sub_dict

                # data_json = json.dumps(full_dict)
                # data_df = pd.DataFrame.from_dict(portfolio_info, orient='index')
                # data_df.columns = [date]
            except:
                return {}

        return full_dict


    def get_current_portfolio_info(self, secid=None):

        portfolio_info = {}

        headers = {'Content-type': self.CONTENT_TYPE}
        params = {'ClientId': self.CLIENT_ID,
                  'Package': self.PACKAGE,
                  'Id': secid,
                  'IDTYpe': 'FundShareClassId',
                  'Content': 1471,
                  'Currencies': 'BAS',
                  'Obsolete': '1'
                  }
        url = self.FUNDAMENTAL_URL
        with requests.Session() as session:
            r = session.post(url, params=params, headers=headers)

            try:
                content = parseString(r.content)

                with open('temp_files/portfolio.xml', 'wb+') as f:
                    f.write(content.toprettyxml(indent='\t', encoding='utf-8'))

                r = xml.parse('temp_files/portfolio.xml')

                r = r['FundShareClass']['Fund']['PortfolioList']['Portfolio']
                portfolio_info['full_holding'] = api.get_portfolio_holding(info_dict=r)

                r = r['PortfolioBreakdown']
                if len(r) < 5: r = r[0]

                portfolio_info['asset_allocation'] = api.get_asset_allocation(info_dict=r)
                portfolio_info['region_allocation_stock'] = api.get_region_allocation(info_dict=r)['stock']
                portfolio_info['region_allocation_bond'] = api.get_region_allocation(info_dict=r)['bond']
                portfolio_info['sector_allocation_stock'] = api.get_sector_allocation(info_dict=r)['stock']
                portfolio_info['sector_allocation_bond'] = api.get_sector_allocation(info_dict=r)['bond']
                portfolio_info['credit_allocation'] = api.get_credit_allocation(info_dict=r)
                portfolio_info['maturity_allocation'] = api.get_maturity_allocation(info_dict=r)
                portfolio_info['currency_allocation'] = api.get_currency_allocation(info_dict=r)

                full_dict = {}
                for key in portfolio_info:
                    sub_dict = {}

                    if key != 'full_holding':
                        if portfolio_info[key]:

                            if (len(portfolio_info[key]) > 2):
                                for mini_dict in portfolio_info[key]:
                                    sub_dict[mini_dict['Type']['value']] = mini_dict['value']
                            else:
                                sub_dict = portfolio_info[key]
                        else:
                            sub_dict = {}

                    else:
                        for mini_dict in portfolio_info[key]:
                            third_dict = {}
                            key_list = [key for key in mini_dict if key != 'value']
                            for sub_key in key_list:
                                third_dict[sub_key] = mini_dict[sub_key]['value']
                            sub_dict[third_dict['SecurityName']] = third_dict
                    full_dict[key] = sub_dict

                # data_json = json.dumps(full_dict)
                # data_df = pd.DataFrame.from_dict(portfolio_info, orient='index')
                # data_df.columns = [date]
            except:
                return {}

        return full_dict


    def get_combined_daily_data(self, performanceid=None, start_date=None, end_date=None):

        try:
            aum = api.get_daily_data(performanceid=performanceid,
                                 start_date=start_date,
                                 end_date=end_date,
                                 DataType='Valuation')[['SecID', 'PerformanceId', 'TNAClass', 'TNAFund', 'Shares']]
        except:
            aum = pd.DataFrame(columns = ['TNAClass', 'TNAFund', 'Shares'])

        try:
            nav = api.get_daily_data(performanceid=performanceid,
                                 start_date=start_date,
                                 end_date=end_date,
                                 DataType='PRICE')[['PreTaxNav']]
        except:
            nav = pd.DataFrame(columns=['PreTaxNav'])

        try:
            adjusted_nav = api.get_daily_data(performanceid=performanceid,
                                          start_date=start_date,
                                          end_date=end_date,
                                          DataType='rips')[['Unit_BAS']]
        except:
            adjusted_nav = pd.DataFrame(columns=['Unit_BAS'])

        # if len(nav) <= 2 * len(adjusted_nav):
        #     adjusted_nav = adjusted_nav.iloc[:int(len(adjusted_nav)/2), :]

        adjusted_nav = adjusted_nav[adjusted_nav.Unit_BAS <= 50]

        data_df = pd.concat([adjusted_nav, nav, aum], axis=1)
        # data_df = adjusted_nav
        data_df.index.name = 'Date'
        data_df = data_df.fillna(method='ffill')

        return data_df
    

    def download_portfolio_xml(self, masterportfolioid=None, date='2018-06-30'):
        
        headers = {'Content-type': self.CONTENT_TYPE}
        params = {'ClientId': self.CLIENT_ID,
                  'Package': self.PACKAGE,
                  'Id': masterportfolioid,
                  'IDTYpe': 'MasterPortfolioId',
                  'Date': date
                  }
        url = self.FUNDAMENTAL_URL
        with requests.Session() as session:
            r = session.post(url, params=params, headers=headers)

            try:
                content = parseString(r.content)

                with open('data/portfolio_xml/{}_{}.xml'.format(secid, date), 'wb+') as f:
                    f.write(content.toprettyxml(indent='\t', encoding='utf-8'))
            
            except Exception as e:
                print (e)
                pass



if __name__=="__main__":

    api = HistoricalAPI()
    mid = MasterID()

    id_df = mid.ID
    id_df.index = id_df['SecId']
    secid_list = id_df['SecId'].tolist()

    start_date = '2000-01-01'
    end_date = '2018-10-25'
    all_date_set = pd.date_range(start_date, end_date, freq='6M')
    all_date_set = [date.strftime("%Y-%m-%d") for date in all_date_set]

    download_flags = [False, False, True, False]

    all_list = pd.read_csv('cn_fund_full.csv')['SecId'].tolist()
    
    # for secid in all_list:
    #     if secid not in secid_list:
    #         print ('download xml file for fund: ' + secid)
    #         api.download_xml(secid = secid)
    
    if False:

        for secid in all_list:
            if secid not in secid_list:
                masterportfolioid = pd.read_csv('data/fundamental_info_new/' + secid + '.csv', index_col=0).loc['master_portid','info']

                if masterportfolioid:
                    for date in all_date_set:
                        print ('get portfolio xml for fund: ' + secid + ' on date: ' + date)
                        api.download_portfolio_xml(masterportfolioid, date)
                else:
                    print ('There is no portfolio data for fund: ' + secid )


    '''
    Download fundamental data
    '''
    if download_flags[0]:
        for secid in all_list:
            if secid not in secid_list:
                secid = 'F00000V88V'

                print ('get fundamental info for fund: ' + secid)
                fundamental_df = api.get_fundamental_info(secid=secid)
                fundamental_df.to_csv('data/fundamental_info_new/' + secid + '.csv')
                id_df.loc[secid, 'MasterPortfolioId'] = fundamental_df.loc['master_portid', 'info']

        id_df.to_csv('test.csv', index=False)

    holding_dict = {}
    if download_flags[1]:

        for secid in secid_list:

            secid = 'F00000Z0DG'
            masterportfolioid = pd.read_csv('data/fundamental_info/' + secid + '.csv', index_col=0).loc['master_portid','info']

            for date in all_date_set:
                date = '2018-06-30'
                print ('get portfolio info for fund: ' + secid + ' on date: ' + date)
                holding_dict[date] = api.get_portfolio_info(masterportfolioid=masterportfolioid, date=date)

            print ()
            with open('data/portfolio_data/' + secid + '.json', 'w', encoding='utf-8') as json_file:
                json.dump(holding_dict, json_file)

    id_df = pd.read_csv('cn_fund_full.csv')
    if download_flags[2]:
        for secid in ['F000000I6C']:
            # if secid not in secid_list:
            if True:
                performanceid = id_df[id_df.SecId == secid]['PerformanceId']
                print('get daily data for fund: ' + secid)
                daily_df = api.get_combined_daily_data(performanceid=performanceid, start_date=start_date, end_date=end_date)
                daily_df.to_csv('data/' + secid + '.csv')

    if download_flags[3]:

        for secid in secid_list:
            print('get current portfolio data for fund: ' + secid)
            dict = api.get_current_portfolio_info(secid=secid)

            with open('data/current_portfolio_data/' + secid + '.json', 'w', encoding='utf-8') as json_file:
                json.dump(dict, json_file)

    print()