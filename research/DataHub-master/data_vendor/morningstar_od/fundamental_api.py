import pandas as pd
import urllib.parse
import requests
import xml.etree.ElementTree as tree
import json
from base_api import BaseAPI
from base_api import MasterID
from config import FUNDAMENTAL_BASE_URL
from config import XPATH

class FundamentalAPI(BaseAPI):

    FUNDAMENTAL_BASE_URL = FUNDAMENTAL_BASE_URL

    def __init__(self, secid):
        super().__init__()
        self.FUNDAMENTAL_URL = urllib.parse.urljoin(self.BASE_URL, self.FUNDAMENTAL_BASE_URL)
        self.reset_secid(secid=secid)

    def reset_secid(self, secid=None):
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
            content = tree.fromstring(r.content)
            self.content = content

    def get_inception_date(self, secid=None):
        return self._get_xpath_text(secid=secid, xpath=XPATH.INCEPTION_DATE.value)

    def get_manager_list(self, secid=None):
        """
        Get historical manager details.
        :param secid:
        :return: DataFrame, including managers' names, education, start and end date, etc
        """
        content = self.content
        if secid is not None:
            content = self.reset_secid(secid=secid)
        tree = self.get_tree_element(tree=content,
                                     xpath='/FundShareClass/Fund/FundManagement/ManagerList/'
        )

        # There are plenty information in ManagerList, therefore we return a dataframe to fully contain all information.
        df = pd.DataFrame()
        dict = {}
        try:
            for n, manager_detail in enumerate(tree.getchildren()):
                detail = {}
                for field in manager_detail.getchildren():
                    if field.tag in ('StartDate', 'EndDate'):
                        detail[field.tag] = field.text
                    elif field.tag in ('ProfessionalInformation'):
                        detail['GivenName'] = self._safe_return(self.get_tree_element(field, XPATH.MANAGER_GIVEN_NAME.value))
                        detail['MiddleName'] = self._safe_return(self.get_tree_element(field, XPATH.MANAGER_MIDDLE_NAME.value))
                        detail['FamilyName'] = self._safe_return(self.get_tree_element(field, XPATH.MANAGER_FAMILY_NAME.value))
                        detail['CareerStartYear'] = self._safe_return(self.get_tree_element(field, XPATH.MANAGER_CAREER_START_YEAR.value))
                        detail['College'] = self._safe_return(self.get_tree_element(field, XPATH.MANAGER_COLLEGE.value))
                        detail['Biograhy'] = self._safe_return(self.get_tree_element(field, XPATH.MANAGER_BIOGRAPHY.value))
                dict[n] = detail
                df = pd.concat([df, pd.DataFrame(detail, index=[n])], axis=0)
                df = df.sort_values(by='StartDate', )
        except:
            return pd.DataFrame(), {}

        return df, dict

    def get_investment_obj(self, secid=None):
        return self._get_xpath_text(secid=secid, xpath=XPATH.INVESTMENT_OBJ.value)

    def get_gifs_code(self, secid=None):
        return self._get_xpath_text(secid=secid, xpath=XPATH.GIFS_CODE.value)

    def get_mpt_benchmark(self, secid=None):
        return self._get_xpath_text(secid=secid, xpath=XPATH.MPT_BENCHMARK.value)

    def get_primary_benchmark(self, secid=None):
        return self._get_xpath_text(secid=secid, xpath=XPATH.PRIMARY_BENCHMARK.value)

    def get_secondary_benchmark(self, secid=None):
        return self._get_xpath_text(secid=secid, xpath=XPATH.SECONDARY_BENCHMARK.value)

    def get_expense_ratio(self, secid=None):
        return self._get_xpath_text(secid=secid, xpath=XPATH.NET_EXPENSERATIO.value)

    def get_management_fee(self, content=None, secid=None):
        return self._get_xpath_text(secid=secid, xpath=XPATH.MANAGEMENT_FEE.value)

    def get_turnover_rate(self, secid=None):
        return self._get_xpath_text(secid=secid, xpath=XPATH.TURNOVER_RATIO.value)

    def get_historical_fund_owner(self, secid=None):
        return self._get_xpath_text(secid=secid, xpath=XPATH.FUND_OWNER.value)

    def get_credit_quality(self, secid=None):
        return self._get_xpath_text(secid=secid, xpath=XPATH.CREDIT_QUALITY.value)

    def get_duration(self, secid=None):
        return self._get_xpath_text(secid=secid, xpath=XPATH.DURATION.value)

    def get_yield_to_maturity(self, secid=None):
        return self._get_xpath_text(secid=secid, xpath=XPATH.YIELD_TO_MATURITY.value)

    def get_maturity_date(self, secid=None):
        return self._get_xpath_text(secid=secid, xpath=XPATH.MATURITY_DATE.value)



if __name__=="__main__":
    print("Test Fundamental API")
    mid = MasterID()

    # isin = 'IE00B11XYX59' # 'LU0106261372', 'IE00B11XYX59'
    # secid = mid.from_isin_to_secid(isin = isin)

    id_df = mid.ID
    secid_list = id_df['SecId'].tolist()

    all_list = pd.read_csv('cn_fund_full.csv')['SecId'].tolist()

    for secid in all_list[626:]:
        if secid not in secid_list:
            print ('get fundamental info for fund: ' + secid)
            info = {}
            api = FundamentalAPI(secid = secid)
        # tic = time.time()
            info['inception_date'] = api.get_inception_date()
            info['manager_list'], dict = api.get_manager_list()
            # info['investment_obj'] = api.get_investment_obj()
            info['gifs_code'] = api.get_gifs_code()
            info['mpt_benchmark'] = api.get_mpt_benchmark()
            info['primary_benchmark'] = api.get_primary_benchmark()
            info['secondary_benchamrk'] = api.get_secondary_benchmark()
            info['expense_ratio'] = api.get_expense_ratio()
            info['management_fee'] = api.get_management_fee()
            info['turnover_rate'] = api.get_turnover_rate()
            info['credit_quality'] = api.get_credit_quality()
            info['duration'] = api.get_duration()
            info['yield_to_maturity'] = api.get_yield_to_maturity()
            info['maturity_date'] = api.get_maturity_date()
            # info['fund_owner'] = api.get_historical_fund_owner()

            fundamental_df = pd.DataFrame.from_dict(info, orient='index')
            fundamental_df.columns = ['info']
            fundamental_df.to_csv('data/fundamental_data_csv/' + secid + '.csv')

            info['manager_list'] = dict
            with open('data/fundamental_data_json/' + secid + '.json', 'w', encoding='utf-8') as json_file:
                    json.dump(info, json_file)

    # csdcc = '206015' # '110011', '206015'
    # secid = mid.from_csdcc_to_secid(csdcc='110011')
    # api = FundamentalAPI(secid = secid)
    # tic = time.time()
    # print(api.get_inception_date())
    # print(api.get_manager_list())
    # print(api.get_investment_obj())
    # print(api.get_gifs_code())
    # print(api.get_mpt_benchmark())
    # print(api.get_primary_benchmark())
    # print(api.get_secondary_benchmark())
    # toc = time.time()
    # print("Elapsed time is {:.2f} seconds.".format(toc-tic))

    print()