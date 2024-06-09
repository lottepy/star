import json
import pandas as pd
from site_packages.xml2dict import XML2Dict

xml = XML2Dict()

live_list = pd.read_csv('cn_fund.csv')['SecId'].tolist()
all_list = pd.read_csv('cn_fund_full.csv')['SecId'].tolist()

basic_info = ['_BrandingId', '_Id', '_LegacyFamilyId', '_Status']

for secid in live_list:
    if secid not in all_list:
        all_list.append(secid)

warehouse = xml.parse('data/warehouse_xml/' + 'F00000ZWDC' + '.xml')['FundShareClass']['Fund']['FundManagement']['Registration']['CountryOfRegistration']
list_type = type(warehouse)

for secid in all_list:

    info_dict = {}
    print ('get some fundamental info for fund: ' + secid)

    try:
        warehouse = xml.parse('data/warehouse_xml/' + secid + '.xml')['FundShareClass']['Fund']['FundManagement']['Registration']['CountryOfRegistration']
        a = type(warehouse)
        if type(warehouse) == list_type:
            warehouse = warehouse[0]['Company']
        else:
            warehouse = warehouse['Company']

        for basic in basic_info:
            try:
                info_dict[basic] = warehouse[basic]['value']
            except:
                info_dict[basic] = None

        try:
            info_dict['CompanyNarratives'] = warehouse['CompanyNarratives']['Profile']['value']
        except:
            info_dict['CompanyNarratives'] = None

        basics = warehouse['CompanyOperation']['CompanyBasics']
        try:
            info_dict['Name'] = basics['Name']['value']
        except:
            info_dict['Name'] = None

        try:
            info_dict['CompanyType'] = basics['CompanyType']['Type']
        except:
            info_dict['CompanyType'] = None

        try:
            info_dict['InceptionDate'] = basics['InceptionDate']['value']
        except:
            info_dict['InceptionDate'] = None

        try:
            info_dict['BusinessType'] = warehouse['CompanyOperation']['BusinessTypeClassification']['BusinessType']
        except:
            info_dict['BusinessType'] = None
        # if warehouse[0]:
        #     for key in warehouse:
        #         ratio_dict[key['Date']['value']] = key['FeeAndExpense']['NetExpenseRatio']['value']
        # else:
        #     ratio_dict[warehouse['Date']['value']] = warehouse['FeeAndExpense']['NetExpenseRatio']['value']
    except Exception as e:
        info_dict = {}
        print (e)

    with open('data/company_info/' + secid + '.json', 'w', encoding='utf-8') as json_file:
        json.dump(info_dict, json_file)
