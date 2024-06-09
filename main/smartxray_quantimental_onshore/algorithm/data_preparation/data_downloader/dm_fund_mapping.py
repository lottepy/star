from algorithm import addpath
from datetime import datetime, timedelta
import pandas as pd
import math, time, os
from datamaster import dm_client
dm_client.refresh_config()
dm_client.start()

def manager_mapping():
    write_out_path = os.path.join(addpath.historical_path, 'fund_manager')
    if not os.path.exists(write_out_path):
        os.makedirs(write_out_path)

    secid_list = pd.read_csv(addpath.ref_path, index_col=0)
    secid_list = secid_list['ms_secid'].tolist()

    mapping = pd.DataFrame()
    batch_size = 50
    error_list = []
    for n in range(math.ceil((len(secid_list) / batch_size))):
        sub_secid_list = secid_list[n * batch_size:min(n * batch_size + batch_size, len(secid_list))]
        time.sleep(0.5)
        try:
            res = dm_client.instrument_fund_data(symbols=sub_secid_list, fields='fund_manager_info')
            for secid in sub_secid_list:
                mapping_list = res['values'][secid]
                mapping_tmp = pd.DataFrame(mapping_list)
                mapping_tmp.index = [secid] * len(mapping_list)
                mapping_tmp.columns = res['fields']
                mapping = pd.concat([mapping, mapping_tmp])
        except Exception as e1:
            for secid in sub_secid_list:
                try:
                    res_er = dm_client.instrument_fund_data(symbols=secid, fields='fund_manager_info')

                    mapping_list_er = res_er['values'][secid]
                    mapping_tmp_er = pd.DataFrame(mapping_list_er)
                    mapping_tmp_er.index = [secid] * len(mapping_list_er)
                    mapping_tmp_er.columns = res_er['fields']
                    mapping = pd.concat([mapping, mapping_tmp_er])
                except Exception as e2:
                    print("**** Fail to download %s " % secid)
                    print(e2)
                    error_list.append(secid)
                    continue
        print("%s: %i has downloaded!" % (str(datetime.now()), (n + 1) * 50))

    mapping.to_csv(os.path.join(write_out_path, 'manager_mapping_raw.csv'), encoding='utf-8-sig')
    error_df = pd.DataFrame(error_list)
    error_df.to_csv(os.path.join(write_out_path, 'manager_mapping_missing.csv'), encoding='utf-8-sig')


def company_mapping():
    write_out_path = os.path.join(addpath.historical_path, 'fund_company')
    if not os.path.exists(write_out_path):
        os.makedirs(write_out_path)

    secid_list = pd.read_csv(addpath.ref_path, index_col=0)['ms_secid'].tolist()
    mapping = pd.DataFrame()

    batch_size = 50
    error_list = []
    for n in range(math.ceil((len(secid_list) / batch_size))):
        time.sleep(1)
        sub_secid_list = secid_list[n * batch_size:min(n * batch_size + batch_size, len(secid_list))]
        try:
            res_provider = dm_client.instrument_fund_data(symbols=sub_secid_list, fields='provider_company_info')
            for secid in sub_secid_list:
                mapping_provider_ls = res_provider['values'][secid][0]
                mapping_tmp = pd.DataFrame(mapping_provider_ls).T
                mapping_tmp.index = [secid]
                mapping_tmp.columns = res_provider['fields']
                mapping = pd.concat([mapping, mapping_tmp])
        except Exception as e1:
            # print("***********")
            # print(sub_secid_list)
            # print(e1)
            for secid in sub_secid_list:
                try:
                    res_provider = dm_client.instrument_fund_data(symbols=secid, fields='provider_company_info')
                    mapping_provider_ls = res_provider['values'][secid][0]
                    mapping_tmp = pd.DataFrame(mapping_provider_ls).T
                    mapping_tmp.index = [secid]
                    mapping_tmp.columns = res_provider['fields']
                    mapping = pd.concat([mapping, mapping_tmp])
                except Exception as e2:
                    print("**** Fail to download %s " % secid)
                    print(e2)
                    error_list.append(secid)
                    continue
        print("%s: %i has downloaded!" % (str(datetime.now()), (n + 1) * 50))

    mapping.to_csv(os.path.join(write_out_path, 'company_mapping_raw.csv'), encoding='utf-8-sig')
    error_df = pd.DataFrame(error_list)
    error_df.to_csv(os.path.join(write_out_path, 'company_mapping_missing.csv'), encoding='utf-8-sig')


def redownload_missings(field):
    read_path = os.path.join(addpath.historical_path, field, '{}_mapping_missing.csv'.format(field[field.index('_')+1:]))
    missing_list = pd.read_csv(read_path)['0'].tolist()
    missing_path = os.path.join(addpath.historical_path, field, '{}_mapping_missing_2.csv'.format(field[field.index('_')+1:]))
    save_path = os.path.join(addpath.historical_path, field, '{}_mapping_raw_2.csv'.format(field[field.index('_')+1:]))
    if field == 'fund_company':
        dm_field = 'provider_company_info'
    else:
        dm_field = 'fund_manager_info'

    error_list = []
    mapping = pd.DataFrame()
    for secid in missing_list:
        time.sleep(0.01)
        try:
            res = dm_client.instrument_fund_data(symbols=secid, fields=dm_field)
            mapping_provider_ls = res['values'][secid][0]
            mapping_tmp = pd.DataFrame(mapping_provider_ls).T
            mapping_tmp.index = [secid]
            mapping_tmp.columns = res['fields']
            mapping = pd.concat([mapping, mapping_tmp])
        except:
            print('Failed {}'.format(secid))
            error_list.append(secid)
            continue
    error_df = pd.DataFrame(error_list)
    error_df.to_csv(missing_path, encoding='utf-8')
    mapping.to_csv(save_path, encoding='utf-8')

if __name__ == '__main__':
    company_mapping()
    # redownload_missings(field='fund_company')
    manager_mapping()
    # redownload_missings(field='fund_manager')
