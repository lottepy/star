import pandas as pd
import numpy as np

from os.path import join

from algorithm import addpath

def generate_bundle(datain, dest, name_list):
    for j in range(datain.shape[1]):
        output = pd.DataFrame([datain.iloc[:,j]]*4).T
        output.columns = ['open', 'high', 'low', 'close']
        output['volume'] = 100
        output.to_csv(os.path.join(dest, name_list[j]))
    return

def generate_fxrate(datain, dest, name_list):
    for j in range(datain.shape[1]):
        output = pd.DataFrame(1, index=datain.index, columns=['fx_rate'])
        output.to_csv(os.path.join(dest, name_list[j]))
    return

def generate_interest(datain, dest, name_list):
    for j in range(datain.shape[1]):
        output = pd.DataFrame(0, index=datain.index, columns=['interest_rate'])
        output.to_csv(os.path.join(dest, name_list[j]))
    return
   
def generate_ref(datain, dest, name_list, ref_name):
    for j in range(datain.shape[1]):
        output = pd.DataFrame(datain.iloc[:,j], index=datain.index)
        output.columns = [ref_name]
        output.to_csv(os.path.join(dest, name_list[j]))
    return

def update_smart_beta_result():
    cn_combined_pv = pd.read_csv(join(addpath.result_path, 'smart_beta', 'CN_combined', 'portfolio value.csv'), index_col=[0], parse_dates=[0], header=None)
    hk_combined_pv = pd.read_csv(join(addpath.result_path, 'smart_beta', 'HK_combined', 'portfolio value.csv'), index_col=[0], parse_dates=[0], header=None)
    us_combined_pv = pd.read_csv(join(addpath.result_path, 'smart_beta', 'US_combined', 'portfolio value.csv'), index_col=[0], parse_dates=[0], header=None)
    smart_beta_pv = pd.concat([cn_combined_pv, hk_combined_pv, us_combined_pv], axis=1)
    name_list = ['No_ETF_CN_combined.csv', 'No_ETF_HK_combined.csv', 'No_ETF_US_combined.csv']
    generate_bundle(smart_beta_pv, join(addpath.data_path, 'bundles'), name_list)
    generate_fxrate(smart_beta_pv, join(addpath.data_path, 'fxrate'), name_list)
    generate_interest(smart_beta_pv, join(addpath.data_path, 'interest'), name_list)

def update_sr_result():
    sr_pv = pd.read_csv(join(addpath.result_path, 'smart_rotation_21', 'portfolio value.csv'), index_col=[0], parse_dates=[0])
    sr_pv.index = sr_pv.index.tz_localize(None).normalize()

    name_list = ['Smart_Rotation.csv']
    generate_bundle(sr_pv, join(addpath.data_path, 'bundles'), name_list)
    generate_fxrate(sr_pv, join(addpath.data_path, 'fxrate'), name_list)
    generate_interest(sr_pv, join(addpath.data_path, 'interest'), name_list)

    sr_mkt_cap = pd.DataFrame(1, index=sr_pv.index, columns=['mktcap'])
    generate_ref(sr_mkt_cap, join(addpath.data_path, 'reference_data', 'mktcap'), name_list, 'mktcap')


