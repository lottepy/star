import os
daily_data_end_point = 'https://aqm-algo.oss-cn-hongkong.aliyuncs.com/0_DymonFx/daily_data/'
intraday_data_end_point = 'https://aqm-algo.oss-cn-hongkong.aliyuncs.com/0_DymonFx/'



spot_list = [
    'NZDJPY', 'NOKSEK', 'GBPJPY', 'GBPCHF', 'GBPCAD',
    'EURJPY', 'EURGBP', 'EURCHF', 'EURCAD', 'CADJPY',
    'CADCHF', 'AUDNZD', 'AUDJPY', 'AUDCAD', 'USDSEK',
    'USDNOK', 'USDJPY', 'USDCHF', 'USDCAD', 'NZDUSD',
    'GBPUSD', 'EURUSD', 'AUDUSD', 'USDZAR', 'USDSGD',
    'USDTHB'
]

ndf_ticker = ['USDKRW', 'USDTWD', 'USDINR', 'USDIDR']




spot_BGNL_list = [os.path.join(daily_data_end_point,'spot_BGNL/',i + '%20BGNL%20Curncy.csv') for i in spot_list]
# spot_BGNL_list = dict(zip(spot_list,spot_BGNL_list))
spot_T150_list = [os.path.join(daily_data_end_point,'spot_T150/',i + '%20T150%20Curncy.csv') for i in spot_list]
# spot_T143_list = [os.path.join(daily_data_end_point,'spot_T143/',i + '%20T143%20Curncy.csv') for i in spot_list]

# spot_T150_list = dict(zip(spot_list,spot_T150_list))
# spot_TN_list = [os.path.join(daily_data_end_point,'spot_TN/',i.replace('USD', '') + 'TN%20BGNL%20Curncy.csv') for i in spot_list]
# spot_TN_list = dict(zip(spot_list,spot_TN_list))

ndf_BGNL_list = [
    'NTN+1M BGNL Curncy.csv',
    'KWN+1M BGNL Curncy.csv',
    'IRN+1M BGNL Curncy.csv',
    'IHN+1M BGNL Curncy.csv'
]
ndf_BGNL_list = [os.path.join(daily_data_end_point,'ndf_BGNL/',i.replace('+','%2B').replace(' ','%20')) for i in ndf_BGNL_list]
ndf_T150_list = [
    'NTN+1M T150 Curncy.csv',
    'KWN+1M T150 Curncy.csv',
    'IRN+1M T150 Curncy.csv',
    'IHN+1M T150 Curncy.csv'
]
ndf_T150_list = [os.path.join(daily_data_end_point,'ndf_T150/',i.replace('+','%2B').replace(' ','%20')) for i in ndf_T150_list]
# ndf_T143_list = [
#     'NTN+1M T143 Curncy.csv',
#     'KWN+1M T143 Curncy.csv',
#     'IRN+1M T143 Curncy.csv',
#     'IHN+1M T143 Curncy.csv'
# ]
# ndf_T143_list = [os.path.join(daily_data_end_point,'ndf_T143/',i.replace('+','%2B').replace(' ','%20')) for i in ndf_T143_list]

ndf_KWN1M_list = [
    'NTN1M BGNL Curncy.csv',
    'KWN1M BGNL Curncy.csv',
    'IRN1M BGNL Curncy.csv',
    'IHN1M BGNL Curncy.csv'
]
ndf_KWN1M_list = [os.path.join(daily_data_end_point,'ndf_KWN1M/',i.replace(' ','%20')) for i in ndf_KWN1M_list]

USDR1T_list = ['USDR1T CMPL Curncy.csv','USDR1T CMP Curncy.csv']
USDR1T_list = [os.path.join(daily_data_end_point,'USDR1T',i.replace(' ','%20')) for i in USDR1T_list]

# temp_intraday_list = [os.path.join(intraday_data_end_point,'temp_intraday_data/',i+'.csv') for i in spot_list + ndf_ticker]

file_mapping_dict={
    #'spot_BGNL' : spot_BGNL_list,
    'spot_T150' : spot_T150_list,
    # 'spot_T143' : spot_T143_list,
    #'spot_TN' : spot_TN_list,
    #'ndf_BGNL' : ndf_BGNL_list,
    'ndf_T150' : ndf_T150_list,
    # 'ndf_T143': ndf_T143_list,
    #'ndf_KWN1M' : ndf_KWN1M_list,
    #'USDR1T' : USDR1T_list
    # 'temp_intraday_data': temp_intraday_list,
}


#存放路径
temp_root_path = 'temp/'
# temp_spot_BGNL_save_path = temp_root_path + 'spot_BGNL'
temp_spot_T150_save_path = temp_root_path + 'spot_T150'
# temp_spot_T143_save_path = temp_root_path + 'spot_T143'
# temp_spot_TN_save_path = temp_root_path + 'spot_TN'
# temp_ndf_BGNL_save_path = temp_root_path + 'ndf_BGNL'
temp_ndf_T150_save_path = temp_root_path + 'ndf_T150'
# temp_ndf_T143_save_path = temp_root_path + 'ndf_T143'
# temp_ndf_KWN1M_save_path = temp_root_path + 'ndf_KWN1M'
# temp_USDR1T_save_path = temp_root_path + 'USDR1T'
# temp_intraday_data = temp_root_path + 'temp_intraday_data'


temp_file_mapping_dict={
    # 'spot_BGNL' : temp_spot_BGNL_save_path,
    'spot_T150' : temp_spot_T150_save_path,
    # 'spot_T143': temp_spot_T143_save_path,
    # 'spot_TN' : temp_spot_TN_save_path,
    # 'ndf_BGNL' : temp_ndf_BGNL_save_path,
    'ndf_T150' : temp_ndf_T150_save_path,
    # 'ndf_T143': temp_ndf_T143_save_path,
    # 'ndf_KWN1M' : temp_ndf_KWN1M_save_path,
    # 'USDR1T' : temp_USDR1T_save_path
    # 'temp_intraday_data': temp_intraday_data
}
T150_dict={
    'spot_T150' : temp_spot_T150_save_path,
    'ndf_T150' : temp_ndf_T150_save_path,
}
# T143_dict={
#     'spot_T143': temp_spot_T143_save_path,
#     'ndf_T143': temp_ndf_T143_save_path,
# }
# intraday_dict={
#     'temp_intraday_data': temp_intraday_data
# }
ndf_name_mapping = {
    'NTN+1M':'USDTWD',
    'KWN+1M':'USDKRW',
    'IRN+1M':'USDINR',
    'IHN+1M':'USDIDR',
}

RIC_spot_list = ['DY_FX_'+x.replace('USD','')+'=' for x in spot_list]
RIC_spot_list = dict(zip(spot_list,RIC_spot_list))
RIC_ndf_list = {
    'USDKRW':'DY_FX_KRW=X',
    'USDTWD':'DY_FX_TWD=X',
    'USDINR':'DY_FX_INR=X',
    'USDIDR':'DY_FX_IDR=X'
}
RIC_list = RIC_spot_list
RIC_spot_list.update(RIC_ndf_list)


