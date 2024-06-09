from lib.commonalgo.data.bbg_downloader import download_his,download_ref
import pandas as pd
import math

# tickers = ['MSEUACWF INDEX','LEGATRUU INDEX']
tickers = ['SHSZ300 Index','CFTBTRID Index']
# bm_ticker = ['MXWD INDEX', 'LEGATRUU INDEX', 'SPX INDEX', 'AWXUSRUS INDEX', 'SPBDU3NT INDEX', 'JHDCGBIG INDEX']
fields = ['PX_LAST']
#
# data = download_his(tickers,fields,'2015-01-01', '2018-10-29')
# print (data.head())
# data.to_csv('bm_china.csv')


# data = pd.read_csv('bm_parse.csv',index_col=0,parse_dates=True,infer_datetime_format=True).pct_change().fillna(0.)
# synetic = dict()
#
# for sb_ratio in range(20,100,20):
#     print(sb_ratio)
#     # adj_sb = math.sqrt(sb_ratio)*10
#     adj_sb = (sb_ratio-10)
#     temp = data['equity']*(adj_sb/100.) + data['bond']*(1-adj_sb/100.)
#     synetic[str(sb_ratio)] = (temp+1.).cumprod()
#
#
# result = pd.DataFrame(synetic)
# result.to_csv('bm_result.csv')


# bm = pd.read_csv('bm_result.csv',index_col=0,parse_dates=True, infer_datetime_format=True)
# port = pd.read_csv('SG_Max.csv',index_col=0,parse_dates=True, infer_datetime_format=True)
#
# data = port.join(bm.add_prefix('bm_'),how='right')
# data = data.fillna(method='ffill').fillna(method='bfill')
# data_norm = (data.pct_change().fillna(0)+1.).cumprod()
# data_norm.to_csv('us_max_bm.csv')

# data = pd.read_csv('bm_parse.csv',index_col=0,parse_dates=True,infer_datetime_format=True).pct_change().fillna(0.)
# synetic = dict()
#
# for sb_ratio in [38.4,57.6,76.8]:
#     print(sb_ratio)
#     # adj_sb = math.sqrt(sb_ratio)*10
#     adj_sb = (sb_ratio-10)
#     temp = data['equity']*(adj_sb/100.) + data['bond']*(1-adj_sb/100.)
#     synetic[str(sb_ratio)] = (temp+1.).cumprod()
#
#
# result = pd.DataFrame(synetic)
# result.to_csv('bm_result_mini.csv')

# bm = pd.read_csv('bm_result_mini.csv',index_col=0,parse_dates=True, infer_datetime_format=True)
# port = pd.read_csv('SGMini_raw.csv',index_col=0,parse_dates=True, infer_datetime_format=True)
# port.index = port.index.normalize()
#
# data = port.join(bm.add_prefix('bm_'),how='right')
# data = data.fillna(method='ffill').fillna(method='bfill')
# data_norm = (data.pct_change().fillna(0)+1.).cumprod()
# data_norm.to_csv('us_min_bm.csv')

data = pd.read_csv('bm_china.csv',index_col=0,parse_dates=True,infer_datetime_format=True).pct_change().fillna(0.)
synetic = dict()

for sb_ratio in range(20,100,20):
    print(sb_ratio)
    # adj_sb = math.sqrt(sb_ratio)*10
    adj_sb = (sb_ratio-5)
    temp = data['equity']*(adj_sb/100.) + data['bond']*(1-adj_sb/100.)
    synetic[str(sb_ratio)] = (temp+1.).cumprod()


result = pd.DataFrame(synetic)
result.to_csv('bm_result_xray.csv')

bm = pd.read_csv('bm_result_xray.csv',index_col=0,parse_dates=True, infer_datetime_format=True)
port = pd.read_csv('huarun.csv',index_col=0,parse_dates=True, infer_datetime_format=True)
port.index = port.index.normalize()

data = port.join(bm.add_prefix('bm_'),how='right')
data = data.fillna(method='ffill').fillna(method='bfill')
data_norm = (data.pct_change().fillna(0)+1.).cumprod()
data_norm.to_csv('cn_huarun_bm.csv')

