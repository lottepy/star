from lib.commonalgo.zipline_patch.markort import get_data

# iuids = ['HK_60_MHIU8']
# iuids = ['US_10_INDA']
# for i in range(0,100):
# 	data = get_data(iuids, [4], 'D', start_date ='2000-1-1', end_date = '2018-08-26', tz_region=None, currency=None, adjust_type=0, fill='nan', precision=4,prelisting_fill=False)
# 	data_adjust = get_data(iuids, [4], 'D', start_date ='2000-1-1', end_date ='2018-8-26', tz_region=None, currency=None, adjust_type=1, fill='nan', precision=4,prelisting_fill=True)
# data.to_csv("MSCI.csv")

iuids = ['HK_20_F000001G43',"HK_20_F00000XAFD"]
# data = get_data(iuids, [4,50,52], 'D', start_date ='2000-1-1', end_date = '2019-04-30', tz_region=None, currency=None, adjust_type=0, fill='nan', precision=4,prelisting_fill=False)
data = get_data(iuids=['HK_10_27'],tags=[4], period='D', start_date='2017-01-01', end_date='2019-07-22',adjust_type=0,fill='nan')

print(data)
