from lib.commonalgo.data.oss_client import OSS_Client

oss = OSS_Client()

local_path = '/Users/Ryan/Downloads/NAV.csv'
local_path2 = '/Users/Ryan/Downloads/NAV2.csv'
oss_key = 'Strategy_Kit/temp.csv'
oss.upload_file(local_path, oss_key)
oss.download_file(local_path2,oss_key)