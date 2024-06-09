import pandas as pd
import numpy as np
import asyncio
import errno
import os
import ssl
from multiprocessing import Pool, cpu_count
ssl._create_default_https_context = ssl._create_unverified_context
from datetime import datetime,date
n_cpus = cpu_count()
from utils.fx_client_lib.config import file_mapping_dict,ndf_name_mapping
from utils.fx_client_lib.config import T150_dict, RIC_list

from utils.fx_client_lib.RQClient import RQClient
import time

def make_sure_path_exists(p):
    #    p = os.path.dirname(path)
    try:
        os.makedirs(p)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise

def get_reuters_data(iuid_list):
    data = {}
    def orderbook_callback(msg):
        data["orderbook|"+msg['iuid']]=msg
    def tick_callback(msg):
        data["tick|"+msg['iuid']]=msg
    def mega_callback(msg):
        data["mega|"+msg['iuid']]=msg
        # print(msg)
    rq = RQClient(iuid_list=iuid_list,snapshot_timeout=60000000, snapshot_refresh_interval=10
                        ,orderbook_callback=orderbook_callback,tick_callback=tick_callback,metadata_callback=mega_callback)
    while len(data)!=3*len(iuid_list):
        time.sleep(.5)
    rq.disconnect_client()
    #将data转换成df或者dict的形式

    return_dict = {}
    for key, value in RIC_list.items():
        PRICE_ASK = data['orderbook|' + RIC_list[key]]['a1']
        PRICE_BID = data['orderbook|' + RIC_list[key]]['b1']
        PRICE_LAST = data['tick|' + RIC_list[key]]['last_fx']
        return_dict[key] = {'PRICE_ASK': PRICE_ASK, 'PRICE_BID': PRICE_BID, 'PRICE_LAST': PRICE_LAST}

    return return_dict

def download_oss_data_by_url(url):
    df = pd.read_csv(url)
    save_name = os.path.join('temp', url.split('/')[-2])
    make_sure_path_exists(save_name)
    save_name = os.path.join(save_name,url.split('/')[-1].replace('%20',' '))
    df.to_csv(save_name,index =None)
    # print(url)

def download_oss_data_multi(url_list):
    p = Pool(n_cpus * 2)
    # p = Pool(1)
    dummy_var_to_ensure_result = p.map(download_oss_data_by_url, url_list)
    p.terminate()
    p = None

def get_oss_data(ticker_list,start_date,end_date):
    #先将文件下到temp中

    url_list = []
    for url in file_mapping_dict.values():
        url_list = url_list + url
    download_oss_data_multi(url_list)
    #读取temp中的csv
    #dict_df用于返回 来记录T150
    dict_df = {}
    for key,value in T150_dict.items():
        for i in os.listdir(value):
            value.split('/')[-1]
            save_name = value.split('/')[-1] + ' ' + i.split('.')[-2]
            df_ticker = pd.read_csv(os.path.join(value,i),index_col=0)
            df_ticker = df_ticker.loc[start_date:end_date]
            df_ticker = df_ticker [['PRICE_LAST','PRICE_BID','PRICE_ASK']]

            ticker_name = i.split('.')[-2].split(' ')[0].replace('%2B','+')
            if ticker_name in ndf_name_mapping:
                ticker_name = ndf_name_mapping[ticker_name]
            dict_df[ticker_name] = df_ticker
    #reuters的data
    reuters_list = list(RIC_list.values())
    reuters_data = get_reuters_data(reuters_list)
    #在T1450的时候取不到T150的数据，所以就用intraday的数据的最新的数据来补充
    for key, value in dict_df.items():
        # T143_df = dict_T143[key]
        # intraday_df = dict_intraday[key]
        df_ticker = dict_df[key]
        #如果取出的时间早于最后一天前的date，默认T150的数据是持续到前一天（每天数据会更新）。
        #if end_date < df_ticker.index[-1]:
        if end_date < date.today().strftime("%Y-%m-%d"):
            #前面在取数据的时候已经根据start_date和end_date进行了取，如果在df_ticker之前，则跳过拼接的步骤
            continue
        else:
            #采用路透的streaming data更新最后一天的T150数据
            date_index = date.today().strftime("%Y-%m-%d")
            df_ticker_reuters = pd.DataFrame(reuters_data[key],index = [date_index])
            dict_df[key] = pd.concat([df_ticker, df_ticker_reuters])

    df_all = pd.DataFrame()
    for ticker in ticker_list:
        if ticker not in dict_df:
            print('ticker: {} is not in data'.format(ticker))
        else:
            df_ticker = dict_df[ticker]
            df_all = pd.concat([df_all, df_ticker], axis=1)
    #df2 reshape
    df2array = df_all.fillna(method='ffill').values
    df2array = df2array.reshape(df2array.shape[0],int(df2array.shape[1]/3),3)
    # time
    time_df = pd.DataFrame({'date':pd.to_datetime(df_all.index, format='%Y-%m-%d')})
    time_df['timestamp'] = time_df['date'].apply(lambda x: datetime.timestamp(x))
    time_df['weekday'] = time_df['date'].apply(lambda x: x.weekday())
    time_df['year'] = time_df['date'].apply(lambda x: x.year)
    time_df['month'] = time_df['date'].apply(lambda x: x.month)
    time_df['day'] = time_df['date'].apply(lambda x: x.day)
    timeArray = time_df[['timestamp', 'weekday', 'year', 'month', 'day']].values
    return df2array,timeArray

