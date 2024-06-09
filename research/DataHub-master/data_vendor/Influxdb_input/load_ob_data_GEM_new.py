# coding=utf-8
from multiprocessing.pool import ThreadPool
import os
import csv
import time
import traceback
from influxdb import InfluxDBClient

client = InfluxDBClient(host='47.75.164.89', username='admin', password='aqumon2050', database='market_data')
pool = ThreadPool(16)
measurement = 'orderbook.cn.10'


def func(filename, iuid, date_str):
    y, m, d = int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8])
    with open(filename, 'r', encoding='gb18030') as csvfile:
        try:
            reader = csv.reader(csvfile)
            data = []
            next(reader)
            for row in reader:
                _, time_str, exec_price, volume, total_volume, amount, b1, bv1, b2, bv2, b3, bv3, \
                b4, bv4, b5, bv5, a1, av1, a2, av2, a3, av3, a4, av4, a5, av5, buysell = row
                hh, mm, ss = map(int, time_str.split(':'))
                ts = int(time.mktime((y, m, d, hh, mm, ss, 0, 0, 0)) * 1000)

                tag_set = ','.join([measurement, 'iuid={}'.format(iuid)])
                field_set = f'b1={b1},bv1={bv1},b2={b2},bv2={bv2},b3={b3},bv3={bv3},b4={b4},bv4={bv4},' \
                            f'b5={b5},bv5={bv5},a1={a1},av1={av1},a2={a2},av2={av2},a3={a3},av3={av3},' \
                            f'a4={a4},av4={av4},a5={a5},av5={av5}'
                line = ' '.join([tag_set, field_set, str(ts)])
                data.append(line)
        except:
            traceback.print_stack()
            print('error parsing file')
    try:
        client.write_points(data, time_precision='ms', batch_size=5000, protocol='line')
    except:
        print('error write points 1')
        try:
            client.write_points(data, time_precision='ms', batch_size=5000, protocol='line')
        except:
            print('error write points 2')

    print('uploaded {} points from {}'.format(len(data), filename))


def get_iuid(region, ticker):
    if (region == 'SH' and ticker.startswith('0')) or (region == 'SZ' and ticker.startswith('39')):
        return 'CN_30_{}'.format(ticker)
    return 'CN_10_{}'.format(ticker)


def get_white_list(year, region):
    white_list = []
    for i in range(1, 12):
        month = "%02d" % i
        white_list.append('DZH-' + region + year + month + '-TXT')
    return white_list


# root_paths = [
#     '//192.168.9.120/nfs_share/_Ashare_HFTdata/分笔委托/stock_orderbook_2018/'
# ]

root_paths = [
    u'/mnt/nas/_Ashare_HFTdata/orderbook/stock_orderbook_2018'  #//linux网盘路径
    # u'/mnt/nas/_Ashare_HFTdata/orderbook/stock_orderbook_2017',
    # u'/mnt/nas/_Ashare_HFTdata/orderbook/stock_orderbook_2016',
]


# root_paths = ['/Users/lilychen/Desktop/AQUMON/GEM/分笔委托/stock_orderbook_2018']  #//本地路径
# root_paths = ['/Volumes/nfs_share/_Ashare_HFTdata/orderbook/stock_orderbook_2018']
               # '/Users/lilychen/Desktop/AQUMON/GEM/分笔委托/stock_orderbook_2018']  #//mac网盘路径

for root_path in root_paths:
    list_region = get_white_list('2018', 'SZ')
    for region_value in list_region:
        region = region_value.split('-')[1][:2]
        second_root = root_path + '/' + region_value
        list_date = os.listdir(second_root)
        for date_value in list_date:
            if 'DS_Store' in date_value:
                continue
            third_root = second_root + '/' + date_value
            print(third_root)
            list_file = os.listdir(third_root)
            for file_value in list_file:
                if file_value[0] != '3':
                    continue
                ticker_code = file_value[:6]
                if ticker_code.startswith('1') or ticker_code.startswith('2'):
                    continue
                iuid = get_iuid(region, ticker_code)

                try:
                    pool.apply_async(func, args=(os.path.join(third_root, file_value), iuid, date_value))
                except:
                    print(third_root)


pool.close()
pool.join()
