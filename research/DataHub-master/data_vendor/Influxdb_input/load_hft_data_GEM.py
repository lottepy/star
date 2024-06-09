from multiprocessing.pool import ThreadPool
import os
import csv
import time
import traceback
from influxdb import InfluxDBClient

client = InfluxDBClient(host='47.75.164.89', username='admin', password='aqumon2050', database='market_data')
pool = ThreadPool(16)
measurement = 'executions.cn.10'


def func(filename, iuid, date_str):
    y, m, d = int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8])
    with open(filename, 'r') as csvfile:
        try:
            reader = csv.reader(csvfile)
            last_time_str = None
            dup_counter = 0
            data = []
            for row in reader:
                time_str = row[0]
                if last_time_str and last_time_str == time_str:
                    dup_counter += 1
                else:
                    hh, mm, ss = int(time_str[:-4]), int(time_str[-4:-2]), int(time_str[-2:])
                    ts = int(time.mktime((y, m, d, hh, mm, ss, 0, 0, 0)) * 1000)
                    dup_counter = 0

                timestamp = ts + dup_counter
                tag_set = ','.join([measurement, 'side={}'.format(row[2]), 'iuid={}'.format(iuid)])
                field_set = ','.join(['price={}'.format(row[1]), 'volume={}'.format(row[3])])
                line = ' '.join([tag_set, field_set, str(timestamp)])
                data.append(line)

                last_time_str = time_str
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


# root_paths = [
#     u'/mnt/nas/_Ashare_HFTdata/execution/stock_trade_2018'  #//linux网盘路径
#     # u'/mnt/nas/_Ashare_HFTdata/execution/stock_trade_2017',
#     # u'/mnt/nas/_Ashare_HFTdata/execution/stock_trade_2016',
# ]

root_paths = [
    '/Users/lilychen/Desktop/AQUMON/GEM/逐笔成交/stock_trade_2018'
]

for root_path in root_paths:
    for root, dirs, files in os.walk(root_path):
        for date_str in dirs:
            if date_str < '20180102':
                continue
            for sub_root, date_dirs, files in os.walk(root + '/' + date_str):
                for filename in files:
                    ticker_code = filename[:6]
                    if ticker_code.startswith('1') or ticker_code.startswith('2'):
                        continue
                    iuid = get_iuid(filename[:2], filename[2:8])
                    pool.apply_async(func, args=(os.path.join(sub_root, filename), iuid, date_str))

pool.close()
pool.join()
