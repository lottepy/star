''' Handle timestamp manipulation

default timezone is UTC
i.e. default_tz = pytz.utc

# TODO next timestamp calculation recheck according to freq (currently: hardcode daily)
'''

import datetime as dt
import pandas as pd
import numpy as np
import pytz
# this function was moved out for result exporter to call externally
from quantcycle.utils.time_manipulation import timestamp2datetimestring


default_tz = pytz.utc

def list2datetime(lst_timestamp: list, tz=default_tz) -> list:
    ''' list of timestamp to datetime '''
    return [dt.datetime.fromtimestamp(int(ts), tz) for ts in lst_timestamp]

def list2timearr(lst_timestamp: list, tz=default_tz, data_freq: str = "DAILY"):
    ''' list of timestamp to time_arr for DataProcessor '''
    lst_dt = [dt.datetime.fromtimestamp(int(ts), tz) for ts in lst_timestamp]
    res = []
    interval = parse_data_freq_seconds(data_freq)
    for t in lst_dt:
        arr = []
        
        arr.append(int(t.timestamp()))
        arr.append(int(t.timestamp()))
        # arr.append(int(t.timestamp()+ interval)) # start and end timestamp are the same now 27/11/2020
        arr.append(t.weekday())
        arr.append(t.year)
        arr.append(t.month)
        arr.append(t.day)
        arr.append(t.hour)
        arr.append(t.minute)
        arr.append(t.second)
        
        res.append(arr)
    return np.array(res, dtype='uint64')

def timeindex2timearr(time_index, data_freq: str = "DAILY") -> list:
    ''' timeindex datetime utc to time_arr for DataProcessor 
    
    Args:
        time_index: pd time index obj in datetime utc  
        e.g. time_index = pd.to_datetime([1, 2]), unit='s')
    '''
    interval = parse_data_freq_seconds(data_freq)
    time_df = pd.DataFrame()
    time_df['timestamp'] = time_index.values.astype(np.int64) // 10 ** 9
    time_df['end_timestamp'] = time_df['timestamp'] # + interval #### start and end timestamp are the same now 27/11/2020
    time_df['weekday'] = time_index.weekday
    time_df['year'] = time_index.year
    time_df['month'] = time_index.month
    time_df['day'] = time_index.day
    time_df['hour'] = time_index.hour
    time_df['minute'] = time_index.minute
    time_df['second'] = time_index.second
    return time_df.values.astype('uint64')

'''
Moved out to quantcycle/utils/time_manipulation.py
'''
# def timestamp2datetimestring(time_index, tz = default_tz) -> list:
#     '''
#     pytz.all_timezones can check availabel timezones
#     tz example: pytz.timezone("Asia/Hong_Kong")
#     e.g. '2020-01-01 08:00:00 HKT'
#     '''
#     return [dt.datetime.fromtimestamp(int(ts), tz).strftime('%Y-%m-%d %H:%M:%S %Z') for ts in time_index]

def datetimestring2timestamp(time_index, tz = default_tz) -> list:
    '''
    pytz.all_timezones can check availabel timezones
    tz example: pytz.timezone("Asia/Hong_Kong")
    e.g. '2020-01-01 08:00:00 HKT'
    '''
    return [int(dt.replace(tzinfo=tz).astimezone(pytz.utc).timestamp()) for dt in pd.to_datetime(time_index)]

def string2timestamp(time_index, timezone: str = 'UTC'):
    '''
    pytz.all_timezones can check available timezones
    timezone = "Asia/Hong_Kong"
    pytz.timezone(timezone)
    
    3 supported string formats:
        '%Y-%m-%d'
        '%Y-%m-%d %H:%M:%S'
        '%Y-%m-%d %H:%M:%S %Z'
    e.g. '2020-01-01 08:00:00 HKT'
    '''
    if len(time_index)==0:
        return time_index
    if len(str(time_index[0])) <= 19:
        if timezone == 'UTC':
            tz = pytz.utc
        else:
            assert timezone in pytz.all_timezones, f'Unknown Time Zone Error: Please check if {timezone} is in pytz.all_timezones.'
            tz = pytz.timezone(timezone)
        res = datetimestring2timestamp(time_index, tz)
    else:
        res = [int(x.timestamp()) for x in pd.to_datetime(time_index)]

    return res

def parse_data_freq_seconds(data_freq: str = "DAILY"):
    if data_freq == 'DAILY':
        return 24*3600
    elif data_freq == 'HOURLY':
        return 3600
    elif data_freq == 'MINUTE':
        return 60
    else:
        assert False, f'timestamp_manipulation: {data_freq} not supported.'
