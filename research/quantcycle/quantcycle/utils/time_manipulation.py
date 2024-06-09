''' DataManager utils timestamp_manipulation.py
Handle timestamp manipulation 

default timezone is UTC
i.e. default_tz = pytz.utc
'''

import datetime as dt
import pytz


def timestamp2datetimestring(time_index, tz = 'UTC') -> list:
    '''
    pytz.all_timezones can check availabel timezones
    tz example: pytz.timezone("Asia/Hong_Kong")
    e.g. '2020-01-01 08:00:00 HKT'
    '''
    tz = pytz.timezone(tz)
    return [dt.datetime.fromtimestamp(int(ts), tz).strftime('%Y-%m-%d %H:%M:%S %Z') for ts in time_index]
