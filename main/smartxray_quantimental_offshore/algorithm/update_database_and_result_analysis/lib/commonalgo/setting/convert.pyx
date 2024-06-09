from cpython.datetime cimport datetime, date
import socket
import struct
import time

import maya
import pytz


cpdef dict REGION_TZ_MAP = {
    'US': 'America/New_York',
    'CN': 'Asia/Shanghai',
    'HK': 'Asia/Hong_Kong',
    'GB': 'Europe/London',
    'DE': 'Europe/Berlin',
    'SG': 'Asia/Singapore',
    'WW': 'UTC'
}
cpdef int SECOND = 1000
cpdef int MINUTE = SECOND * 60
cpdef int HOUR = MINUTE * 60
cpdef int DAY = HOUR * 24
cpdef int WEEK = DAY * 7



cpdef int ip_to_int(str addr):
    return struct.unpack("!I", socket.inet_aton(addr))[0]


cpdef str int_to_ip(int addr):
    return socket.inet_ntoa(struct.pack("!I", addr))


cpdef str timestamp_to_string(long epoch):
    return maya.MayaDT(int(epoch)).iso8601()


cpdef str get_tz(str region):
    return REGION_TZ_MAP[region]


cpdef object get_tzinfo(str region):
    return pytz.timezone(get_tz(region))


cpdef long datetime_to_timestamp(datetime dt, str unit='ms'):
    epoch = maya.MayaDT.from_datetime(dt).epoch
    if unit == 'ms':
        epoch *= 1000
    elif unit == 'ns':
        epoch *= 1000000
    return int(epoch)


cpdef long now(str unit='ms'):
    epoch = time.time()
    if unit == 'ms':
        epoch *= 1000
    elif unit == 'ns':
        epoch *= 1000000
    return int(epoch)


cpdef long parse_as_timestamp(str dt_string, str unit='ms'):
    epoch = maya.parse(dt_string).epoch
    if unit == 'ms':
        epoch *= 1000
    elif unit == 'ns':
        epoch *= 1000000
    return int(epoch)


cpdef datetime timestamp_to_datetime(long epoch, str country_code):
    if epoch > 2147483648:
        epoch /= 1000
    tz = get_tz(country_code)
    return maya.MayaDT(epoch).datetime(tz)


cpdef datetime get_current_datetime(str country_code):
    tz = get_tz(country_code)
    return maya.now().datetime(tz)


cpdef datetime convert_tz(datetime dt, str region):
    tz = pytz.timezone(get_tz(region))
    return dt.astimezone(tz)


cpdef datetime strptime_with_tz(str dt_string, str country_code):
    tz = get_tz(country_code)
    return maya.when(dt_string, timezone=tz)


cpdef datetime parse_localtime(str dt_string, str region):
    tz = pytz.timezone(get_tz(region))
    return tz.localize(maya.parse(dt_string).datetime(naive=True))


cpdef str format_timestamp(long epoch, str region, str fmt):
    dt = timestamp_to_datetime(epoch, region)
    return dt.strftime(fmt)


cpdef date format_date(str date_str):
    dt = maya.parse(date_str).datetime()
    return dt.date()


cpdef datetime localize(datetime dt, str region):
    tz = pytz.timezone(get_tz(region))
    return tz.localize(dt)


cpdef long get_when_timestamp(str date_string, str region, str unit='ms'):
    epoch = maya.when(string=date_string, timezone=get_tz(region)).epoch
    if unit == 'ms':
        epoch *= 1000
    elif unit == 'ns':
        epoch *= 1000000
    return int(epoch)
