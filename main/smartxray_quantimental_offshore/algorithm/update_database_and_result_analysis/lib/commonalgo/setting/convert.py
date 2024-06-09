# -*- coding:utf-8 -*-
import socket
import struct
import time

import maya
import pytz


REGION_TZ_MAP = {
    'US': 'America/New_York',
    'CN': 'Asia/Shanghai',
    'HK': 'Asia/Hong_Kong',
    'GB': 'Europe/London',
    'DE': 'Europe/Berlin',
    'SG': 'Asia/Singapore',
    'WW': 'UTC'
}
SECOND = 1000
MINUTE = SECOND * 60
HOUR = MINUTE * 60
DAY = HOUR * 24
WEEK = DAY * 7


def ip_to_int(addr):
    return struct.unpack("!I", socket.inet_aton(addr))[0]


def int_to_ip(addr):
    return socket.inet_ntoa(struct.pack("!I", addr))


def timestamp_to_string(epoch):
    return maya.MayaDT(int(epoch)).iso8601()


def get_tz(region):
    return REGION_TZ_MAP[region]


def get_tzinfo(region):
    return pytz.timezone(get_tz(region))


def datetime_to_timestamp(dt, unit='ms'):
    epoch = maya.MayaDT.from_datetime(dt).epoch
    if unit == 'ms':
        epoch *= 1000
    elif unit == 'ns':
        epoch *= 1000000
    return int(epoch)


def now(unit='ms'):
    epoch = time.time()
    if unit == 'ms':
        epoch *= 1000
    elif unit == 'ns':
        epoch *= 1000000
    return int(epoch)


def parse_as_timestamp(dt_string, unit='ms'):
    epoch = maya.parse(dt_string).epoch
    if unit == 'ms':
        epoch *= 1000
    elif unit == 'ns':
        epoch *= 1000000
    return int(epoch)


def timestamp_to_datetime(epoch, country_code):
    if epoch > 2147483648:
        epoch /= 1000
    tz = get_tz(country_code)
    return maya.MayaDT(epoch).datetime(tz)


def get_current_datetime(country_code):
    tz = get_tz(country_code)
    return maya.now().datetime(tz)


def convert_tz(dt, region):
    tz = pytz.timezone(get_tz(region))
    return dt.astimezone(tz)


def strptime_with_tz(dt_string, country_code):
    tz = get_tz(country_code)
    return maya.when(dt_string, timezone=tz)


def parse_localtime(dt_string, region):
    tz = pytz.timezone(get_tz(region))
    return tz.localize(maya.parse(dt_string).datetime(naive=True))


def format_timestamp(epoch, region, fmt):
    dt = timestamp_to_datetime(epoch, region)
    return dt.strftime(fmt)


def format_date(date_str):
    dt = maya.parse(date_str).datetime()
    return dt.date()


def localize(dt, region):
    tz = pytz.timezone(get_tz(region))
    return tz.localize(dt)


def get_when_timestamp(date_string, region, unit='ms'):
    epoch = maya.when(string=date_string, timezone=get_tz(region)).epoch
    if unit == 'ms':
        epoch *= 1000
    elif unit == 'ns':
        epoch *= 1000000
    return int(epoch)
