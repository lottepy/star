from datetime import datetime,timedelta

import pandas as pd
from pandas.tseries.offsets import YearBegin, YearEnd
from pytz import timezone


def _timestamp_to_utc_datetime(timestamp: int or list):
    a = pd.to_datetime(timestamp, unit='s')
    return a


def _utc_date_range(freq, start_timestamp, end_timestamp, day_offset=0):
    start = _timestamp_to_utc_datetime(start_timestamp)
    _start = start-YearBegin()-timedelta(days=40)
    end = _timestamp_to_utc_datetime(end_timestamp)
    _end = end+YearEnd()
    output = pd.date_range(start=_start, end=_end, freq=freq)
    if day_offset > 0:
        output = [i+timedelta(days=day_offset) for i in output]
    output = [i for i in output if i >= start and i < end]
    return output


def _find_offset(aValue, aList):
    # 如果要找的数字不在里面, 则会返回最后一个比它小的数字
    offset = next(x[0] for x in enumerate(aList) if x[1] > aValue)
    return offset -1 if offset >= 0 else 0 # in case offset == -1


def sample(timestamp_list, allocation_freq, lookback_points_list, day_offset = 0):
    sample_list = _utc_date_range(
        freq=allocation_freq, start_timestamp=timestamp_list[0], end_timestamp=timestamp_list[-1], day_offset=day_offset)
    time_list = _timestamp_to_utc_datetime(timestamp_list)
    ix_list = [_find_offset(i, time_list) for i in sample_list]
    sample_ix_list = [[[sample_ix-lookback_points, sample_ix]
                       for sample_ix in ix_list if sample_ix-lookback_points >= 0] for lookback_points in lookback_points_list]
    return sample_ix_list


def timestamp_to_str(timestamp: int):
    tmp = _timestamp_to_utc_datetime(timestamp)
    return tmp.strftime('%Y/%m/%d')
