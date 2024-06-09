from datetime import datetime

from metrics_calculator.core.sampler import *
from metrics_calculator.core.sampler import (_find_offset,
                                             _timestamp_to_utc_datetime,
                                             _utc_date_range)


def test__timestamp_to_utc_datetime():
    a = _timestamp_to_utc_datetime(1325433600)
    assert a.year == 2012 and a.month == 1 and a.day == 1 and a.hour == 16


def test__timestamp_to_utc_datetime2():
    a = _timestamp_to_utc_datetime(1325462400)
    assert a.year == 2012 and a.month == 1 and a.day == 2 and a.hour == 0


def test__timestamp_to_utc_datetime3():
    a, b = _timestamp_to_utc_datetime([1325433600, 1325462400])
    assert a.year == 2012 and a.month == 1 and a.day == 1 and a.hour == 16
    assert b.year == 2012 and b.month == 1 and b.day == 2 and b.hour == 0


def test__utc_date_range():
    output = _utc_date_range('H', 1325433600, 1325462400)
    assert len(output) == 9 and output[0].hour == 16 and output[-1].hour == 0


def test__utc_date_range2():
    output = _utc_date_range('H', 1325435400, 1325462400)
    assert len(output) == 8 and output[0].hour == 16 and output[-1].hour == 23

def test_utc_date_range_with_day_offset():
    output = _utc_date_range('M',
                             1325462400, # 2012.01.02 00:00:00+0
                             1331856000, # 2012.03.16 00:00:00+0
                             day_offset=15
                             )
    assert len(output) == 3 and output[0].day == 15 and output[0].month == 1

def test_utc_date_range_with_day_offset2():
    output = _utc_date_range('M',
                             1325462400, # 2012.01.02 00:00:00+0
                             1331856000, # 2012.03.16 00:00:00+0
                             day_offset=16
                             )
    assert len(output) == 3 and output[0].day == 16 and output[0].month == 1

def test_utc_date_range_with_day_offset3():
    output = _utc_date_range('M',
                             1325462400, # 2012.01.02 00:00:00+0
                             1331856000, # 2012.03.16 00:00:00+0
                             day_offset=17
                             )
    assert len(output) == 2 and output[0].day == 17 and output[0].month == 1

def test__find_offset():
    output = _find_offset(4, [1, 2, 3, 4, 5])
    assert output == 3


def test__find_offset2():
    output = _find_offset(4, [1, 2, 3, 5])
    assert output == 2
