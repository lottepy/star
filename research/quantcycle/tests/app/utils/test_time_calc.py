import pytz
import numpy as np
from datetime import datetime, timedelta


def test_new_day_logic():
    d200228t140230 = 1582898550
    d200228t215959 = 1582927199
    d200228t220000 = 1582927200
    d200229t000000 = 1582934400
    d200229t220000 = 1583013600
    d200301t000000 = 1583020800

    assert is_new_day_alternative_2(d200228t215959, d200228t140230) is False
    assert is_new_day_alternative_2(d200228t220000, d200228t140230) is True
    assert is_new_day_alternative_2(d200229t000000, d200228t220000) is False
    assert is_new_day_alternative_2(d200229t220000, d200228t220000) is True
    assert is_new_day_alternative_2(d200229t220000, d200228t140230) is True
    assert is_new_day_alternative_2(d200229t220000, d200229t220000) is False
    assert is_new_day_alternative_2(d200301t000000, d200229t220000) is False
    assert is_new_day_alternative_2(d200301t000000, d200229t000000) is True


def is_new_day(current_time: int, last_time: int):
    # Cut-off time: 22:00 UTC

    if last_time is None:  # beginning day
        return True

    assert current_time >= last_time
    current_time = datetime.fromtimestamp(current_time, pytz.utc)
    last_time = datetime.fromtimestamp(last_time, pytz.utc)
    current_day_cutoff = current_time.replace(hour=22, minute=0, second=0)
    cur_to_cur_cutoff_delta = current_time - current_day_cutoff
    last_to_cur_cutoff_delta = last_time - current_day_cutoff
    last_day_cutoff = last_time.replace(hour=22, minute=0, second=0)
    last_to_last_cutoff_delta = last_time - last_day_cutoff

    if last_to_cur_cutoff_delta.total_seconds() >= 0:
        return False
    if (last_to_last_cutoff_delta.total_seconds() >= 0 > cur_to_cur_cutoff_delta.total_seconds() and
            last_day_cutoff + timedelta(days=1) == current_day_cutoff):
        return False
    if (last_to_last_cutoff_delta.total_seconds() <= cur_to_cur_cutoff_delta.total_seconds() < 0 and
            last_day_cutoff == current_day_cutoff):
        return False
    return True


def is_new_day_alternative(current_time: int, last_time: int):
    # Cut-off time: 22:00 UTC
    # A numpy datetime64 implementation

    if last_time is None:  # beginning day
        return True

    assert current_time >= last_time
    unix_epoch = np.datetime64('1970-01-01T00:00:00')
    current_time = unix_epoch + current_time
    last_time = unix_epoch + last_time
    current_day_cutoff = np.datetime64(str(current_time)[0:str(current_time).find('T')] + 'T22:00:00')
    cur_to_cur_cutoff_delta = (current_time - current_day_cutoff) / np.timedelta64(1, 's')
    last_to_cur_cutoff_delta = (last_time - current_day_cutoff) / np.timedelta64(1, 's')
    last_day_cutoff = np.datetime64(str(last_time)[0:str(last_time).find('T')] + 'T22:00:00')
    last_to_last_cutoff_delta = (last_time - last_day_cutoff) / np.timedelta64(1, 's')

    if last_to_cur_cutoff_delta >= 0:
        return False
    if (last_to_last_cutoff_delta >= 0 > cur_to_cur_cutoff_delta and
            last_day_cutoff + np.timedelta64(1, 'D') == current_day_cutoff):
        return False
    if (last_to_last_cutoff_delta <= cur_to_cur_cutoff_delta < 0 and
            last_day_cutoff == current_day_cutoff):
        return False
    return True

def is_new_day_alternative_2(current_timestamp: int, last_timestamp: int):
    # Cut-off time: 22:00 UTC
    # A numpy datetime64 implementation

    timedelta64_1sec = np.timedelta64(1, 's')
    timedelta64_1day = np.timedelta64(1, 'D')
    unix_epoch = np.datetime64('1970-01-01T00:00:00')
    anchor_time = np.datetime64('1970-01-01T22:00:00')

    if last_timestamp == -1:  # beginning day
        return True

    assert current_timestamp >= last_timestamp
    current_time = unix_epoch + current_timestamp * timedelta64_1sec
    last_time = unix_epoch + last_timestamp * timedelta64_1sec

    # Calculate 22:00 UTC cutoff
    delta_days = current_timestamp // 86400
    current_day_cutoff = anchor_time + delta_days * timedelta64_1day
    delta_days = last_timestamp // 86400
    last_day_cutoff = anchor_time + delta_days * timedelta64_1day

    cur_to_cur_cutoff_delta = (current_time - current_day_cutoff) / timedelta64_1sec
    last_to_cur_cutoff_delta = (last_time - current_day_cutoff) / timedelta64_1sec
    last_to_last_cutoff_delta = (last_time - last_day_cutoff) / timedelta64_1sec

    if last_to_cur_cutoff_delta >= 0:
        return False
    if (last_to_last_cutoff_delta >= 0 > cur_to_cur_cutoff_delta and
            last_day_cutoff + timedelta64_1day == current_day_cutoff):
        return False
    if (last_to_last_cutoff_delta <= cur_to_cur_cutoff_delta < 0 and
            last_day_cutoff == current_day_cutoff):
        return False
    return True
