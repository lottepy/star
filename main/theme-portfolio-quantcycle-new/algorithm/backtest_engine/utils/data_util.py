import numba as nb
import numpy as np

from .constants import MinuteTime, MinuteTimeExtra, SnapshotTime, TradeTime
from .helper import raw_array_to_array


@nb.jit(cache=True, nopython=True)
def find_index(time_axis1, time_axis2):
    index = np.zeros(time_axis1.shape[0])
    j1 = 0
    for i in range(time_axis1.shape[0]):
        for j in range(j1, time_axis2.shape[0]):
            if time_axis2[j] > time_axis1[i]:
                break
        j1 = j
        index[i] = j1
    return index


def calc_minute_time_array_extra(minute_time_extra_col: list, data_dict, params: dict) -> np.ndarray:
    """
        计算回看/回放 所需的index，各列的偏移量分别为 MinuteTimeExtra.MINUTE_INDEX1.value 等。
        返回 shape (n, 6) 的 array
    """
    minute_time_array = raw_array_to_array(data_dict['minute_time_array_raw'], data_dict['minute_time_array_shape']) \
                            if 'minute_time_array_raw' in data_dict else np.empty((1, 1), dtype=np.float64)
    minute_data_array = raw_array_to_array(data_dict['minute_data_array_raw'], data_dict['minute_data_array_shape']) \
                            if 'minute_data_array_raw' in data_dict else np.empty((1, 1, 1), dtype=np.float64)
    snapshot_time_array = raw_array_to_array(data_dict['snapshot_time_array_raw'], data_dict['snapshot_time_array_shape']) \
                            if 'snapshot_time_array_raw' in data_dict else np.empty((1, 1), dtype=np.float64)
    snapshot_data_array = raw_array_to_array(data_dict['snapshot_data_array_raw'], data_dict['snapshot_data_array_shape']) \
                            if 'snapshot_data_array_raw' in data_dict else np.empty((1, 1, 1), dtype=np.float64)
    trade_time_array = raw_array_to_array(data_dict['trade_time_array_raw'], data_dict['trade_time_array_shape']) \
                            if 'trade_time_array_raw' in data_dict else np.empty((1, 1), dtype=np.float64)
    trade_data_array = raw_array_to_array(data_dict['trade_data_array_raw'], data_dict['trade_data_array_shape']) \
                            if 'trade_data_array_raw' in data_dict else np.empty((1, 1, 1), dtype=np.float64)

    minute_window_time = params['algo'].get('minute_window_time')
    snapshot_window_time = params['algo'].get('snapshot_window_time')
    trade_window_time = params['algo'].get('tick_window_time')

    arr = np.zeros((minute_time_array.shape[0], 6))  # shape (n, 6)
    if 'MINUTE_INDEX1' in minute_time_extra_col:
        arr[:, MinuteTimeExtra.MINUTE_INDEX1.value] = find_index(minute_time_array[:, MinuteTime.TIMESTAMP.value] - minute_window_time*60,
                                                                 minute_time_array[:, MinuteTime.TIMESTAMP.value])
    if 'MINUTE_INDEX2' in minute_time_extra_col:
        arr[:, MinuteTimeExtra.MINUTE_INDEX2.value] = list(range(1, minute_time_array.shape[0]+1))

    if 'SNAPSHOT_INDEX1' in minute_time_extra_col:
        arr[:, MinuteTimeExtra.SNAPSHOT_INDEX1.value] = find_index(minute_time_array[:, MinuteTime.TIMESTAMP.value] - snapshot_window_time,
                                                                   snapshot_time_array[:, SnapshotTime.TIMESTAMP.value])
    if 'SNAPSHOT_INDEX2' in minute_time_extra_col:
        arr[:, MinuteTimeExtra.SNAPSHOT_INDEX2.value] = find_index(minute_time_array[:, MinuteTime.TIMESTAMP.value],
                                                                   snapshot_time_array[:, SnapshotTime.TIMESTAMP.value])

    if 'TRADE_INDEX1' in minute_time_extra_col:
        arr[:, MinuteTimeExtra.TRADE_INDEX1.value] = find_index(minute_time_array[:, MinuteTime.TIMESTAMP.value] - trade_window_time,
                                                                   trade_time_array[:, TradeTime.TIMESTAMP.value])
    if 'TRADE_INDEX2' in minute_time_extra_col:
        arr[:, MinuteTimeExtra.TRADE_INDEX2.value] = find_index(minute_time_array[:, MinuteTime.TIMESTAMP.value],
                                                                   trade_time_array[:, TradeTime.TIMESTAMP.value])

    return arr
