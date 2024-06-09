import numpy as np

from .constants import Time
from .helper import get_time


def prepare_time_related_data(data_time_array: np.ndarray, i: int, window_size: int):
    """
        Returns:
        - time_data: array, shape (window_size, 9)   constants.Time
        - current_time_ts: timestamp, int
        - current_year_month_day: array, such as [2020, 1, 1]
        - current_time_value: total second on that day start from 00:00, int
        - current_time_weekday: int
    """
    time_data = data_time_array[i - window_size + 1: i + 1]
    _current_time = time_data[-1]
    current_time_ts = int(_current_time[Time.TIMESTAMP.value])
    current_year_month_day = _current_time[Time.YEAR.value:Time.DAY.value + 1]
    current_time_value = get_time(_current_time[Time.HOUR.value:Time.SECOND.value + 1])
    current_time_weekday = int(_current_time[Time.WEEKDAY.value])

    return time_data, current_time_ts, current_year_month_day, current_time_value, current_time_weekday


def prepare_window_related_data(data_array, fx_data_array, rate_array, rate_time_array, time_index_tracker_dict: dict,
                                i: int, window_size: int, save_data_size: int):
    """
        For strategy util risk calculation.
        +2 is need for window_rate_data since fx need use t+1 interest rate

        Returns:
        - window_data: array, shape (window_size, n_symbols, 4)     constants.Data
        - window_past_data: array, shape (save_data_size, n_symbols, 4)  constants.Data
        - window_past_fx_data: array, shape (save_data_size, n_ccy)
        - window_rate_data: array, shape (save_data_size, n_symbols, 3)   constants.Rate
        - window_rate_time_data: array, shape (save_data_size, 9)    constants.Time
    """
    window_data = data_array[i - window_size + 1: i + 1]
    window_past_data = data_array[max(0, i - save_data_size + 1): i + 1]
    _fx_data_time_index = time_index_tracker_dict["fx_data"].time_index
    window_past_fx_data = fx_data_array[max(0, _fx_data_time_index - save_data_size + 1): _fx_data_time_index + 1]
    _dividends_time_index = time_index_tracker_dict["dividends"].time_index
    window_rate_data = rate_array[max(0, _dividends_time_index - save_data_size + 1):_dividends_time_index + 1]
    window_rate_time_data = rate_time_array[max(0, _dividends_time_index - save_data_size + 1):_dividends_time_index + 1]

    return window_data, window_past_data, window_past_fx_data, window_rate_data, window_rate_time_data


def prepare_current_data(data_array, fx_data_array, rate_array, time_index_tracker_dict, i: int):
    """
        Returns:
        - current_data: array, shape (n_symbols, 4)   constants.Data
        - current_fx_data: array, shape (n_ccy, )
        - current_rate: array, shape (n_symbols, 3)   constants.Rate
    """
    current_data = data_array[i]
    current_fx_data = fx_data_array[time_index_tracker_dict["fx_data"].time_index]
    current_rate = rate_array[time_index_tracker_dict["dividends"].time_index]
    # current_fx_time = fx_data_time_array[time_index_tracker_dict["fx_data"].time_index]
    # current_rate_time = rate_time_array[time_index_tracker_dict["dividends"].time_index]
    return current_data, current_fx_data, current_rate
