import pandas as pd
from constants import *

def cal_rebalancing_dates(start, end, rebalance_freq):
    rebalancing_dates = [start]
    tmp = pd.date_range(start, end, freq=REB_FREQ[rebalance_freq]).tolist()
    if tmp[0] != start:
        rebalancing_dates = rebalancing_dates + tmp
    else:
        rebalancing_dates = tmp

    return rebalancing_dates


def cal_rebalancing_dates_backward(data_start, formation_date, rebalance_freq):
    tmp = pd.date_range(data_start, formation_date, freq=REB_FREQ[rebalance_freq]).tolist()
    if tmp[-1] != formation_date:
        rebalancing_dates_bwd = tmp[:-1]
        rebalancing_dates_bwd = rebalancing_dates_bwd + [formation_date]
    else:
        rebalancing_dates_bwd = tmp

    return rebalancing_dates_bwd