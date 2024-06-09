#from dateutil.relativedelta import relativedelta
import pandas as pd
from constants import *


# def cal_rebalancing_dates(start, end, rebalance_freq):
#     if rebalance_freq == "ANNUALLY":
#         rebalancing_dates = []
#         tmp_dt = start
#         while tmp_dt <= end:
#             rebalancing_dates.append(tmp_dt)
#             tmp_dt = tmp_dt + relativedelta(years=1)
#     elif rebalance_freq == "SEMIANNUALLY":
#         rebalancing_dates = []
#         tmp_dt = start
#         while tmp_dt <= end:
#             rebalancing_dates.append(tmp_dt)
#             tmp_dt = tmp_dt + relativedelta(months=6)
#     elif rebalance_freq == "QUARTERLY":
#         rebalancing_dates = []
#         tmp_dt = start
#         while tmp_dt <= end:
#             rebalancing_dates.append(tmp_dt)
#             tmp_dt = tmp_dt + relativedelta(months=3)
#     elif rebalance_freq == "BIMONTHLY":
#         rebalancing_dates = []
#         tmp_dt = start
#         while tmp_dt <= end:
#             rebalancing_dates.append(tmp_dt)
#             tmp_dt = tmp_dt + relativedelta(months=2)
#     elif rebalance_freq == "MONTHLY":
#         rebalancing_dates = []
#         tmp_dt = start
#         while tmp_dt <= end:
#             rebalancing_dates.append(tmp_dt)
#             tmp_dt = tmp_dt + relativedelta(months=6)
#     elif rebalance_freq == "BIWEEKLY":
#         rebalancing_dates = []
#         tmp_dt = start
#         while tmp_dt <= end:
#             rebalancing_dates.append(tmp_dt)
#             tmp_dt = tmp_dt + relativedelta(weeks=2)
#     elif rebalance_freq == "WEEKLY":
#         rebalancing_dates = []
#         tmp_dt = start
#         while tmp_dt <= end:
#             rebalancing_dates.append(tmp_dt)
#             tmp_dt = tmp_dt + relativedelta(weeks=1)
#     elif rebalance_freq == "DAILY":
#         rebalancing_dates = []
#         tmp_dt = start
#         while tmp_dt <= end:
#             rebalancing_dates.append(tmp_dt)
#             tmp_dt = tmp_dt + relativedelta(days=6)
#
#     return rebalancing_dates
#
#
# def cal_rebalancing_dates_backward(start, end, rebalance_freq):
#     if rebalance_freq == "ANNUALLY":
#         rebalancing_dates_bwd = []
#         tmp_dt = end
#         while tmp_dt >= start:
#             rebalancing_dates_bwd.append(tmp_dt)
#             tmp_dt = tmp_dt - relativedelta(years=1)
#     elif rebalance_freq == "SEMIANNUALLY":
#         rebalancing_dates_bwd = []
#         tmp_dt = end
#         while tmp_dt >= start:
#             rebalancing_dates_bwd.append(tmp_dt)
#             tmp_dt = tmp_dt - relativedelta(months=6)
#     elif rebalance_freq == "QUARTERLY":
#         rebalancing_dates_bwd = []
#         tmp_dt = end
#         while tmp_dt >= start:
#             rebalancing_dates_bwd.append(tmp_dt)
#             tmp_dt = tmp_dt - relativedelta(months=3)
#     elif rebalance_freq == "BIMONTHLY":
#         rebalancing_dates_bwd = []
#         tmp_dt = end
#         while tmp_dt >= start:
#             rebalancing_dates_bwd.append(tmp_dt)
#             tmp_dt = tmp_dt - relativedelta(months=2)
#     elif rebalance_freq == "MONTHLY":
#         rebalancing_dates_bwd = []
#         tmp_dt = end
#         while tmp_dt >= start:
#             rebalancing_dates_bwd.append(tmp_dt)
#             tmp_dt = tmp_dt - relativedelta(months=6)
#     elif rebalance_freq == "BIWEEKLY":
#         rebalancing_dates_bwd = []
#         tmp_dt = end
#         while tmp_dt >= start:
#             rebalancing_dates_bwd.append(tmp_dt)
#             tmp_dt = tmp_dt - relativedelta(weeks=2)
#     elif rebalance_freq == "WEEKLY":
#         rebalancing_dates_bwd = []
#         tmp_dt = end
#         while tmp_dt >= start:
#             rebalancing_dates_bwd.append(tmp_dt)
#             tmp_dt = tmp_dt - relativedelta(weeks=1)
#     elif rebalance_freq == "DAILY":
#         rebalancing_dates_bwd = []
#         tmp_dt = end
#         while tmp_dt >= start:
#             rebalancing_dates_bwd.append(tmp_dt)
#             tmp_dt = tmp_dt - relativedelta(days=6)
#
#     return rebalancing_dates_bwd

def cal_rebalancing_dates(start, end, rebalance_freq):
    rebalancing_dates = [start]
    tmp = pd.date_range(start, end, freq=reb_freq[rebalance_freq]).tolist()
    if tmp[0] != start:
        rebalancing_dates = rebalancing_dates + tmp
    else:
        rebalancing_dates = tmp

    return rebalancing_dates


def cal_rebalancing_dates_backward(data_start, formation_date, rebalance_freq):
    tmp = pd.date_range(data_start, formation_date, freq=reb_freq[rebalance_freq]).tolist()
    if tmp[-1] != formation_date:
        rebalancing_dates_bwd = tmp[:-1]
        rebalancing_dates_bwd = rebalancing_dates_bwd + [formation_date]
    else:
        rebalancing_dates_bwd = tmp

    return rebalancing_dates_bwd