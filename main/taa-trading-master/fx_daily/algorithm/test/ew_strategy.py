import numpy as np

def ew_strategy(window_data, time_data, pv, strategy_param, last_order):
    # pv(1*security): 记录每个货币的pv = init_pv + pnl
    # window_data(time, symbol, field) field: last, bid, ask
    # time_data(time, field) field: timestamp, weekday, year, month, day

    pv = np.sum(pv)

    n_security = window_data.shape[1]
    weight = np.ones(n_security)/n_security
    ccp_target = pv * weight

    return ccp_target