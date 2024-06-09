import pandas as pd
from datetime import timedelta, datetime
import numpy as np
from tqdm import tqdm
from collections import Counter


START_DATE = pd.to_datetime('2010-01-01')
END_DATE = pd.to_datetime('2020-11-30')
SSE_CALENDAR = pd.read_csv('data/TRADEDATE.csv', index_col=0, parse_dates=True)
STOCK_LIST = pd.read_csv('data/stockpools/ASHARE_COMPONENT.csv', index_col=0).columns.tolist()


def _align_df(df, columns, index):
    df = df.loc[index, columns]
    return df


def _offset_trading_day(start_day, offset):
    assert offset != 0
    union = list(sorted(set(SSE_CALENDAR.index.tolist() + [start_day])))
    ix = union.index(start_day)
    if ix + offset < 0:
        raise ValueError('Exceed TRADEDATE start day')
    if ix + offset >= len(union):
        raise ValueError('Exceed TRADEDATE end day')
    return union[ix + offset]


def _get_rebalance_dates(start_date, end_date, freq):
    if freq == 'Y':
        rebalance_dates = [datetime(i, 5, 1) for i in range(start_date.year, end_date.year+1)]
        rebalance_dates = [_offset_trading_day(i, -1) for i in rebalance_dates]
        rebalance_dates = [i for i in rebalance_dates if i >= START_DATE and i <= END_DATE]
        return rebalance_dates
    else:
        assert freq in ['M', 'W']
        rebalance_dates = pd.date_range(START_DATE, END_DATE, freq=freq)
        rebalance_dates = [_offset_trading_day(i+timedelta(days=1), -1) for i in rebalance_dates]
        rebalance_dates = [i for i in rebalance_dates if i >= START_DATE and i <= END_DATE]
        return rebalance_dates
   

def _is_main_board(symbol):
    if symbol[0] == "6":
        if symbol[1] == "0":
            return True
        else:
            return False
    else:
        if int(symbol[:3]) < 2:
            return True
        else:
            return False


def _get_percentile(raw, mask, pct):
    tmp = raw[mask]
    tmp = tmp[~np.isnan(tmp)]
    if len(tmp) < 100:
        return np.nan
    else:
        return np.percentile(tmp, pct)
    

def classic_ff5():
    mkt_cap = pd.read_csv('data/features/marketcap.csv', index_col=0, parse_dates=True)
    rtn = pd.read_csv('data/features/rtn.csv', index_col=0, parse_dates=True)
    riskfreerate = pd.read_csv('data/features/riskfreerate.csv', index_col=0, parse_dates=True)
    components = pd.read_csv('data/stockpools/ASHARE_TRADE_COMPONENT.csv', index_col=0, parse_dates=True)
    # calc bm using annual report
    netasset = pd.read_csv('data/features/netasset.csv', index_col=0, parse_dates=True)
    tmp = pd.concat([mkt_cap, pd.DataFrame(index=netasset.index)], axis=1, sort=True).ffill().loc[netasset.index]
    bm = netasset/tmp
    bm = bm.loc[[i for i in bm.index if i.month==12]]
    bm = pd.concat([bm, pd.DataFrame(index=mkt_cap.index)], axis=1, sort=True).ffill().loc[mkt_cap.index] # not correct for Jan, Feb, Mar but enough for classic models since only April's last trade day will be use
    bm = bm.loc[:, components.columns]
    
    # calc op using annual report
    # 旧OP: operating profit / net asset = (operating profit / gross revenue) * (gross revenue / shares) / (book value / shares) 
    # 新OP: operating profit ttm / net asset
    opttm = pd.read_csv('data/features/OPTTMR.csv', index_col=0, parse_dates=True)
    op = opttm / netasset
    op = op.loc[[i for i in op.index if i.month==12]]
    op = pd.concat([op, pd.DataFrame(index=mkt_cap.index)], axis=1, sort=True).ffill().loc[mkt_cap.index] # not correct for Jan, Feb, Mar but enough for classic models since only April's last trade day will be use
    op = op.loc[:, components.columns]
    
    # calc INV using annual report , total asset
    # totalasset = pd.read_csv('data/features/totalasset.csv', index_col=0, parse_dates=True)
    # tmp = totalasset.loc[[i for i in totalasset.index if i.month==12]]
    # inv = tmp / tmp.shift(1) - 1
    # inv = pd.concat([inv, pd.DataFrame(index=mkt_cap.index)], axis=1, sort=True).ffill().loc[mkt_cap.index]# not correct for Jan, Feb, Mar but enough for classic models since only April's last trade day will be use
    # inv = inv.loc[:, components.columns]
    yoyasset = pd.read_csv('data/features/YOYASSET.csv', index_col=0, parse_dates=True)
    yoyasset = yoyasset.loc[[i for i in yoyasset.index if i.month==12]]
    inv = pd.concat([yoyasset, pd.DataFrame(index=mkt_cap.index)], axis=1, sort=True).ffill().loc[mkt_cap.index]# not correct for Jan, Feb, Mar but enough for classic models since only April's last trade day will be use
    inv = inv.loc[:, components.columns]
    
    columns = components.columns.tolist()
    is_main_board = np.array([_is_main_board(i) for i in columns])
    index = SSE_CALENDAR.loc[START_DATE: END_DATE].index.tolist()
    
    mkt_cap = _align_df(mkt_cap, columns, index)
    rtn = _align_df(rtn, columns, index)
    riskfreerate = _align_df(riskfreerate, ['RF'], index)
    components = _align_df(components, columns, index)
    
    rebalance_dates = _get_rebalance_dates(START_DATE, END_DATE, 'Y')
    # MKT
    weights = pd.DataFrame(index=index, columns=columns)
    for date in tqdm(rebalance_dates):
        one_component = components.loc[date].values
        one_mkt_cap = mkt_cap.loc[date].values
        one_weight = one_component * one_mkt_cap
        one_weight[np.isnan(one_weight)] = 0
        one_weight = one_weight / np.sum(np.abs(one_weight))
        weights.loc[date] = one_weight
    weights = weights.astype(np.float64)
    for i in tqdm(range(1, len(index))):
        one_weight = weights.iloc[i].values
        if np.isnan(one_weight).all():
            last_weight = weights.iloc[i-1].values
            today_rtn = rtn.iloc[i].values
            tmp = last_weight * (1 + today_rtn)
            tmp[np.isnan(tmp)] = 0
            weights.iloc[i] = tmp / np.sum(np.abs(tmp))
    mkt_rtn = (weights.shift(1) * rtn).sum(axis=1).to_frame('MKT')
    
    # SMB, HML
    weights_list = [pd.DataFrame(index=index, columns=columns) for i in range(18)]
    for date in tqdm(rebalance_dates):
        one_component = components.loc[date].values
        one_mkt_cap = mkt_cap.loc[date].values
        one_bm = bm.loc[date].values
        # group
        mkt_cap_mid_point = _get_percentile(one_mkt_cap, (one_component * is_main_board).astype(bool), 50)
        mkt_max = _get_percentile(one_mkt_cap, (one_component * is_main_board).astype(bool), 100)
        
        bm_upper = _get_percentile(one_bm, (one_component * is_main_board).astype(bool), 70)
        bm_lower = _get_percentile(one_bm, (one_component * is_main_board).astype(bool), 30)
        bm_max = _get_percentile(one_bm, (one_component * is_main_board).astype(bool), 100)
        
        group = np.ones(len(columns)) * np.nan
        # 0: S/L Small Low
        # 1: S/M Small Middle
        # 2: S/H Small High
        # 3: B/L Big Low
        # 4: B/M Big Middle
        # 5: B/H Big High
        for j in range(len(columns)):
            # print(one_component[j])
            if one_component[j]:
                if one_mkt_cap[j] <= mkt_cap_mid_point:
                    tmp1 = 0
                elif one_mkt_cap[j] <= mkt_max:
                    tmp1 = 3
                else:
                    tmp1 = np.nan
                if one_bm[j] <= bm_lower:
                    tmp2 = 0
                elif one_bm[j] <= bm_upper:
                    tmp2 = 1
                elif one_bm[j] <= bm_max:
                    tmp2 = 2
                else:
                    tmp2 = np.nan
                group[j] = tmp1 + tmp2
        # print(Counter(group))
        for i in range(6):
            one_mask = (group == i)
            # print(f"{date.strftime('%Y-%m-%d')} group {i}: {np.sum(one_mask.astype(int))}")
            one_weight = one_mask * one_mkt_cap
            one_weight[np.isnan(one_weight)] = 0
            one_weight = one_weight / np.sum(np.abs(one_weight))
            weights_list[i].loc[date] = one_weight
    portfolio_rtn_list = []
    for ii in range(6):
        weights = weights_list[ii]
        weights = weights.astype(np.float64)
        for i in range(1, len(index)):
            one_weight = weights.iloc[i].values
            if np.isnan(one_weight).all():
                last_weight = weights.iloc[i-1].values
                today_rtn = rtn.iloc[i].values
                tmp = last_weight * (1 + today_rtn)
                tmp[np.isnan(tmp)] = 0
                weights.iloc[i] = tmp / np.sum(np.abs(tmp))
        weights_list[ii] = weights
        portfolio_rtn = (weights.shift(1) * rtn).sum(axis=1).to_frame('rtn')
        portfolio_rtn_list.append(portfolio_rtn)
    smb_1 = (((portfolio_rtn_list[0]["rtn"] + portfolio_rtn_list[1]["rtn"] + portfolio_rtn_list[2]["rtn"]) - (portfolio_rtn_list[3]["rtn"] + portfolio_rtn_list[4]["rtn"] + portfolio_rtn_list[5]["rtn"]))/3).to_frame('SMB')
    hml = (((portfolio_rtn_list[2]["rtn"] + portfolio_rtn_list[5]["rtn"]) - (portfolio_rtn_list[0]["rtn"] + portfolio_rtn_list[3]["rtn"]))/2).to_frame('HML')

    # SMB, RMW
    weights_list = [pd.DataFrame(index=index, columns=columns) for i in range(18)]
    for date in tqdm(rebalance_dates):
        one_component = components.loc[date].values
        one_mkt_cap = mkt_cap.loc[date].values
        one_op = op.loc[date].values
        # group
        mkt_cap_mid_point = _get_percentile(one_mkt_cap, (one_component * is_main_board).astype(bool), 50)
        mkt_max = _get_percentile(one_mkt_cap, (one_component * is_main_board).astype(bool), 100)
        
        op_upper = _get_percentile(one_op, (one_component * is_main_board).astype(bool), 70)
        op_lower = _get_percentile(one_op, (one_component * is_main_board).astype(bool), 30)
        op_max = _get_percentile(one_op, (one_component * is_main_board).astype(bool), 100)
        
        group = np.ones(len(columns)) * np.nan
        # 0: S/W Small Weak
        # 1: S/M Small Neutral
        # 2: S/R Small Robust
        # 3: B/W Big Weak
        # 4: B/M Big Neutral
        # 5: B/R Big Robust
        for j in range(len(columns)):
            # print(one_component[j])
            if one_component[j]:
                if one_mkt_cap[j] <= mkt_cap_mid_point:
                    tmp1 = 0
                elif one_mkt_cap[j] <= mkt_max:
                    tmp1 = 3
                else:
                    tmp1 = np.nan
                if one_op[j] <= op_lower:
                    tmp2 = 0
                elif one_op[j] <= op_upper:
                    tmp2 = 1
                elif one_op[j] <= op_max:
                    tmp2 = 2
                else:
                    tmp2 = np.nan
                group[j] = tmp1 + tmp2
        # print(Counter(group))
        for i in range(6):
            one_mask = (group == i)
            # print(f"{date.strftime('%Y-%m-%d')} group {i}: {np.sum(one_mask.astype(int))}")
            one_weight = one_mask * one_mkt_cap
            one_weight[np.isnan(one_weight)] = 0
            one_weight = one_weight / np.sum(np.abs(one_weight))
            weights_list[i].loc[date] = one_weight
    portfolio_rtn_list = []
    for ii in range(6):
        weights = weights_list[ii]
        weights = weights.astype(np.float64)
        for i in range(1, len(index)):
            one_weight = weights.iloc[i].values
            if np.isnan(one_weight).all():
                last_weight = weights.iloc[i-1].values
                today_rtn = rtn.iloc[i].values
                tmp = last_weight * (1 + today_rtn)
                tmp[np.isnan(tmp)] = 0
                weights.iloc[i] = tmp / np.sum(np.abs(tmp))
        weights_list[ii] = weights
        portfolio_rtn = (weights.shift(1) * rtn).sum(axis=1).to_frame('rtn')
        portfolio_rtn_list.append(portfolio_rtn)
    smb_2 = (((portfolio_rtn_list[0]["rtn"] + portfolio_rtn_list[1]["rtn"] + portfolio_rtn_list[2]["rtn"]) - (portfolio_rtn_list[3]["rtn"] + portfolio_rtn_list[4]["rtn"] + portfolio_rtn_list[5]["rtn"]))/3).to_frame('SMB')
    rmw = (((portfolio_rtn_list[2]["rtn"] + portfolio_rtn_list[5]["rtn"]) - (portfolio_rtn_list[0]["rtn"] + portfolio_rtn_list[3]["rtn"]))/2).to_frame('RMW')
    
    # SMB, CMA
    weights_list = [pd.DataFrame(index=index, columns=columns) for i in range(18)]
    for date in tqdm(rebalance_dates):
        one_component = components.loc[date].values
        one_mkt_cap = mkt_cap.loc[date].values
        one_inv = inv.loc[date].values
        # group
        mkt_cap_mid_point = _get_percentile(one_mkt_cap, (one_component * is_main_board).astype(bool), 50)
        mkt_max = _get_percentile(one_mkt_cap, (one_component * is_main_board).astype(bool), 100)
        
        inv_upper = _get_percentile(one_inv, (one_component * is_main_board).astype(bool), 70)
        inv_lower = _get_percentile(one_inv, (one_component * is_main_board).astype(bool), 30)
        inv_max = _get_percentile(one_inv, (one_component * is_main_board).astype(bool), 100)
        
        group = np.ones(len(columns)) * np.nan
        # 0: S/W Small Conservative
        # 1: S/M Small Neutral
        # 2: S/R Small Aggressive
        # 3: B/W Big Conservative
        # 4: B/M Big Neutral
        # 5: B/R Big Aggressive
        for j in range(len(columns)):
            # print(one_component[j])
            if one_component[j]:
                if one_mkt_cap[j] <= mkt_cap_mid_point:
                    tmp1 = 0
                elif one_mkt_cap[j] <= mkt_max:
                    tmp1 = 3
                else:
                    tmp1 = np.nan
                if one_inv[j] <= inv_lower:
                    tmp2 = 0
                elif one_inv[j] <= inv_upper:
                    tmp2 = 1
                elif one_inv[j] <= inv_max:
                    tmp2 = 2
                else:
                    tmp2 = np.nan
                group[j] = tmp1 + tmp2
        # print(Counter(group))
        for i in range(6):
            one_mask = (group == i)
            # print(f"{date.strftime('%Y-%m-%d')} group {i}: {np.sum(one_mask.astype(int))}")
            one_weight = one_mask * one_mkt_cap
            one_weight[np.isnan(one_weight)] = 0
            one_weight = one_weight / np.sum(np.abs(one_weight))
            weights_list[i].loc[date] = one_weight
    portfolio_rtn_list = []
    for ii in range(6):
        weights = weights_list[ii]
        weights = weights.astype(np.float64)
        for i in range(1, len(index)):
            one_weight = weights.iloc[i].values
            if np.isnan(one_weight).all():
                last_weight = weights.iloc[i-1].values
                today_rtn = rtn.iloc[i].values
                tmp = last_weight * (1 + today_rtn)
                tmp[np.isnan(tmp)] = 0
                weights.iloc[i] = tmp / np.sum(np.abs(tmp))
        weights_list[ii] = weights
        portfolio_rtn = (weights.shift(1) * rtn).sum(axis=1).to_frame('rtn')
        portfolio_rtn_list.append(portfolio_rtn)
    smb_3 = (((portfolio_rtn_list[0]["rtn"] + portfolio_rtn_list[1]["rtn"] + portfolio_rtn_list[2]["rtn"]) - (portfolio_rtn_list[3]["rtn"] + portfolio_rtn_list[4]["rtn"] + portfolio_rtn_list[5]["rtn"]))/3).to_frame('SMB')
    cma = (((portfolio_rtn_list[0]["rtn"] + portfolio_rtn_list[3]["rtn"]) - (portfolio_rtn_list[2]["rtn"] + portfolio_rtn_list[5]["rtn"]))/2).to_frame('CMA')

    smb = (smb_1 + smb_2 + smb_3)/3

    payoff_df = pd.concat([riskfreerate, mkt_rtn, smb, hml, rmw, cma], axis=1)
    payoff_df.to_csv('data/payoffs/pricing_factors/FF5/classic_ff5.csv')
    return


def simplified_ff5():
    mkt_cap = pd.read_csv('data/features/marketcap.csv', index_col=0, parse_dates=True)
    rtn = pd.read_csv('data/features/rtn.csv', index_col=0, parse_dates=True)
    riskfreerate = pd.read_csv('data/features/riskfreerate.csv', index_col=0, parse_dates=True)
    components = pd.read_csv('data/stockpools/ASHARE_TRADE_COMPONENT.csv', index_col=0, parse_dates=True)
    bm = pd.read_csv('data/features/bm.csv', index_col=0, parse_dates=True)
    op = pd.read_csv('data/features/op.csv', index_col=0, parse_dates=True)
    inv = pd.read_csv('data/features/inv_net_directly.csv', index_col=0, parse_dates=True)
    
    columns = components.columns.tolist()
    index = SSE_CALENDAR.loc[START_DATE: END_DATE].index.tolist()
    
    mkt_cap = _align_df(mkt_cap, columns, index)
    rtn = _align_df(rtn, columns, index)
    riskfreerate = _align_df(riskfreerate, ['RF'], index)
    components = _align_df(components, columns, index)
    bm = _align_df(bm, columns, index)
    op = _align_df(op, columns, index)
    inv = _align_df(inv, columns, index)
    
    rebalance_dates = _get_rebalance_dates(START_DATE, END_DATE, 'M')
    #MKT
    weights = pd.DataFrame(index=index, columns=columns)
    for date in tqdm(rebalance_dates):
        one_component = components.loc[date].values
        one_mkt_cap = mkt_cap.loc[date].values
        one_weight = one_component * one_mkt_cap
        one_weight[np.isnan(one_weight)] = 0
        one_weight = one_weight / np.sum(np.abs(one_weight))
        weights.loc[date] = one_weight
    weights = weights.astype(np.float64)
    for i in range(1, len(index)):
        one_weight = weights.iloc[i].values
        if np.isnan(one_weight).all():
            last_weight = weights.iloc[i-1].values
            today_rtn = rtn.iloc[i].values
            tmp = last_weight * (1 + today_rtn)
            tmp[np.isnan(tmp)] = 0
            weights.iloc[i] = tmp / np.sum(np.abs(tmp))
    mkt_rtn = (weights.shift(1) * rtn).sum(axis=1).to_frame('MKT')
    
    # SIZE
    top_weights = pd.DataFrame(index=index, columns=columns)
    bot_weights = pd.DataFrame(index=index, columns=columns)
    for date in tqdm(rebalance_dates):
        one_component = components.loc[date].values
        one_mkt_cap = mkt_cap.loc[date].values
        mkt_cap_upper = _get_percentile(one_mkt_cap, one_component.astype(bool), 90)
        mkt_cap_lower = _get_percentile(one_mkt_cap, one_component.astype(bool), 10)
        one_top = (one_mkt_cap >= mkt_cap_upper) * one_mkt_cap * one_component
        one_top[np.isnan(one_top)] = 0
        one_top = one_top / np.sum(np.abs(one_top))
        top_weights.loc[date] = one_top
        one_bot = (one_mkt_cap <= mkt_cap_lower) * one_mkt_cap * one_component
        one_bot[np.isnan(one_bot)] = 0
        one_bot = one_bot / np.sum(np.abs(one_bot))
        bot_weights.loc[date] = one_bot
    top_weights = top_weights.astype(np.float64)
    bot_weights = bot_weights.astype(np.float64)
    
    for i in range(1, len(index)):
        one_weight = top_weights.iloc[i].values
        if np.isnan(one_weight).all():
            last_weight = top_weights.iloc[i-1].values
            today_rtn = rtn.iloc[i].values
            tmp = last_weight * (1 + today_rtn)
            tmp[np.isnan(tmp)] = 0
            top_weights.iloc[i] = tmp / np.sum(np.abs(tmp))
    top_rtn = (top_weights.shift(1) * rtn).sum(axis=1)
    
    for i in range(1, len(index)):
        one_weight = bot_weights.iloc[i].values
        if np.isnan(one_weight).all():
            last_weight = bot_weights.iloc[i-1].values
            today_rtn = rtn.iloc[i].values
            tmp = last_weight * (1 + today_rtn)
            tmp[np.isnan(tmp)] = 0
            bot_weights.iloc[i] = tmp / np.sum(np.abs(tmp))
    bot_rtn = (bot_weights.shift(1) * rtn).sum(axis=1)

    smb = (bot_rtn - top_rtn).to_frame('SMB')
        
    # VALUE
    top_weights = pd.DataFrame(index=index, columns=columns)
    bot_weights = pd.DataFrame(index=index, columns=columns)
    for date in tqdm(rebalance_dates):
        one_component = components.loc[date].values
        one_mkt_cap = mkt_cap.loc[date].values
        one_bm = bm.loc[date].values
        bm_upper = _get_percentile(one_bm, one_component.astype(bool), 90)
        bm_lower = _get_percentile(one_bm, one_component.astype(bool), 10)
        one_top = (one_bm >= bm_upper) * one_mkt_cap * one_component
        one_top[np.isnan(one_top)] = 0
        one_top = one_top / np.sum(np.abs(one_top))
        top_weights.loc[date] = one_top
        one_bot = (one_bm <= bm_lower) * one_mkt_cap * one_component
        one_bot[np.isnan(one_bot)] = 0
        one_bot = one_bot / np.sum(np.abs(one_bot))
        bot_weights.loc[date] = one_bot
    top_weights = top_weights.astype(np.float64)
    bot_weights = bot_weights.astype(np.float64)
    
    for i in range(1, len(index)):
        one_weight = top_weights.iloc[i].values
        if np.isnan(one_weight).all():
            last_weight = top_weights.iloc[i-1].values
            today_rtn = rtn.iloc[i].values
            tmp = last_weight * (1 + today_rtn)
            tmp[np.isnan(tmp)] = 0
            top_weights.iloc[i] = tmp / np.sum(np.abs(tmp))
    top_rtn = (top_weights.shift(1) * rtn).sum(axis=1)
    
    for i in range(1, len(index)):
        one_weight = bot_weights.iloc[i].values
        if np.isnan(one_weight).all():
            last_weight = bot_weights.iloc[i-1].values
            today_rtn = rtn.iloc[i].values
            tmp = last_weight * (1 + today_rtn)
            tmp[np.isnan(tmp)] = 0
            bot_weights.iloc[i] = tmp / np.sum(np.abs(tmp))
    bot_rtn = (bot_weights.shift(1) * rtn).sum(axis=1)
    hml = (top_rtn - bot_rtn).to_frame('HML')
    
    # profibility
    top_weights = pd.DataFrame(index=index, columns=columns)
    bot_weights = pd.DataFrame(index=index, columns=columns)
    for date in tqdm(rebalance_dates):
        one_component = components.loc[date].values
        one_mkt_cap = mkt_cap.loc[date].values
        one_op = op.loc[date].values
        op_upper = _get_percentile(one_op, one_component.astype(bool), 90)
        op_lower = _get_percentile(one_op, one_component.astype(bool), 10)
        one_top = (one_op >= op_upper) * one_mkt_cap * one_component
        one_top[np.isnan(one_top)] = 0
        one_top = one_top / np.sum(np.abs(one_top))
        top_weights.loc[date] = one_top
        one_bot = (one_op <= op_lower) * one_mkt_cap * one_component
        one_bot[np.isnan(one_bot)] = 0
        one_bot = one_bot / np.sum(np.abs(one_bot))
        bot_weights.loc[date] = one_bot
    top_weights = top_weights.astype(np.float64)
    bot_weights = bot_weights.astype(np.float64)
    
    for i in range(1, len(index)):
        one_weight = top_weights.iloc[i].values
        if np.isnan(one_weight).all():
            last_weight = top_weights.iloc[i-1].values
            today_rtn = rtn.iloc[i].values
            tmp = last_weight * (1 + today_rtn)
            tmp[np.isnan(tmp)] = 0
            top_weights.iloc[i] = tmp / np.sum(np.abs(tmp))
    top_rtn = (top_weights.shift(1) * rtn).sum(axis=1)
    
    for i in range(1, len(index)):
        one_weight = bot_weights.iloc[i].values
        if np.isnan(one_weight).all():
            last_weight = bot_weights.iloc[i-1].values
            today_rtn = rtn.iloc[i].values
            tmp = last_weight * (1 + today_rtn)
            tmp[np.isnan(tmp)] = 0
            bot_weights.iloc[i] = tmp / np.sum(np.abs(tmp))
    bot_rtn = (bot_weights.shift(1) * rtn).sum(axis=1)
    rmw = (top_rtn - bot_rtn).to_frame('RMW')
    
    # inv
    top_weights = pd.DataFrame(index=index, columns=columns)
    bot_weights = pd.DataFrame(index=index, columns=columns)
    for date in tqdm(rebalance_dates):
        one_component = components.loc[date].values
        one_mkt_cap = mkt_cap.loc[date].values
        one_inv = inv.loc[date].values
        inv_upper = _get_percentile(one_inv, one_component.astype(bool), 90)
        inv_lower = _get_percentile(one_inv, one_component.astype(bool), 10)
        one_top = (one_inv >= inv_upper) * one_mkt_cap * one_component
        one_top[np.isnan(one_top)] = 0
        one_top = one_top / np.sum(np.abs(one_top))
        top_weights.loc[date] = one_top
        one_bot = (one_inv <= inv_lower) * one_mkt_cap * one_component
        one_bot[np.isnan(one_bot)] = 0
        one_bot = one_bot / np.sum(np.abs(one_bot))
        bot_weights.loc[date] = one_bot
    top_weights = top_weights.astype(np.float64)
    bot_weights = bot_weights.astype(np.float64)
    
    for i in range(1, len(index)):
        one_weight = top_weights.iloc[i].values
        if np.isnan(one_weight).all():
            last_weight = top_weights.iloc[i-1].values
            today_rtn = rtn.iloc[i].values
            tmp = last_weight * (1 + today_rtn)
            tmp[np.isnan(tmp)] = 0
            top_weights.iloc[i] = tmp / np.sum(np.abs(tmp))
    top_rtn = (top_weights.shift(1) * rtn).sum(axis=1)
    
    for i in range(1, len(index)):
        one_weight = bot_weights.iloc[i].values
        if np.isnan(one_weight).all():
            last_weight = bot_weights.iloc[i-1].values
            today_rtn = rtn.iloc[i].values
            tmp = last_weight * (1 + today_rtn)
            tmp[np.isnan(tmp)] = 0
            bot_weights.iloc[i] = tmp / np.sum(np.abs(tmp))
    bot_rtn = (bot_weights.shift(1) * rtn).sum(axis=1)
    cma = (bot_rtn - top_rtn).to_frame('CMA')
    
    payoff_df = pd.concat([riskfreerate, mkt_rtn, smb, hml, rmw, cma], axis=1)
    payoff_df.to_csv('data/payoffs/pricing_factors/FF5/simplified_ff5.csv')
    return


def weekly_ff5():
    mkt_cap = pd.read_csv('data/features/marketcap.csv', index_col=0, parse_dates=True)
    rtn = pd.read_csv('data/features/rtn.csv', index_col=0, parse_dates=True)
    riskfreerate = pd.read_csv('data/features/riskfreerate.csv', index_col=0, parse_dates=True)
    components = pd.read_csv('data/stockpools/ASHARE_TRADE_COMPONENT.csv', index_col=0, parse_dates=True)
    bm = pd.read_csv('data/features/bm.csv', index_col=0, parse_dates=True)
    op = pd.read_csv('data/features/op.csv', index_col=0, parse_dates=True)
    inv = pd.read_csv('data/features/inv_net_directly.csv', index_col=0, parse_dates=True)
    
    columns = components.columns.tolist()
    index = SSE_CALENDAR.loc[START_DATE: END_DATE].index.tolist()
    
    mkt_cap = _align_df(mkt_cap, columns, index)
    rtn = _align_df(rtn, columns, index)
    riskfreerate = _align_df(riskfreerate, ['RF'], index)
    components = _align_df(components, columns, index)
    bm = _align_df(bm, columns, index)
    op = _align_df(op, columns, index)
    inv = _align_df(inv, columns, index)
    
    rebalance_dates = _get_rebalance_dates(START_DATE, END_DATE, 'W')
    #MKT
    weights = pd.DataFrame(index=index, columns=columns)
    for date in tqdm(rebalance_dates):
        one_component = components.loc[date].values
        one_mkt_cap = mkt_cap.loc[date].values
        one_weight = one_component * one_mkt_cap
        one_weight[np.isnan(one_weight)] = 0
        one_weight = one_weight / np.sum(np.abs(one_weight))
        weights.loc[date] = one_weight
    weights = weights.astype(np.float64)
    for i in range(1, len(index)):
        one_weight = weights.iloc[i].values
        if np.isnan(one_weight).all():
            last_weight = weights.iloc[i-1].values
            today_rtn = rtn.iloc[i].values
            tmp = last_weight * (1 + today_rtn)
            tmp[np.isnan(tmp)] = 0
            weights.iloc[i] = tmp / np.sum(np.abs(tmp))
    mkt_rtn = (weights.shift(1) * rtn).sum(axis=1).to_frame('MKT')
    
    # SIZE
    top_weights = pd.DataFrame(index=index, columns=columns)
    bot_weights = pd.DataFrame(index=index, columns=columns)
    for date in tqdm(rebalance_dates):
        one_component = components.loc[date].values
        one_mkt_cap = mkt_cap.loc[date].values
        mkt_cap_upper = _get_percentile(one_mkt_cap, one_component.astype(bool), 90)
        mkt_cap_lower = _get_percentile(one_mkt_cap, one_component.astype(bool), 10)
        one_top = (one_mkt_cap >= mkt_cap_upper) * one_mkt_cap * one_component
        one_top[np.isnan(one_top)] = 0
        one_top = one_top / np.sum(np.abs(one_top))
        top_weights.loc[date] = one_top
        one_bot = (one_mkt_cap <= mkt_cap_lower) * one_mkt_cap * one_component
        one_bot[np.isnan(one_bot)] = 0
        one_bot = one_bot / np.sum(np.abs(one_bot))
        bot_weights.loc[date] = one_bot
    top_weights = top_weights.astype(np.float64)
    bot_weights = bot_weights.astype(np.float64)
    
    for i in range(1, len(index)):
        one_weight = top_weights.iloc[i].values
        if np.isnan(one_weight).all():
            last_weight = top_weights.iloc[i-1].values
            today_rtn = rtn.iloc[i].values
            tmp = last_weight * (1 + today_rtn)
            tmp[np.isnan(tmp)] = 0
            top_weights.iloc[i] = tmp / np.sum(np.abs(tmp))
    top_rtn = (top_weights.shift(1) * rtn).sum(axis=1)
    
    for i in range(1, len(index)):
        one_weight = bot_weights.iloc[i].values
        if np.isnan(one_weight).all():
            last_weight = bot_weights.iloc[i-1].values
            today_rtn = rtn.iloc[i].values
            tmp = last_weight * (1 + today_rtn)
            tmp[np.isnan(tmp)] = 0
            bot_weights.iloc[i] = tmp / np.sum(np.abs(tmp))
    bot_rtn = (bot_weights.shift(1) * rtn).sum(axis=1)

    smb = (bot_rtn - top_rtn).to_frame('SMB')
        
    # VALUE
    top_weights = pd.DataFrame(index=index, columns=columns)
    bot_weights = pd.DataFrame(index=index, columns=columns)
    for date in tqdm(rebalance_dates):
        one_component = components.loc[date].values
        one_mkt_cap = mkt_cap.loc[date].values
        one_bm = bm.loc[date].values
        bm_upper = _get_percentile(one_bm, one_component.astype(bool), 90)
        bm_lower = _get_percentile(one_bm, one_component.astype(bool), 10)
        one_top = (one_bm >= bm_upper) * one_mkt_cap * one_component
        one_top[np.isnan(one_top)] = 0
        one_top = one_top / np.sum(np.abs(one_top))
        top_weights.loc[date] = one_top
        one_bot = (one_bm <= bm_lower) * one_mkt_cap * one_component
        one_bot[np.isnan(one_bot)] = 0
        one_bot = one_bot / np.sum(np.abs(one_bot))
        bot_weights.loc[date] = one_bot
    top_weights = top_weights.astype(np.float64)
    bot_weights = bot_weights.astype(np.float64)
    
    for i in range(1, len(index)):
        one_weight = top_weights.iloc[i].values
        if np.isnan(one_weight).all():
            last_weight = top_weights.iloc[i-1].values
            today_rtn = rtn.iloc[i].values
            tmp = last_weight * (1 + today_rtn)
            tmp[np.isnan(tmp)] = 0
            top_weights.iloc[i] = tmp / np.sum(np.abs(tmp))
    top_rtn = (top_weights.shift(1) * rtn).sum(axis=1)
    
    for i in range(1, len(index)):
        one_weight = bot_weights.iloc[i].values
        if np.isnan(one_weight).all():
            last_weight = bot_weights.iloc[i-1].values
            today_rtn = rtn.iloc[i].values
            tmp = last_weight * (1 + today_rtn)
            tmp[np.isnan(tmp)] = 0
            bot_weights.iloc[i] = tmp / np.sum(np.abs(tmp))
    bot_rtn = (bot_weights.shift(1) * rtn).sum(axis=1)
    hml = (top_rtn - bot_rtn).to_frame('HML')
    
    # profibility
    top_weights = pd.DataFrame(index=index, columns=columns)
    bot_weights = pd.DataFrame(index=index, columns=columns)
    for date in tqdm(rebalance_dates):
        one_component = components.loc[date].values
        one_mkt_cap = mkt_cap.loc[date].values
        one_op = op.loc[date].values
        op_upper = _get_percentile(one_op, one_component.astype(bool), 90)
        op_lower = _get_percentile(one_op, one_component.astype(bool), 10)
        one_top = (one_op >= op_upper) * one_mkt_cap * one_component
        one_top[np.isnan(one_top)] = 0
        one_top = one_top / np.sum(np.abs(one_top))
        top_weights.loc[date] = one_top
        one_bot = (one_op <= op_lower) * one_mkt_cap * one_component
        one_bot[np.isnan(one_bot)] = 0
        one_bot = one_bot / np.sum(np.abs(one_bot))
        bot_weights.loc[date] = one_bot
    top_weights = top_weights.astype(np.float64)
    bot_weights = bot_weights.astype(np.float64)
    
    for i in range(1, len(index)):
        one_weight = top_weights.iloc[i].values
        if np.isnan(one_weight).all():
            last_weight = top_weights.iloc[i-1].values
            today_rtn = rtn.iloc[i].values
            tmp = last_weight * (1 + today_rtn)
            tmp[np.isnan(tmp)] = 0
            top_weights.iloc[i] = tmp / np.sum(np.abs(tmp))
    top_rtn = (top_weights.shift(1) * rtn).sum(axis=1)
    
    for i in range(1, len(index)):
        one_weight = bot_weights.iloc[i].values
        if np.isnan(one_weight).all():
            last_weight = bot_weights.iloc[i-1].values
            today_rtn = rtn.iloc[i].values
            tmp = last_weight * (1 + today_rtn)
            tmp[np.isnan(tmp)] = 0
            bot_weights.iloc[i] = tmp / np.sum(np.abs(tmp))
    bot_rtn = (bot_weights.shift(1) * rtn).sum(axis=1)
    rmw = (top_rtn - bot_rtn).to_frame('RMW')
    
    # inv
    top_weights = pd.DataFrame(index=index, columns=columns)
    bot_weights = pd.DataFrame(index=index, columns=columns)
    for date in tqdm(rebalance_dates):
        one_component = components.loc[date].values
        one_mkt_cap = mkt_cap.loc[date].values
        one_inv = inv.loc[date].values
        inv_upper = _get_percentile(one_inv, one_component.astype(bool), 90)
        inv_lower = _get_percentile(one_inv, one_component.astype(bool), 10)
        one_top = (one_inv >= inv_upper) * one_mkt_cap * one_component
        one_top[np.isnan(one_top)] = 0
        one_top = one_top / np.sum(np.abs(one_top))
        top_weights.loc[date] = one_top
        one_bot = (one_inv <= inv_lower) * one_mkt_cap * one_component
        one_bot[np.isnan(one_bot)] = 0
        one_bot = one_bot / np.sum(np.abs(one_bot))
        bot_weights.loc[date] = one_bot
    top_weights = top_weights.astype(np.float64)
    bot_weights = bot_weights.astype(np.float64)
    
    for i in range(1, len(index)):
        one_weight = top_weights.iloc[i].values
        if np.isnan(one_weight).all():
            last_weight = top_weights.iloc[i-1].values
            today_rtn = rtn.iloc[i].values
            tmp = last_weight * (1 + today_rtn)
            tmp[np.isnan(tmp)] = 0
            top_weights.iloc[i] = tmp / np.sum(np.abs(tmp))
    top_rtn = (top_weights.shift(1) * rtn).sum(axis=1)
    
    for i in range(1, len(index)):
        one_weight = bot_weights.iloc[i].values
        if np.isnan(one_weight).all():
            last_weight = bot_weights.iloc[i-1].values
            today_rtn = rtn.iloc[i].values
            tmp = last_weight * (1 + today_rtn)
            tmp[np.isnan(tmp)] = 0
            bot_weights.iloc[i] = tmp / np.sum(np.abs(tmp))
    bot_rtn = (bot_weights.shift(1) * rtn).sum(axis=1)
    cma = (bot_rtn - top_rtn).to_frame('CMA')
    
    payoff_df = pd.concat([riskfreerate, mkt_rtn, smb, hml, rmw, cma], axis=1)
    payoff_df.to_csv('data/payoffs/pricing_factors/FF5/weekly_ff5.csv')
    return
    
    
if __name__ == '__main__':
    # classic_ff5()
    # simplified_ff5()
    weekly_ff5()