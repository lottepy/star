import os
import pandas as pd
from tqdm import tqdm
import numpy as np
import statsmodels.api as sm


START_DATE = pd.to_datetime('2010-01-01')
END_DATE = pd.to_datetime('2020-11-30')
SSE_CALENDAR = pd.read_csv('data/TRADEDATE.csv', index_col=0, parse_dates=True)
STOCK_LIST = pd.read_csv('data/stockpools/ASHARE_COMPONENT.csv', index_col=0).columns.tolist()


def _concat_by_col(df_list):
    df = pd.concat(df_list, axis=1, sort=True)
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


def _find_lookup_date(date):
    year = date.year
    month = date.month
    if month <= 4:
        return pd.to_datetime(f"{year-1}-09-30")
    elif month <= 8:
        return pd.to_datetime(f"{year}-03-31")
    elif month <= 10:
        return pd.to_datetime(f"{year}-06-30")
    else:
        return pd.to_datetime(f"{year}-09-30")


def _find_lookup_date_2(date):
    year = date.year
    month = date.month
    if month <= 4:
        return pd.to_datetime(f"{year-2}-12-31")
    else:
        return pd.to_datetime(f"{year-1}-12-31")


def _ffill_financial_quarterly_data(quarterly_data, target_index):
    target_df = pd.DataFrame(index = target_index, columns=quarterly_data.columns)
    for i in tqdm(range(len(target_df))):
        date = target_df.index[i]
        lookup_date = _find_lookup_date(_offset_trading_day(date, 1))
        if lookup_date < quarterly_data.index[0]:
            continue
        target_df.iloc[i] = quarterly_data.loc[lookup_date]
    return target_df


def _ffill_financial_annually_data(annually_data, target_index):
    target_df = pd.DataFrame(index = target_index, columns=annually_data.columns)
    for i in tqdm(range(len(target_df))):
        date = target_df.index[i]
        lookup_date = _find_lookup_date_2(_offset_trading_day(date, 1))
        if lookup_date < annually_data.index[0]:
            continue
        target_df.iloc[i] = annually_data.loc[lookup_date]
    return target_df


def _calc_quarterly_data_from_cumulative(quarterly_data):
    result_df = pd.DataFrame(columns=quarterly_data.columns, index=quarterly_data.index)
    for i in range(len(result_df.index)):
        date = result_df.index[i]
        if date.month == 3:
            result_df.iloc[i] = quarterly_data.iloc[i]
        else:
            try:
                result_df.iloc[i] = quarterly_data.iloc[i] - quarterly_data.iloc[i-1]
            except:
                pass
    return result_df


def combine_rtn():
    df_list = []
    for name in tqdm(STOCK_LIST):
        try:
            df = pd.read_csv(f'data/features/raw/adjust_close/{name}.csv', index_col=0, parse_dates=True)
            df.columns = [name]
            df[name] = df[name].pct_change()
        except:
            df = pd.DataFrame(columns=[name])
        df_list.append(df)
    combined_df = _concat_by_col(df_list)
    combined_df.to_csv('data/features/rtn.csv')
    

def combine_log_rtn():
    df_list = []
    for name in tqdm(STOCK_LIST):
        try:
            df = pd.read_csv(f'data/features/raw/adjust_close/{name}.csv', index_col=0, parse_dates=True)
            df.columns = [name]
            df[name] = np.log(df[name]).diff()
        except:
            df = pd.DataFrame(columns=[name])
        df_list.append(df)
    combined_df = _concat_by_col(df_list)
    combined_df.to_csv('data/features/log_rtn.csv')


def combine_adjust_close():
    df_list = []
    for name in tqdm(STOCK_LIST):
        try:
            df = pd.read_csv(f'data/features/raw/adjust_close/{name}.csv', index_col=0, parse_dates=True)
            df.columns = [name]
        except:
            df = pd.DataFrame(columns=[name])
        df_list.append(df)
    combined_df = _concat_by_col(df_list)
    combined_df.to_csv('data/features/adjust_close.csv')
    
    
def combine_turnover():
    df_list = []
    for name in tqdm(STOCK_LIST):
        try:
            df = pd.read_csv(f'data/features/raw/turnover/{name}.csv', index_col=0, parse_dates=True)
            df.columns = [name]
            df = df/100
        except:
            df = pd.DataFrame(columns=[name])
        df_list.append(df)
    combined_df = _concat_by_col(df_list)
    combined_df.to_csv('data/features/turnover.csv')


def combine_volume():
    df_list = []
    for name in tqdm(STOCK_LIST):
        try:
            df = pd.read_csv(f'data/features/raw/volume/{name}.csv', index_col=0, parse_dates=True)
            df.columns = [name]
        except:
            df = pd.DataFrame(columns=[name])
        df_list.append(df)
    combined_df = _concat_by_col(df_list)
    combined_df.to_csv('data/features/volume.csv')


def combine_amount():
    df_list = []
    for name in tqdm(STOCK_LIST):
        try:
            df = pd.read_csv(f'data/features/raw/amount/{name}.csv', index_col=0, parse_dates=True)
            df.columns = [name]
        except:
            df = pd.DataFrame(columns=[name])
        df_list.append(df)
    combined_df = _concat_by_col(df_list)
    combined_df.to_csv('data/features/amount.csv')


def check_rtn():
    # 连续交易的日期价格波动都应该在10%以内, 科创板688xxx在20%以内
    print("All a shares")
    ashare = pd.read_csv('data/stockpools/ASHARE_COMPONENT.csv', index_col=0, parse_dates=True)
    rtn = pd.read_csv('data/features/rtn.csv', index_col=0, parse_dates=True)
    volume = pd.read_csv('data/features/volume.csv', index_col=0, parse_dates=True)
    index = [i for i in ashare.index if i in rtn.index]
    rtn = rtn.loc[index, ashare.columns]
    volume = volume.loc[index, ashare.columns]
    volume = volume.fillna(0)
    flag = volume.astype(bool) & volume.shift(1).fillna(0).astype(bool)
    rtn_ = flag.astype(int) * rtn
    rtn_ = rtn_ * ashare.astype(int)
    main_board = [i for i in rtn.columns if i[:3] not in ['688', '300']]
    star_board = [i for i in rtn.columns if i[:3] == '688']
    chinext_board = [i for i in rtn.columns if i[:3] == '300']
    print(rtn_.loc[:, main_board].max().max())
    print(rtn_.loc[:, main_board].min().min())
    
    print("CSI800")
    ashare = pd.read_csv('data/stockpools/CSI800_COMPONENT.csv', index_col=0, parse_dates=True)
    rtn = pd.read_csv('data/features/rtn.csv', index_col=0, parse_dates=True)
    volume = pd.read_csv('data/features/volume.csv', index_col=0, parse_dates=True)
    index = [i for i in ashare.index if i in rtn.index]
    rtn = rtn.loc[index, ashare.columns]
    volume = volume.loc[index, ashare.columns]
    volume = volume.fillna(0)
    flag = volume.astype(bool) & volume.shift(1).fillna(0).astype(bool)
    rtn_ = flag.astype(int) * rtn
    rtn_ = rtn_ * ashare.astype(int)
    main_board = [i for i in rtn.columns if i[:3] not in ['688', '300']]
    star_board = [i for i in rtn.columns if i[:3] == '688']
    chinext_board = [i for i in rtn.columns if i[:3] == '300']
    print(rtn_.loc[:, main_board].max().max())
    print(rtn_.loc[:, main_board].min().min())
    return
    
    
def combine_mkt_cap():
    df_list = []
    for name in tqdm(STOCK_LIST):
        try:
            df = pd.read_csv(f'data/features/raw/close/{name}.csv', index_col=0, parse_dates=True)
            df.columns = [name]
        except:
            df = pd.DataFrame(columns=[name])
        df_list.append(df)
    close_ = _concat_by_col(df_list)
    
    df_list = []
    for name in tqdm(STOCK_LIST):
        try:
            df = pd.read_csv(f'data/features/raw/totalshare/{name}.csv', index_col=0, parse_dates=True)
            df.columns = [name]
        except:
            df = pd.DataFrame(columns=[name])
        df_list.append(df)
    totalshare = _concat_by_col(df_list)
    (close_ * totalshare).to_csv('data/features/marketcap.csv')
    return    
    
    
def combine_totalshare():
    df_list = []
    for name in tqdm(STOCK_LIST):
        try:
            df = pd.read_csv(f'data/features/raw/totalshare/{name}.csv', index_col=0, parse_dates=True)
            df.columns = [name]
        except:
            df = pd.DataFrame(columns=[name])
        df_list.append(df)
    totalshare = _concat_by_col(df_list)
    totalshare.to_csv('data/features/totalshare.csv')

    
def derive_net_asset_from_pblyr_or_pbmrq():
    df_list = []
    for name in tqdm(STOCK_LIST):
        try:
            df = pd.read_csv(f'data/features/raw/PBLYR/{name}.csv', index_col=0, parse_dates=True)
            df.columns = [name]
        except:
            df = pd.DataFrame(columns=[name])
        df_list.append(df)
    pblyr = _concat_by_col(df_list)
    
    mkt_cap = pd.read_csv('data/features/marketcap.csv', index_col=0, parse_dates=True)

    netassetlyr = pd.DataFrame(index=pblyr.index, columns=pblyr.columns)
    for i in pblyr.index:
        netassetlyr.loc[i] = mkt_cap.loc[i] / pblyr.loc[i]
    netassetlyr = pd.concat([netassetlyr, pd.DataFrame(index=SSE_CALENDAR.loc[START_DATE: END_DATE].index)], axis=1).ffill()
    netassetlyr.to_csv('data/features/netassetlyr.csv')
    
    df_list = []
    for name in tqdm(STOCK_LIST):
        try:
            df = pd.read_csv(f'data/features/raw/PBMRQ/{name}.csv', index_col=0, parse_dates=True)
            df.columns = [name]
        except:
            df = pd.DataFrame(columns=[name])
        df_list.append(df)
    pbmrq = _concat_by_col(df_list)
    
    mkt_cap = pd.read_csv('data/features/marketcap.csv', index_col=0, parse_dates=True)

    netassetmrq = pd.DataFrame(index=pbmrq.index, columns=pbmrq.columns)
    for i in pbmrq.index:
        netassetmrq.loc[i] = mkt_cap.loc[i] / pbmrq.loc[i]
    netassetmrq = pd.concat([netassetmrq, pd.DataFrame(index=SSE_CALENDAR.loc[START_DATE: END_DATE].index)], axis=1).ffill()
    netassetmrq.to_csv('data/features/netassetmrq.csv')
    return
    
    
def derive_net_asset():
    bps = pd.read_csv('data/features/raw/BPS.csv',index_col=0, parse_dates=True)
    totalshare = pd.read_csv('data/features/totalshare.csv', index_col=0, parse_dates=True)
    totalshare = pd.concat([totalshare, pd.DataFrame(index=bps.index)], axis=1, sort=True)
    totalshare = totalshare.ffill()
    netasset = bps * totalshare.loc[bps.index, bps.columns]
    netasset.to_csv('data/features/netasset.csv')
    return


def derive_operating_revenue():
    orps = pd.read_csv('data/features/raw/ORPS.csv',index_col=0, parse_dates=True)
    totalshare = pd.read_csv('data/features/totalshare.csv', index_col=0, parse_dates=True)
    totalshare = pd.concat([totalshare, pd.DataFrame(index=orps.index)], axis=1, sort=True)
    totalshare = totalshare.ffill()
    operating_revenue = orps * totalshare.loc[orps.index, orps.columns]
    operating_revenue = _calc_quarterly_data_from_cumulative(operating_revenue)
    operating_revenue.to_csv('data/features/operating_revenue.csv')
    return


def derive_assetsturn():
    operating_revenue = pd.read_csv('data/features/operating_revenue.csv', index_col=0, parse_dates=True)
    totalasset = pd.read_csv('data/features/totalasset.csv', index_col=0, parse_dates=True)
    assetsturn = operating_revenue / totalasset.rolling(2).mean()
    assetsturn = _ffill_financial_quarterly_data(assetsturn, SSE_CALENDAR.loc[START_DATE: END_DATE].index)
    assetsturn.to_csv('data/features/assetsturn.csv')


def derive_bm():
    mkt_cap = pd.read_csv('data/features/marketcap.csv', index_col=0, parse_dates=True)
    netassetlyr = pd.read_csv('data/features/netassetlyr.csv', index_col=0, parse_dates=True)
    netassetmrq = pd.read_csv('data/features/netassetmrq.csv', index_col=0, parse_dates=True)
    bmlyr = netassetlyr / mkt_cap
    bmmrq = netassetmrq / mkt_cap
    bmlyr.to_csv('data/features/bmlyr.csv')
    bmmrq.to_csv('data/features/bmmrq.csv')
    
    netasset = pd.read_csv('data/features/netasset.csv', index_col=0, parse_dates=True)
    netasset_ = _ffill_financial_quarterly_data(netasset, mkt_cap.index)
    bm = netasset_ / mkt_cap
    bm.to_csv('data/features/bm.csv') # 按照保守估计最近的财报和最新市值计算
    return
    
    
def derive_op():
    # operating profit ttm / netasset
    op_ = pd.read_csv('data/features/raw/OPTTM.csv', index_col=0, parse_dates=True) # 也可以选择 OPTTMR, OPTTM
    netasset = pd.read_csv('data/features/netasset.csv', index_col=0, parse_dates=True)
    op = op_ / netasset.rolling(4).mean()
    op = _ffill_financial_quarterly_data(op, SSE_CALENDAR.loc[START_DATE: END_DATE].index)
    op.to_csv('data/features/op.csv')
    return


def derive_total_asset():
    netasset = pd.read_csv('data/features/netasset.csv', index_col=0, parse_dates=True)
    l2a = pd.read_csv('data/features/raw/LIBILITYTOASSET.csv', index_col=0, parse_dates=True)
    totalasset = netasset / (1 - l2a/100)
    # totalasset = _ffill_financial_quarterly_data(totalasset, SSE_CALENDAR.loc[START_DATE: END_DATE].index)
    totalasset.to_csv('data/features/totalasset.csv')
    return


def derive_INV():
    netasset = pd.read_csv('data/features/netasset.csv', index_col=0, parse_dates=True)
    totalasset = pd.read_csv('data/features/totalasset.csv', index_col=0, parse_dates=True)
    inv_net = netasset / netasset.shift(4) - 1
    inv_total = totalasset / totalasset.shift(4) - 1
    inv_net = _ffill_financial_quarterly_data(inv_net, SSE_CALENDAR.loc[START_DATE: END_DATE].index)
    inv_total = _ffill_financial_quarterly_data(inv_total, SSE_CALENDAR.loc[START_DATE: END_DATE].index)
    inv_net.to_csv('data/features/inv_net.csv')
    inv_total.to_csv('data/features/inv_total.csv')


def derive_INV_directly():
    yoyequity = pd.read_csv('data/features/raw/YOYEQUITY.csv', index_col=0, parse_dates=True)
    yoyequity = _ffill_financial_quarterly_data(yoyequity, SSE_CALENDAR.loc[START_DATE: END_DATE].index)
    yoyequity.to_csv('data/features/inv_net_directly.csv')
    yoyasset = pd.read_csv('data/features/raw/YOYASSET.csv', index_col=0, parse_dates=True)
    yoyasset = _ffill_financial_quarterly_data(yoyasset, SSE_CALENDAR.loc[START_DATE: END_DATE].index)
    yoyasset.to_csv('data/features/inv_total_directly.csv')

    
def derive_industry():
    sector_map = {
        "011001": "农林牧渔",
        "011002": "采掘",
        "011003": "化工",
        "011004": "钢铁",
        "011005": "有色金属",
        "011006": "电子",
        "011007": "汽车",
        "011008": "家用电器",
        "011009": "食品饮料",
        "011010": "纺织服装",
        "011011": "轻工制造",
        "011012": "医药生物",
        "011013": "公用事业",
        "011014": "交通运输",
        "011015": "房地产",
        "011016": "商业贸易",
        "011017": "休闲服务",
        "011018": "银行",
        "011019": "非银金融",
        "011020": "综合",
        "011021": "建筑材料",
        "011022": "建筑装饰",
        "011023": "电气设备",
        "011024": "机械设备",
        "011025": "国防军工",
        "011026": "计算机",
        "011027": "传媒",
        "011028": "通信"
    }
    n_map = {}
    df_dict = {}
    for k, v in sector_map.items():
        df = pd.read_csv(f'data/features/raw/industry/{k}.csv', index_col=0, parse_dates=True).fillna(0)
        df = df * int(k[-2:])
        df_dict[k] = df
    
    columns = []
    dates = []
    for v in df_dict.values():
        columns += v.columns.tolist()
        dates += v.index.tolist()
    columns = sorted(list(set(columns)))
    dates = sorted(list(set(dates)))
    
    industry = pd.DataFrame(0, index=dates, columns=columns)
    for k in sector_map.keys():
        tmp = pd.DataFrame(index=dates, columns=columns)
        tmp.update(df_dict[k])
        industry += tmp.fillna(0)
    industry = industry.loc[pd.to_datetime("2014-01-01"):]
    industry.to_csv('data/features/industry.csv')
    
    industry_name = pd.DataFrame(columns=['Name'])
    for k,v in sector_map.items():
        industry_name.loc[int(k[-2:])] = v
    industry_name.to_csv('data/features/industry_name.csv')
    return
        
        
def derive_roa_roe():
    roa = pd.read_csv('data/features/raw/ROATTM.csv', index_col=0, parse_dates=True)
    roa = _ffill_financial_quarterly_data(roa, SSE_CALENDAR.loc[START_DATE: END_DATE].index)
    roa.to_csv('data/features/roa.csv')
    roe = pd.read_csv('data/features/raw/ROETTM.csv', index_col=0, parse_dates=True)
    roe = _ffill_financial_quarterly_data(roe, SSE_CALENDAR.loc[START_DATE: END_DATE].index)
    roe.to_csv('data/features/roe.csv')
    
    
def derive_roa_roe_yoy():
    roa = pd.read_csv('data/features/raw/ROA.csv', index_col=0, parse_dates=True)
    roa = _calc_quarterly_data_from_cumulative(roa)
    roa = _ffill_financial_quarterly_data(roa-roa.shift(4), SSE_CALENDAR.loc[START_DATE: END_DATE].index)
    roa.to_csv('data/features/roayoy.csv')
    
    roe = pd.read_csv('data/features/raw/ROEDILUTED.csv', index_col=0, parse_dates=True)
    roe = _calc_quarterly_data_from_cumulative(roe)
    roe = _ffill_financial_quarterly_data(roe-roe.shift(4), SSE_CALENDAR.loc[START_DATE: END_DATE].index)
    roe.to_csv('data/features/roeyoy.csv')
    
    
def derive_net_income():
    nitogr = pd.read_csv('data/features/raw/NITOGR.csv', index_col=0, parse_dates=True)
    grps = pd.read_csv('data/features/raw/GRPS.csv', index_col=0, parse_dates=True)
    totalshare = pd.read_csv('data/features/totalshare.csv', index_col=0, parse_dates=True)
    totalshare = pd.concat([totalshare, pd.DataFrame(index=grps.index)], axis=1, sort=True)
    totalshare = totalshare.ffill()
    ni = nitogr * grps * totalshare.loc[grps.index, grps.columns] / 100
    ni = _calc_quarterly_data_from_cumulative(ni)
    ni.to_csv('data/features/net_income.csv')
    
    
def derive_ep():
    ni = pd.read_csv('data/features/net_income.csv', index_col=0, parse_dates=True)
    mkt_cap = pd.read_csv('data/features/marketcap.csv', index_col=0, parse_dates=True)
    
    ep = _ffill_financial_quarterly_data(ni, SSE_CALENDAR.loc[START_DATE: END_DATE].index) / mkt_cap
    ep_ttm = _ffill_financial_quarterly_data(ni.rolling(4).sum(), SSE_CALENDAR.loc[START_DATE: END_DATE].index) / mkt_cap
    
    ep.to_csv('data/features/ep.csv')
    ep_ttm.to_csv('data/features/ep_ttm.csv')
    
    
def derive_sp():
    op = pd.read_csv('data/features/operating_revenue.csv', index_col=0, parse_dates=True)
    mkt_cap = pd.read_csv('data/features/marketcap.csv', index_col=0, parse_dates=True)
    
    sp = _ffill_financial_quarterly_data(op, SSE_CALENDAR.loc[START_DATE: END_DATE].index) / mkt_cap
    sp_ttm = _ffill_financial_quarterly_data(op.rolling(4).sum(), SSE_CALENDAR.loc[START_DATE: END_DATE].index) / mkt_cap
    
    sp.to_csv('data/features/sp.csv')
    sp_ttm.to_csv('data/features/sp_ttm.csv')
    
    
def derive_beta_residual():
    rtn = pd.read_csv('data/features/rtn.csv', index_col=0, parse_dates=True)
    rtn.columns = [i[-2:]+i[:6] for i in rtn.columns]
    tmp = pd.read_csv('data/payoffs/pricing_factors/CAPM/classic_capm.csv', index_col=0, parse_dates=True)
    rf = tmp[['RF']]
    mkt = tmp[['MKT']]
    excess_rtn = rtn - rf.values
    excess_mkt = mkt - rf.values
    data = pd.concat([excess_rtn, excess_mkt], axis=1)
    beta = pd.DataFrame(index=rtn.index, columns=rtn.columns)
    resid = pd.DataFrame(index=rtn.index, columns=rtn.columns)
    for i in tqdm(range(251, len(data))):
        one_data = data.iloc[i-251: i+1]
        for col in tqdm(excess_rtn.columns):
            if one_data[col].isnull().sum() < 50:
                res = sm.OLS.from_formula(f'{col} ~ 1 + MKT', one_data).fit()
                beta.loc[data.index[i], col] = res.params['MKT']
                resid.loc[data.index[i], col] = res.resid.iloc[-1]
    beta.columns = [f'{i[-6:]}.{i[:2]}' for i in beta.columns]
    resid.columns = [f'{i[-6:]}.{i[:2]}' for i in resid.columns]
    beta.to_csv('data/features/beta.csv')
    resid.to_csv('data/features/idio_resid.csv')
    return


def derive_brr_leverage():
    asset = pd.read_csv('data/features/totalasset.csv', index_col=0, parse_dates=True)
    equity = pd.read_csv('data/features/netasset.csv', index_col=0, parse_dates=True)
    liability = asset - equity
    # wc = pd.read_csv('data/features/raw/WORKINGCAPITAL.csv', index_col=0, parse_dates=True)
    # ld2wc = pd.read_csv('data/features/raw/LONGLIBILITYTOWORKINGCAPITAL.csv', index_col=0, parse_dates=True)
    ncl2l = pd.read_csv('data/features/raw/NCLTOLIBILITY.csv', index_col=0, parse_dates=True)
    ncl = liability * ncl2l / 100
    
    me = pd.read_csv('data/features/marketcap.csv', index_col=0, parse_dates=True)
    # MLEV
    mlev = (me + _ffill_financial_quarterly_data(ncl, SSE_CALENDAR.loc[START_DATE: END_DATE].index))/me
    # DTOA
    dtoa = _ffill_financial_quarterly_data(liability/asset, SSE_CALENDAR.loc[START_DATE: END_DATE].index)
    # BLEV
    blev = _ffill_financial_quarterly_data((equity+ncl)/equity, SSE_CALENDAR.loc[START_DATE: END_DATE].index)
    mlev.to_csv('data/features/mlev.csv')
    dtoa.to_csv('data/features/dtoa.csv')
    blev.to_csv('data/features/blev.csv')
    return


def derive_leverage_ratio():
    asset = pd.read_csv('data/features/totalasset.csv', index_col=0, parse_dates=True)
    equity = pd.read_csv('data/features/netasset.csv', index_col=0, parse_dates=True)
    liability = asset - equity
    
    _ffill_financial_quarterly_data(liability/equity, SSE_CALENDAR.loc[START_DATE: END_DATE].index).to_csv('data/features/debt2equity.csv')
    _ffill_financial_quarterly_data(liability/asset, SSE_CALENDAR.loc[START_DATE: END_DATE].index).to_csv('data/features/debt2asset.csv')
    _ffill_financial_quarterly_data(equity/asset, SSE_CALENDAR.loc[START_DATE: END_DATE].index).to_csv('data/features/equity2asset.csv')
    
    
    
def derive_CFO():
    cfops = pd.read_csv('data/features/raw/CFOPS.csv', index_col=0, parse_dates=True)
    totalshare = pd.read_csv('data/features/totalshare.csv', index_col=0, parse_dates=True)
    totalshare = pd.concat([totalshare, pd.DataFrame(index=cfops.index)], axis=1, sort=True)
    totalshare = totalshare.ffill()
    cfo = cfops * totalshare.loc[cfops.index, cfops.columns]
    cfo = _calc_quarterly_data_from_cumulative(cfo)
    cfo.to_csv('data/features/cfo.csv')
    return
    
    
def derive_growth_ratio():
    net_income = pd.read_csv('data/features/net_income.csv', index_col=0, parse_dates=True)
    operating_revenue = pd.read_csv('data/features/operating_revenue.csv', index_col=0, parse_dates=True)
    cfo = pd.read_csv('data/features/cfo.csv', index_col=0, parse_dates=True)
    
    _ffill_financial_quarterly_data((net_income/net_income.shift(1)).replace({np.inf: np.nan, -np.inf: np.nan}), SSE_CALENDAR.loc[START_DATE: END_DATE].index).to_csv('data/features/niqoq.csv')
    _ffill_financial_quarterly_data((net_income/net_income.shift(4)).replace({np.inf: np.nan, -np.inf: np.nan}), SSE_CALENDAR.loc[START_DATE: END_DATE].index).to_csv('data/features/niyoy.csv')
    
    _ffill_financial_quarterly_data((operating_revenue/operating_revenue.shift(1)).replace({np.inf: np.nan, -np.inf: np.nan}), SSE_CALENDAR.loc[START_DATE: END_DATE].index).to_csv('data/features/orqoq.csv')
    _ffill_financial_quarterly_data((operating_revenue/operating_revenue.shift(4)).replace({np.inf: np.nan, -np.inf: np.nan}), SSE_CALENDAR.loc[START_DATE: END_DATE].index).to_csv('data/features/oryoy.csv')
    
    _ffill_financial_quarterly_data((cfo/cfo.shift(4)).replace({np.inf: np.nan, -np.inf: np.nan}), SSE_CALENDAR.loc[START_DATE: END_DATE].index).to_csv('data/features/ocfyoy.csv')
    
    
def derive_egro_sgro():
    net_income = pd.read_csv('data/features/net_income.csv', index_col=0, parse_dates=True)
    operating_revenue = pd.read_csv('data/features/operating_revenue.csv', index_col=0, parse_dates=True)

    net_income_annu = net_income.loc[[i for i in net_income.index if i.month == 12]]
    operating_revenue_annu = operating_revenue.loc[[i for i in operating_revenue.index if i.month == 12]]
    
    egro = pd.DataFrame(index=net_income_annu.index, columns=net_income_annu.columns)
    for i in tqdm(range(4, len(net_income_annu))):
        one_net_income_annu = net_income_annu.iloc[i-4: i+1]
        for col in tqdm(net_income.columns):
            if one_net_income_annu[col].isnull().sum() == 0:
                Y = one_net_income_annu[col]
                X = sm.add_constant(np.arange(5))
                res = sm.OLS(Y, X).fit()
                egro.loc[net_income_annu.index[i], col] = res.params.values[-1] / Y.mean()
    _ffill_financial_annually_data(egro, SSE_CALENDAR.loc[START_DATE: END_DATE].index).to_csv('data/features/egro.csv')
                
    sgro = pd.DataFrame(index=operating_revenue_annu.index, columns=operating_revenue_annu.columns)
    for i in tqdm(range(4, len(operating_revenue_annu))):
        one_operating_revenue_annu = operating_revenue_annu.iloc[i-4: i+1]
        for col in tqdm(operating_revenue.columns):
            if one_operating_revenue_annu[col].isnull().sum() == 0:
                Y = one_operating_revenue_annu[col]
                X = sm.add_constant(np.arange(5))
                res = sm.OLS(Y, X).fit()
                sgro.loc[operating_revenue_annu.index[i], col] = res.params.values[-1] / Y.mean()
    _ffill_financial_annually_data(sgro, SSE_CALENDAR.loc[START_DATE: END_DATE].index).to_csv('data/features/sgro.csv')
                
                
def derive_ebit2ev_gp2ev():
    ebit = pd.read_csv('data/features/raw/EBITTTMR.csv', index_col=0, parse_dates=True)
    gp = pd.read_csv('data/features/raw/GROSSMARGINTTMR.csv', index_col=0, parse_dates=True)
    ev = pd.read_csv('data/features/raw/EVWITHOUTCASH.csv', index_col=0, parse_dates=True)
    
    _ffill_financial_quarterly_data(ebit/ev, SSE_CALENDAR.loc[START_DATE: END_DATE].index).to_csv('data/features/ebit2ev.csv')
    _ffill_financial_quarterly_data(gp/ev, SSE_CALENDAR.loc[START_DATE: END_DATE].index).to_csv('data/features/gp2ev.csv')
    
    
if __name__ == '__main__':
    # combine_rtn()
    # combine_log_rtn()
    # combine_adjust_close()
    # combine_turnover()
    # combine_volume()
    # combine_amount()
    # check_rtn()
    # combine_mkt_cap()
    # combine_totalshare()
    # derive_net_asset()
    # derive_operating_revenue()
    # derive_assetsturn()
    # derive_bm()
    # derive_op()
    # derive_total_asset()
    # derive_INV()
    # derive_INV_directly()
    # derive_industry()
    # derive_roa_roe()
    # derive_roa_roe_yoy()
    # derive_net_income()
    # derive_ep()
    # derive_sp()
    # derive_beta_residual()
    # derive_brr_leverage()
    # derive_leverage_ratio()
    # derive_CFO()
    derive_growth_ratio()
    # derive_egro_sgro()
    # derive_ebit2ev_gp2ev()