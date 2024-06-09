from qtoolkit.data import choice_client, dm_client
import os
import pandas as pd
from tqdm import tqdm
import time
dm_client.start()


START_DATE = pd.to_datetime('2010-01-01')
END_DATE = pd.to_datetime('2020-11-30')
SSE_CALENDAR = pd.read_csv('data/TRADEDATE.csv', index_col=0, parse_dates=True)
STOCK_LIST = pd.read_csv('data/stockpools/ASHARE_COMPONENT.csv', index_col=0).columns.tolist()


def _find_last_update_date(path, default):
    try:
        last_update_date = pd.read_csv(path, index_col=0, parse_dates=True).index[-1]
    except FileNotFoundError:
        last_update_date = default
    return last_update_date


def _get_local(path, default_column='0'):
    if isinstance(default_column, str):
        default_column = [default_column]
    try:
        df = pd.read_csv(path, index_col=0, parse_dates=True)
    except FileNotFoundError:
        df = pd.DataFrame(columns=default_column)
    return df


def _concat_by_row(df_list):
    df = pd.concat(df_list, axis=0)
    df.sort_index(inplace=True)
    df = df.loc[~df.index.duplicated(keep='first')]
    return df


def bpsnew():
    # stock_list = pd.read_csv('data/stockpools/raw/ASHARENST.csv', index_col=0, parse_dates=True).columns.tolist()
    # for name in tqdm(stock_list[:2]):
    #     try:
    #         df = pd.read_csv(f'data/features/raw/bpsnew/{name}.csv', index_col=0, parse_dates=True)
    #         last_update_date = df.index[-1]
    #     except FileNotFoundError:
    #         df = pd.DataFrame()
    #         last_update_date = START_DATE
    #     last_update_date = max(last_update_date, START_DATE)
    #     date_list = [last_update_date] + pd.date_range(last_update_date, END_DATE, freq='W').tolist() + [END_DATE]
    #     df_list = [df]
    #     for date in tqdm(date_list):
    #         data = choice_client.css(name, 'BPSNEW', f'EndDate={date.strftime("%Y-%m-%d")}')
    #         new_df = pd.DataFrame(data.values[0, 0], index=[date], columns=['BPSNEW'])
    #         df_list.append(new_df)
    #     df = pd.concat(df_list, axis=0)
    #     df.sort_index(inplace=True)
    #     df = df.loc[~df.index.duplicated(keep='first')]
    #     df.to_csv(f'data/features/raw/bpsnew/{name}.csv')
    print("暂时不需要")


def download_shibor_3m():
    # last_update_date = _find_last_update_date('data/features/raw/Shibor3M.IR.csv', START_DATE)
    # data = choice_client.csd("Shibor3M.IR", "CLOSE", last_update_date.strftime("%Y-%m-%d"), END_DATE.strftime("%Y-%m-%d"),"period=1,adjustflag=1,curtype=1,order=1,market=CNSESH")
    # old_df = _get_local('data/features/raw/Shibor3M.IR.csv', 'CLOSE')
    # new_df = pd.DataFrame(data.values.flatten(), index=data.index, columns='CLOSE')
    # df = _concat_by_row([old_df, new_df])
    # df.to_csv('data/features/raw/Shibor3M.IR.csv')
    print("没有权限, 请手动从bbg下载")
    
    
def calc_riskfree_rate():
    df = _get_local('data/features/raw/Shibor3M.IR.csv', 'CLOSE')
    df['RF'] = df['CLOSE']/100/365
    df = df[['RF']]
    df.to_csv('data/features/riskfreerate.csv')
    

def download_ohlc():
    for name in tqdm(STOCK_LIST):
        path = f'data/features/raw/ohlc/{name}.csv'
        last_update_date = _find_last_update_date(path, START_DATE)
        try:
            data = dm_client.historical(symbols=name,
                                        fields='open,high,low,close',
                                        adjust_type=0,
                                        start_date=last_update_date.strftime("%Y-%m-%d"),
                                        end_date=END_DATE.strftime("%Y-%m-%d"),
                                        calendar='SSE')
            new_df = pd.DataFrame(data['values'][name], columns=data['fields'])
            new_df.set_index('date', drop=True, inplace=True)
            new_df.index = pd.to_datetime(new_df.index)
        except:
            new_df = pd.DataFrame(columns=['open', 'high', 'low', 'close'])
        old_df = _get_local(path, ['open', 'high', 'low', 'close'])
        df = _concat_by_row([old_df, new_df])
        df.to_csv(path)
        
        
def download_adjust_ohlc():
    for name in tqdm(STOCK_LIST):
        path = f'data/features/raw/adjust_ohlc/{name}.csv'
        last_update_date = _find_last_update_date(path, START_DATE)
        try:
            data = dm_client.historical(symbols=name,
                                        fields='open,high,low,close',
                                        adjust_type=3,
                                        start_date=last_update_date.strftime("%Y-%m-%d"),
                                        end_date=END_DATE.strftime("%Y-%m-%d"),
                                        calendar='SSE')
            new_df = pd.DataFrame(data['values'][name], columns=data['fields'])
            new_df.set_index('date', drop=True, inplace=True)
            new_df.index = pd.to_datetime(new_df.index)
            new_df.columns = ['adjust_open', 'adjust_high', 'adjust_low', 'adjust_close']
        except:
            new_df = pd.DataFrame(columns=['adjust_open', 'adjust_high', 'adjust_low', 'adjust_close'])
        old_df = _get_local(path, ['adjust_open', 'adjust_high', 'adjust_low', 'adjust_close'])
        df = _concat_by_row([old_df, new_df])
        df.to_csv(path)


def download_total_share():
    for name in tqdm(STOCK_LIST):
        path = f'data/features/raw/totalshare/{name}.csv'
        last_update_date = _find_last_update_date(path, START_DATE)
        try:
            data = dm_client.historical(symbols=name,
                                        fields='choice_total_share',
                                        start_date=last_update_date.strftime("%Y-%m-%d"),
                                        end_date=END_DATE.strftime("%Y-%m-%d"),
                                        calendar='SSE')
            new_df = pd.DataFrame(data['values'][name], columns=data['fields'])
            new_df.set_index('date', drop=True, inplace=True)
            new_df.index = pd.to_datetime(new_df.index)
            new_df.columns = ['totalshare']
        except:
            new_df = pd.DataFrame(columns=['totalshare'])
        old_df = _get_local(path, 'totalshare')
        df = _concat_by_row([old_df, new_df])
        df.to_csv(path)
        
        
def download_turnover():
    for name in tqdm(STOCK_LIST):
        path = f'data/features/raw/turnover/{name}.csv'
        last_update_date = _find_last_update_date(path, START_DATE)
        try:
            data = dm_client.historical(symbols=name,
                                        fields='choice_turnover',
                                        start_date=last_update_date.strftime("%Y-%m-%d"),
                                        end_date=END_DATE.strftime("%Y-%m-%d"),
                                        calendar='SSE')
            new_df = pd.DataFrame(data['values'][name], columns=data['fields'])
            new_df.set_index('date', drop=True, inplace=True)
            new_df.index = pd.to_datetime(new_df.index)
            new_df.columns = ['turnover']
        except:
            new_df = pd.DataFrame(columns=['turnover'])
        old_df = _get_local(path, 'turnover')
        df = _concat_by_row([old_df, new_df])
        df.to_csv(path)


def download_volume():
    for name in tqdm(STOCK_LIST):
        path = f'data/features/raw/volume/{name}.csv'
        last_update_date = _find_last_update_date(path, START_DATE)
        try:
            data = dm_client.historical(symbols=name,
                                        fields='volume',
                                        start_date=last_update_date.strftime("%Y-%m-%d"),
                                        end_date=END_DATE.strftime("%Y-%m-%d"),
                                        calendar='SSE')
            new_df = pd.DataFrame(data['values'][name], columns=data['fields'])
            new_df.set_index('date', drop=True, inplace=True)
            new_df.index = pd.to_datetime(new_df.index)
            new_df.columns = ['volume']
        except:
            new_df = pd.DataFrame(columns=['volume'])
        old_df = _get_local(path, 'volume')
        df = _concat_by_row([old_df, new_df])
        df.to_csv(path)
        

def download_amount():
    for name in tqdm(STOCK_LIST):
        path = f'data/features/raw/amount/{name}.csv'
        last_update_date = _find_last_update_date(path, START_DATE)
        try:
            data = dm_client.historical(symbols=name,
                                        fields='amount',
                                        start_date=last_update_date.strftime("%Y-%m-%d"),
                                        end_date=END_DATE.strftime("%Y-%m-%d"),
                                        calendar='SSE')
            new_df = pd.DataFrame(data['values'][name], columns=data['fields'])
            new_df.set_index('date', drop=True, inplace=True)
            new_df.index = pd.to_datetime(new_df.index)
            new_df.columns = ['amount']
        except:
            new_df = pd.DataFrame(columns=['amount'])
        old_df = _get_local(path, 'amount')
        df = _concat_by_row([old_df, new_df])
        df.to_csv(path)


def download_PB():
    for name in tqdm(STOCK_LIST):
        path = f'data/features/raw/PBLYR/{name}.csv'
        last_update_date = _find_last_update_date(path, START_DATE)
        try:
            data = choice_client.csd(name,
                                     "PBLYR",
                                     START_DATE.strftime("%Y-%m-%d"),
                                     END_DATE.strftime("%Y-%m-%d"),
                                     "period=3,adjustflag=1,curtype=1,order=1,market=CNSESH")
            new_df = data
            new_df.columns = ['PBLYR']
            new_df.index = pd.to_datetime(new_df.index)
        except:
            new_df = pd.DataFrame(columns=['PBLYR'])
        old_df = _get_local(path, 'PBLYR')
        df = _concat_by_row([old_df, new_df])
        df.to_csv(path)

    for name in tqdm(STOCK_LIST):
        path = f'data/features/raw/PBMRQ/{name}.csv'
        last_update_date = _find_last_update_date(path, START_DATE)
        try:
            data = choice_client.csd(name,
                                     "PBMRQ",
                                     START_DATE.strftime("%Y-%m-%d"),
                                     END_DATE.strftime("%Y-%m-%d"),
                                     "period=2,adjustflag=1,curtype=1,order=1,market=CNSESH")
            new_df = data
            new_df.columns = ['PBMRQ']
            new_df.index = pd.to_datetime(new_df.index)
        except:
            new_df = pd.DataFrame(columns=['PBMRQ'])
        old_df = _get_local(path, 'PBMRQ')
        df = _concat_by_row([old_df, new_df])
        df.to_csv(path)
    return


def download_financial_data():
    fields = ['BPS', 'OPTOGR', 'NITOGR', 'ORPS', 'GRPS', 'LIBILITYTOASSET', 'PNITTMR', 'EBITTTMR', 'GROSSMARGINTTMR', 'EVWITHOUTCASH', 'ROA', 'TANGIBLEASSETSTOASSET', 'OPTTMR', 'YOYEQUITY', 'YOYASSET', 'NCLTOLIBILITY', 'LONGLIBILITYTOWORKINGCAPITAL', 'WORKINGCAPITAL', 'CFOPS', 'ROEDILUTED']
    years = [i for i in range(START_DATE.year, END_DATE.year+1)]
    dates = []
    for y in years:
        dates.append(pd.to_datetime(f'{y}-03-31'))
        dates.append(pd.to_datetime(f'{y}-06-30'))
        dates.append(pd.to_datetime(f'{y}-09-30'))
        dates.append(pd.to_datetime(f'{y}-12-31'))
    dates = [i for i in dates if i<=END_DATE and i >= START_DATE]
    for field in fields[-1:]:
        one_field_df_list = []
        for date in tqdm(dates):
            one_date_df_list = []
            for i in tqdm(range(len(STOCK_LIST)//80+1)):
                symbols = ','.join(STOCK_LIST[i*80: (i+1)*80])
                data = choice_client.css(symbols, field, f"ReportDate={date.strftime('%Y-%m-%d')}")
                df = pd.DataFrame(data.values, index=[date], columns=data.columns.get_level_values(0))
                one_date_df_list.append(df)
            one_date_df = pd.concat(one_date_df_list, axis=1)
            one_field_df_list.append(one_date_df)
        one_field_df = pd.concat(one_field_df_list, axis=0)
        one_field_df.sort_index(inplace=True)
        one_field_df = one_field_df.loc[~one_field_df.index.duplicated(keep='first')]
        one_field_df.to_csv(f'data/features/raw/{field}.csv')
    return


def download_financial_data_ttm():
    fields = ['OPTTM', 'ROATTM', 'ROETTM', 'NITTM']
    years = [i for i in range(START_DATE.year, END_DATE.year+1)]
    dates = []
    for y in years:
        dates.append(pd.to_datetime(f'{y}-03-31'))
        dates.append(pd.to_datetime(f'{y}-06-30'))
        dates.append(pd.to_datetime(f'{y}-09-30'))
        dates.append(pd.to_datetime(f'{y}-12-31'))
    dates = [i for i in dates if i<=END_DATE and i >= START_DATE]
    for field in fields[-1:]:
        one_field_df_list = []
        for date in tqdm(dates):
            one_date_df_list = []
            for i in tqdm(range(len(STOCK_LIST)//80+1)):
                symbols = ','.join(STOCK_LIST[i*80: (i+1)*80])
                data = choice_client.css(symbols, field, f"TradeDate={date.strftime('%Y-%m-%d')},TTMType=1")
                df = pd.DataFrame(data.values, index=[date], columns=data.columns.get_level_values(0))
                one_date_df_list.append(df)
            one_date_df = pd.concat(one_date_df_list, axis=1)
            one_field_df_list.append(one_date_df)
        one_field_df = pd.concat(one_field_df_list, axis=0)
        one_field_df.sort_index(inplace=True)
        one_field_df = one_field_df.loc[~one_field_df.index.duplicated(keep='first')]
        one_field_df.to_csv(f'data/features/raw/{field}.csv')
    return
    
    
def download_industry():
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
    for name in tqdm(sector_map.keys()):
        path = f'data/features/raw/industry/{name}.csv'
        last_update_date = _find_last_update_date(path, START_DATE)
        dates = [last_update_date] + pd.date_range(last_update_date, END_DATE, freq='M').tolist() + [END_DATE]
        dates = sorted(list(set(dates)))
        new_df_list = []
        for date in tqdm(dates):
            counter = 0
            while(counter < 3):
                try:
                    data = choice_client.sector(name, date.strftime("%Y-%m-%d"))
                    break
                except Exception as e:
                    print(e)
                    time.sleep(2)
                    data = None
                counter += 1                                   
            if data is None:
                continue
            new_df_list.append(pd.DataFrame(1, columns=data[0].values, index=[date]))
        new_df = pd.concat(new_df_list, axis=0, sort=True)
        try:
            old_df = pd.read_csv(path, index_col=0, parse_dates=True)
        except FileNotFoundError:
            old_df = pd.DataFrame()
        df = _concat_by_row([old_df, new_df])
        df.to_csv(path)
    

if __name__ == '__main__':
    # bpsnew()
    # download_shibor_3m()
    # calc_riskfree_rate()
    # download_ohlc()
    # download_adjust_ohlc()
    # download_turnover()
    # download_total_share()
    # download_volume()
    # download_amount()
    # download_PB()
    download_financial_data()
    # download_financial_data_ttm()
    # download_industry()