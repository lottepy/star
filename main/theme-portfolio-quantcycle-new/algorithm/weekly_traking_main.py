import os
import pandas as pd
from datetime import datetime
from datamaster import dm_client
import requests
import json
from algorithm.utils.theme_backtest import theme_backtest
from constants import *


def update_data(portfolio_name):
    portfolio_path=os.path.join(addpath.data_path,"tracking_weight",portfolio_name,'portfolios')
    underlying_type=PARAMETER_ALGO[portfolio_name]['underlying_type']
    if underlying_type == "US_STOCK":
        rootpath = os.path.join(addpath.data_path, "us_data")
        r = requests.get(
            'https://algo-internal.aqumon.com/datamaster/exchange_calendar/?start_date=' + '2000-01-01' + '&end_date=' + '2022-12-31' + '&ex=' + 'NYSE')
        start_date = '2009-12-30'
    elif underlying_type == "HK_STOCK":
        rootpath = os.path.join(addpath.data_path, "hk_data")
        r = requests.get(
            'https://algo-internal.aqumon.com/datamaster/exchange_calendar/?start_date=' + '2000-01-01' + '&end_date=' + '2022-12-31' + '&ex=' + 'SEHK')
        start_date = '2014-12-30'
    elif underlying_type == "CN_STOCK":
        rootpath = os.path.join(addpath.data_path, "cn_data")
        r = requests.get(
            'https://algo-internal.aqumon.com/datamaster/exchange_calendar/?start_date=' + '2000-01-01' + '&end_date=' + '2022-12-31' + '&ex=' + 'SSE')
        start_date = '2015-12-30'

    user_info = r.content.decode()
    user_dict = json.loads(user_info)

    calendar = pd.DataFrame(user_dict)
    calendar.sort_values(by='data', inplace=True)
    calendar.index = pd.to_datetime(calendar['data'], format='%Y-%m-%d')

    output_path=os.path.join(rootpath,"trading")
    file_names=os.listdir(portfolio_path)
    if '.DS_Store' in file_names:
        file_names.remove('.DS_Store')
    rebalance_list = [datetime.strptime(ele[:-4], '%Y-%m-%d') for ele in file_names]
    last_reb_time=max(rebalance_list)
    symbol_list_all=[]
    dm_client.start()
    for file in file_names:
        symbol_list=pd.read_csv(os.path.join(portfolio_path,file),index_col=0)
        symbol_list_all.extend(symbol_list.index.tolist())

    last_reb_weight=pd.read_csv(os.path.join(portfolio_path,last_reb_time.strftime('%Y-%m-%d')+'.csv'),index_col=0)
    last_reb_weight=last_reb_weight[['weight']]
    last_reb_weight.index.name=last_reb_time.strftime('%Y-%m-%d')
    if underlying_type == "HK_STOCK":
        last_reb_weight.index = ['1105' + '0' * (6 - len(symbol[:-10])) + symbol[:-10] for symbol in last_reb_weight.index]
    elif underlying_type == "US_STOCK":
        last_reb_weight.index = ['US_10_' + symbol for symbol in last_reb_weight.index]


    # symbol_list_all=pd.read_csv(os.path.join(rootpath,'symbol_list.csv'))['symbol'].tolist()
    # start_date = '2009-12-30'
    nodata_symbol=[]
    for symbol in symbol_list_all:
        print(symbol)
        if underlying_type == "HK_STOCK":
            instrument_id='1105'+'0'*(6-len(symbol[:-10]))+symbol[:-10]
        elif underlying_type == "US_STOCK":
            instrument_id = 'US_10_'+ symbol
        else:
            instrument_id=symbol

        tmp = pd.DataFrame(
            columns=['date', 'PX_OPEN_RAW', 'PX_HIGH_RAW', 'PX_LOW_RAW', 'PX_LAST_RAW', 'PX_VOLUME_RAW',
                     'DAY_TO_DAY_TOT_RETURN_GROSS_DVDS', 'EQY_SH_OUT_RAW', 'MARKET_CAP',
                     'DAY_TO_DAY_TOT_RETURN_GROSS_DVDS_PRODUCT', 'adjust_factor', 'PX_LAST'])

        data = dm_client.historical(symbols=instrument_id, start_date=start_date,
                                    fields=['adjust_factor', 'choice_mktcap', 'volume', 'close',
                                            'open', 'high', 'low', 'bbg_daily_total_return','bbg_total_shares_outstanding'])
        data = pd.DataFrame(data['values'][instrument_id], columns=data['fields'])
        data=data.dropna(subset=['volume'])
        data['date'] = pd.to_datetime(data['date'])
        if len(data['adjust_factor'].dropna())==0:
            data['adjust_factor']=1
        else:
            data['adjust_factor']=data['adjust_factor'].ffill().bfill()

        tmp['date'] = data['date']
        tmp['PX_OPEN_RAW'] = data['open']
        tmp['PX_HIGH_RAW'] = data['high']
        tmp['PX_LOW_RAW'] = data['low']
        tmp['PX_LAST_RAW'] = data['close']
        tmp['PX_VOLUME_RAW'] = data['volume']
        tmp['MARKET_CAP'] = data['choice_mktcap']
        tmp['adjust_factor'] = data['adjust_factor']
        tmp['DAY_TO_DAY_TOT_RETURN_GROSS_DVDS'] = data['bbg_daily_total_return']
        tmp['PX_LAST'] = tmp['PX_LAST_RAW'] * tmp['adjust_factor']
        tmp['PX_VOLUME'] = tmp['PX_VOLUME_RAW']/tmp['adjust_factor']
        tmp['EQY_SH_OUT_RAW']=data['bbg_total_shares_outstanding']
        tmp['PX_LAST']=tmp['PX_LAST'].ffill()
        tmp['PX_VOLUME']=tmp['PX_VOLUME'].fillna(0)
        tmp.set_index('date',inplace=True)

        # data_start_date=max(tmp.index[0],calendar.index[0])
        # tmp = tmp.loc[calendar.loc[data_start_date:tmp.index[-1], :].index, :]

        tmp.to_csv(os.path.join(output_path, symbol + '.csv'))
        # print(tmp)
        if len(tmp)==0:
            print(symbol,'has no data in dm')
            nodata_symbol.append('US_10_'+symbol)
    print(nodata_symbol)

    REFERENCE={
        "US_STOCK":'SPX Index',
        "CN_STOCK": '000300.SH',
        "HK_STOCK": 'HSTECH Index'
    }
    symbol=REFERENCE[underlying_type]
    if portfolio_name in ['US_Tech_B','US_Tech_S','US_5G']:
        symbol ='CCMP Index'
    print(symbol)
    tmp = pd.DataFrame(
        columns=['date','market_index'])

    data = dm_client.historical(symbols=symbol, start_date=start_date,
                                fields=['close'])
    data = pd.DataFrame(data['values'][symbol], columns=data['fields'])
    data['date'] = pd.to_datetime(data['date'])
    tmp['date'] = data['date']
    tmp['market_index'] = data['close']
    if portfolio_name in ['US_Tech_B','US_Tech_S','US_5G']:
        tmp.to_csv(os.path.join(rootpath,"reference","market_index_nasdaq.csv"),index=False)
    else:
        tmp.to_csv(os.path.join(rootpath,"reference","market_index.csv"),index=False)
    # print(tmp)
    return last_reb_weight

def combine():
    REFERENCE={
        "US_STOCK":'SPX Index',
        "CN_STOCK": '000300.SH',
        "HK_STOCK": 'HSTECH Index'
    }
    portfolio_name_list = ['CN_Quality','CN_Value','HK_Hstech_B','HK_Hstech_S',
                           'US_Profit_B','US_Profit_S','US_Safety_B','US_Safety_S',
                           'US_Tech_B','US_Tech_S']
    strategy_kit_path = os.path.join(addpath.result_path,task,'strategy_kit')
    writer = pd.ExcelWriter(os.path.join(strategy_kit_path,'Theme portfolio.xlsx'), engine="xlsxwriter")
    combine_pv = pd.DataFrame()
    combine_weighting = pd.DataFrame()
    combine_benchmark = pd.DataFrame()
    for portfolio_name in portfolio_name_list:
        symbol = REFERENCE[PARAMETER_ALGO[portfolio_name]['underlying_type']]
        if portfolio_name in ['US_Tech_B', 'US_Tech_S', 'US_5G']:
            symbol = 'CCMP Index'
        combine_benchmark.loc[portfolio_name,'Benchmark'] = symbol
        file_path = os.path.join(strategy_kit_path,portfolio_name+'.xlsx')
        pv = pd.read_excel(file_path,sheet_name='pv',index_col=0)
        weighting = pd.read_excel(file_path, sheet_name='weighting', index_col=0)
        for index in pv.index:
            combine_pv.loc[index,portfolio_name]=pv.loc[index,100]
        if not symbol in combine_pv:
            combine_pv[symbol]=pv.iloc[:,1]
        for index in weighting.index:
            combine_weighting.loc[index,portfolio_name]=weighting.loc[index,100]
    combine_pv = combine_pv.reindex(columns=portfolio_name_list+['SPX Index','000300.SH','HSTECH Index','CCMP Index'])
    combine_pv.to_excel(writer, sheet_name='pv')
    combine_weighting=combine_weighting.fillna(0)
    combine_weighting.to_excel(writer, sheet_name='weighting')
    combine_benchmark.to_excel(writer, sheet_name='benchmark')
    writer.save()


if __name__ == "__main__":
    portfolio_name_list = ['CN_Quality','CN_Value','HK_Hstech_B','HK_Hstech_S',
                           'US_Profit_B','US_Profit_S','US_Safety_B','US_Safety_S',
                           'US_Tech_B','US_Tech_S']
    # portfolio_name_list = ['US_Profit_B','US_Profit_S','US_Safety_B','US_Safety_S',
    #                            'US_Tech_B','US_Tech_S']
    task='tracking'
    for portfolio_name in portfolio_name_list:
        # print(portfolio_name)
        update_tracking_file=os.path.join(addpath.result_path,task,'strategy_kit',portfolio_name+'.xlsx')
        writer=pd.ExcelWriter(update_tracking_file,engine="xlsxwriter")
        last_reb_weight=pd.read_excel(writer,sheet_name='weighting',index_col=0)
        last_reb_weight=update_data(portfolio_name)
        pv, report=theme_backtest(portfolio_name,task)
        last_reb_weight.columns=[100]
        last_reb_weight.to_excel(writer,sheet_name='weighting')
        pv=pv[pv.index>=datetime(2020,1,7)]
        pv=pv/pv.iloc[0,:]
        pv.columns=[100,'benchmark_100']
        pv.to_excel(writer,sheet_name='pv')
        writer.save()
    combine()







