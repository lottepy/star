import os
from os.path import join
from datamaster import dm_client
import pandas as pd
from algorithm import addpath
from choice_client import c

if __name__ == '__main__':
    data_path = join(addpath.root_path, 'data')
    hk_data_path = join(addpath.root_path, 'HK_data')
    ticker = pd.read_csv(os.path.join(data_path, 'ticker_industry.csv'))
    hk_symbol = pd.read_csv(os.path.join(hk_data_path, 'symbol_list.csv'))

    dm_client.start()

    for i in ticker['Stkcd']:
        print(i)
        try:
            tmp_data = dm_client.historical(symbols=i, start_date='2008-01-01',
                                            fields=['adjust_factor', 'choice_mktcap', 'volume', 'close',
                                                    'open', 'high', 'low'])
            tmp_data = pd.DataFrame(tmp_data['values'][i], columns=tmp_data['fields'])
            tmp_data['date'] = pd.to_datetime(tmp_data['date'])
            tmp_data.set_index('date', inplace=True)
            tmp_data = tmp_data.rename({'close': 'PX_LAST_RAW', 'open': 'PX_OPEN_RAW',
                                        'high': 'PX_HIGH_RAW', 'low': 'PX_LOW_RAW',
                                        'choice_mktcap':'MARKET_CAP', 'volume': 'PX_VOLUME_RAW'},axis=1)
            tmp_data['close'] = tmp_data['PX_LAST_RAW'] * tmp_data['adjust_factor']
            tmp_data['open'] = tmp_data['PX_OPEN_RAW'] * tmp_data['adjust_factor']
            tmp_data['high'] = tmp_data['PX_HIGH_RAW'] * tmp_data['adjust_factor']
            tmp_data['low'] = tmp_data['PX_LOW_RAW'] * tmp_data['adjust_factor']
            tmp_data = tmp_data[tmp_data['open'].notna()]
            tmp_data.to_csv(join(data_path, i + '.csv'))

        except:
            continue


    # data = pd.DataFrame()
    #
    # symbol = hk_symbol.iloc[0, 0]
    # data = c.csd(symbol, "CLOSE,VOLUME,TOTALSHARE, FRONTTAFACTOR", "2020-07-30", "2020-09-01",
    #              "BaseDate=2020-09-02,period=1,adjustflag=1,curtype=1,order=1,market=CNSESH")
    # data.index = pd.to_datetime(data.index, format='%Y/%m/%d', errors='ignore')
    # data['adjclose'] = data.iloc[:, 0] * data.iloc[:, 3]
    #
    # dataold = pd.read_csv(
    #     r'D:\AQUMON\ThemeProject\aqumon-theme-portfolio\data\cn_data\trading' + '\\' + symbol + '.csv')
    # dataold.loc[:, 'date'] = pd.to_datetime(dataold.iloc[:, 0], format='%Y-%m-%d', errors='ignore')
    #
    # dataout = pd.DataFrame(index=data.index, columns=dataold.columns)
    # dataout.loc[:, 'date'] = dataout.index
    # dataout.loc[:, 'PX_LAST'] = data.iloc[:, 0]
    # dataout.loc[:, 'PX_VOLUME'] = data.iloc[:, 1]
    # dataout.loc[:, 'EQY_SH_OUT'] = data.iloc[:, 2]
    # dataout.loc[:, 'DAY_TO_DAY_TOT_RETURN_GROSS_DVDS'] = data.iloc[:, 4] / data.iloc[:, 4].shift() - 1
    # dataout = dataout.drop(index=dataout.index[0])
    # dataout = pd.concat([dataold, dataout], axis=0)
    #
    # dataout.to_csv(r'D:\AQUMON\ThemeProject\aqumon-theme-portfolio\data\cn_data\trading' + '\\' + symbol + '.csv',
    #                index=False)
    #
    # #
    #
