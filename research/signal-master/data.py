from datamaster import dm_client
import pandas as pd


if __name__ == '__main__':
    # dm_client.start()
    # symbols = ['AUDUSD1M Curncy', 'EURUSD1M Curncy', 'GBPUSD1M Curncy',
    #            'NZDUSD1M Curncy', 'CAD1M Curncy', 'CHF1M Curncy',
    #            'JPY1M Curncy', 'NOK1M Curncy', 'SEK1M Curncy', 'SGD1M Curncy',
    #            'THB1M Curncy']
    # data = dm_client.get_historical_data(symbols=symbols,
    #                                      start_date='2010-01-01',
    #                                      end_date='2020-07-31',
    #                                      fields='close',
    #                                      to_dataframe=True)
    # df_list = []
    # for sym in symbols:
    #     if len(sym) == 15:
    #         data[sym].set_index('date', inplace=True, drop=True)
    #         data[sym].rename(columns={'close': sym[:6]}, inplace=True)
    #     else:
    #         data[sym].set_index('date', inplace=True, drop=True)
    #         data[sym].rename(columns={'close': 'USD'+sym[:3]}, inplace=True)
    #     df_list.append(data[sym])
    # df = pd.concat(df_list, axis=1, sort=True)
    # df.to_csv('data/1MFP.csv')

    dm_client.start()
    symbols = ['AUD6M Curncy', 'EUR6M Curncy', 'GBP6M Curncy',
               'NZD6M Curncy', 'CAD6M Curncy', 'CHF6M Curncy',
               'JPY6M Curncy', 'NOK6M Curncy', 'SEK6M Curncy', 'SGD6M Curncy',
               'THB6M Curncy']
    data = dm_client.get_historical_data(symbols=symbols,
                                         start_date='2010-01-01',
                                         end_date='2020-07-31',
                                         fields='close',
                                         to_dataframe=True)
    df_list = []
    for sym in symbols:
        if sym in ['AUD6M Curncy', 'EUR6M Curncy', 'GBP6M Curncy',
                   'NZD6M Curncy']:
            data[sym].set_index('date', inplace=True, drop=True)
            data[sym].rename(columns={'close': sym[:3]+'USD'}, inplace=True)
        else:
            data[sym].set_index('date', inplace=True, drop=True)
            data[sym].rename(columns={'close': 'USD'+sym[:3]}, inplace=True)
        df_list.append(data[sym])
    df = pd.concat(df_list, axis=1, sort=True)
    df.to_csv('data/6MFP.csv')
