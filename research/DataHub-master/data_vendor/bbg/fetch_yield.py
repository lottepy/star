from lib.commonalgo.data.bbg_downloader import download_his,download_ref, download_intraday
import pandas as pd


# tickers = ['700 HK Equity', 'FB US Equity', 'TWTR US Equity','QQQ US Equity','SPX Index']
# fields = ['PX_LAST','DAY_TO_DAY_TOT_RETURN_GROSS_DVDS']
# tickers = ['2822IV Index', '2822 HK Equity', '2823IV Index', '2823 HK Equity']
# tickers = ['2813 HK EQUITY', '3010 HK EQUITY','3081 HK EQUITY','3101 HK EQUITY','3115 HK EQUITY',
#            '3140 HK EQUITY','3141 HK EQUITY','3147 HK EQUITY','3169 HK EQUITY']
tickers = ['NEAR US Equity', 'FLOT US Equity', 'MINT US Equity',
           'ERND LN EQUITY', 'ERNA LN EQUITY','FLOT LN EQUITY' ,'FLOA LN EQUITY',"MINT LN EQUITY"]

# tickers = ['INAVRNDU Index']
# tickers = ['SPBDUB3T Index', 'SBMMTB3 Index']
# fields = ['PX_LAST','PX_LOW','PX_HIGH','PX_OPEN','FUND_NET_ASSET_VAL','ETF_INAV_VALUE']
# fields = ['PX_LAST']
# fields = ['ETF_INAV_TICKER']
# fields_intraday = ['PX_LAST','ETF_INAV_VALUE']
# fields = ['HB_INDUSTRY_GROUP_ALLOC', 'HB_INDUSTRY_SECTOR_ALLOCATION']
# fields = ['EQY_DVD_YLD_IND_NET']

# data = download_ref(tickers,fields)

iopv_map = pd.read_csv('iopv_map.csv',index_col=0)['ETF_INAV_TICKER'].to_dict()

for ticker in tickers:
    iopv = iopv_map.get(ticker) + ' INDEX'
    ticker_data = download_intraday(ticker, 'TRADE', '2018-07-01', '2018-11-23', interval=10)
    ticker_data = pd.DataFrame(index=ticker_data['time'],data=ticker_data[['close','volume']].values, columns=['close','volume'])
    iopv_data = download_intraday(iopv, 'TRADE', '2018-07-01', '2018-11-23', interval=10)
    iopv_data = pd.Series(index=iopv_data['time'], data=iopv_data['close'].values).rename('iopv')
    data = pd.concat([ticker_data, iopv_data],axis=1,join ='inner')
    data.to_csv(ticker + '.csv')



# data = download_his(tickers,fields,'2018-07-01', '2018-11-23')
data = download_intraday(['INAVRNDU INDEX','FLOT'],'TRADE','2018-11-01', '2018-11-23',interval=10)
# data = download_intraday('VTI US EQUITY','TRADE','2018-11-01', '2018-11-23',interval=30)
print (data)



