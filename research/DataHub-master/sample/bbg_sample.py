import utils
from lib.commonalgo.data.bbg_downloader import *

## Macro Data Download sample
# data = LocalTerminal.get_historical('GDP CQOQ Index',['ECO_RELEASE_DT','ACTUAL_RELEASE'],
#                                     '2018-01-01', '2019-09-10','',0,0,
#                                     RELEASE_STAGE_OVERRIDE='P').as_frame()

# data = LocalTerminal.get_reference_data('GDP CQOQ Index', 'ECO_RELEASE_DT,BN_SURVEY_MEDIAN', RELEASE_STAGE_OVER=False)

## reference data
ticker = '700 HK Equity'
tickers = ['2800 HK Equity', '2823 HK Equity']
fields = ['PX_LAST','MF_TOT_1D','CUR_MKT_CAP']
# data = download_ref(tickers,fields)

## historical data
# data = download_his(tickers,fields,'2020-12-01', '2020-1-17',DPDF=False)

# intraday bar data, min data
# data = download_intraday(ticker = ticker,start= '2019-12-01', end='2019-12-05',interval=1)

## intraday tick data, min data, 
## Huge Data, BE CAREFULL !!!
# data = download_intraday_tick(ticker = ticker,start= '2019-12-02', end='2019-12-02')


## screen, or EQS
# data =  download_screener('util_bbg01')
data.to_clipboard()
print (data)



