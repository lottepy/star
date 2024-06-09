from lib.commonalgo.data.bbg_downloader import download_his,download_ref
# symbols = ['ECPIUS Q318 Index']
# fileds = ['PX_LAST']

# Monthly
# symbols = ['CPI YOY Index']
# fileds = ['PR291', 'PR292', 'PR295'] # Forecast
# data = download_his(symbols,fileds, start_date= '2004-01-01', end_date='2018-10-31', period='MONTHLY')
# data.to_csv('CPI_est.csv')


# fileds = ['PX_LAST'] # Real_Data
# data = download_his(symbols,fileds, start_date= '1959-01-01', end_date='2018-10-31', period='MONTHLY')
# data.to_csv('CPI.csv')
# Daily
# symbols = ['BNKKRYMH  Index', 'BNKKTRSY  Index', 'FDTR Index']
# fileds = ['PX_LAST']
# data = download_his(symbols,fileds, start_date= '2006-01-01', end_date='2018-10-31')
# data.to_csv('daily.csv')

# Daily
# symbols = ['H15T3M Index', 'H15T10Y Index', 'H15T5Y Index']
# fileds = ['PX_LAST']
# data = download_his(symbols,fileds, start_date= '1959-01-01', end_date='2018-10-31')
# data.to_csv('treasury_yield_curve.csv')