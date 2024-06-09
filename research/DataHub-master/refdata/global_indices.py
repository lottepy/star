from lib.commonalgo.data.bbg_downloader import download_his

tickers = ['HSI INDEX', 'SPX INDEX', 'SHSZ300 INDEX', 'SHCOMP INDEX','MXWD INDEX']
# fields = ['PE_RATIO']
fields = ['PX_LAST']

data = download_his(tickers,fields,'1990-01-01', '2018-9-14',period='DAILY')
data.to_csv('global_indices.csv')