from lib.commonalgo.data.bbg_downloader import download_his,download_ref
import pandas as pd
tickers = ['2822 HK EQUITY',  '3140 HK EQUITY', '3141 HK EQUITY', 'VWO US Equity','SCHF US EQUITY', 'BNDX US EQUITY']
fields = ['HB_INDUSTRY_SECTOR_ALLOCATION',
          'HB_GEO_CNTRY_ALLOC',
          'HB_RATING_CLASS_ALLOCATION',
          'HB_MATURITY_BAND_ALLOCATION'
          ]

# for ticker in tickers:
for field in fields:
    dis_data = {}
    for ticker in tickers:
        try:
            data = download_ref(ticker, field)
            data = data[field].iloc[0]
            print(data)
            dis_data[ticker] = dict(zip(data[data.columns[0]],data[data.columns[1]]))
        except:
            pass
    fname = data.columns[0] + '_dis.csv'
    pd.DataFrame(dis_data).to_csv(fname)