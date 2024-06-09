from requests import session
import pandas as pd
import time

_session =  session()

symbols = ['HD','DIS','MSFT','BA','MMM','NKE','JNJ','MCD','INTC','XOM']

for symbol in symbols:

    endpoint = 'https://www.quandl.com/api/v3/datasets/EOD/{}?api_key=RbPmgLPow-aPRKpp6bC6'.format(symbol)

    result = _session.get(url=endpoint).json()

    data = pd.DataFrame(data = result.get('dataset').get('data'),
                        columns = result.get('dataset').get('column_names'))

    print (data.head())
    fname = symbol + '.csv'
    data.to_csv(fname)
    time.sleep(0.5)

