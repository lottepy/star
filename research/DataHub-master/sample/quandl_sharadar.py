from requests import session
import pandas as pd
import time

_session =  session()

symbols = ['HD','DIS','MSFT','BA','MMM','NKE','JNJ','MCD','INTC','XOM']

for symbol in symbols:

    params = {
        'date.gte': '2018-01-01',
        'date.lte': '2018-10-17',
        'ticker': symbol,
        'api_key':'RbPmgLPow-aPRKpp6bC6'
    }

    endpoint = 'https://www.quandl.com/api/v3/datatables/SHARADAR/SEP.json?'

    result = _session.get(url=endpoint,
                          params =params).json()

    columns_names = [x.get('name') for x in result.get('datatable').get('columns')]

    data = pd.DataFrame(data = result.get('datatable').get('data'),
                        columns = columns_names)

    print (data.head())
    fname = 'log/sharadar_' + symbol + '.csv'
    data.to_csv(fname)
    time.sleep(0.5)

