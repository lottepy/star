import requests
import pandas as pd

data = pd.read_csv('uk_etflist.csv')

seclist = data['SecId'].tolist()

_session = requests.session()

def download(url, filename):
    try:
        u = _session.get(url)

        file_size = int(u.headers['content-length'])
        print("Downloading: %s Bytes: %s" % (filename, file_size))
        with open(filename, 'wb') as f:
            for chunk in u.iter_content(chunk_size=1024):
                if chunk:  # filter out keep-alive new chunks
                    f.write(chunk)
                    f.flush()
        f.close()
    except:
        pass

url = 'http://edw.morningstar.com/GetDictionaryXML.aspx?ClientId=magnumhk&DicType=TYPECODE&Id={}&Search='.format('Maturity Range')
fname = 'code.xml'

download(url,fname)


# for sec in seclist:
#     url = 'http://edw.morningstar.com/DataOutput.aspx?Package=EDW&ClientId=magnumhk&Id={}&IDTYpe=FundShareClassId&Content=1471&Currencies=BAS'.format(sec)
#     file_name = "xmldata/{}.xml".format(sec)
#     download(url, file_name)

