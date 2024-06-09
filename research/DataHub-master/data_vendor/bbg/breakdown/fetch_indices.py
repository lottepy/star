from lib.commonalgo.data.bbg_downloader import download_his,download_ref
import pandas as pd

symbol_file = 'bbg_index_fulllist.txt'
shortsym_file = 'bbg_index_shortlist.txt'
flds_file = 'indices_flds.txt'

def file2list(filename):
    with open(filename,'r') as f:
        results = f.readlines()
        return [x.replace('\n','') for x in results]

# symbol_list = file2list(symbol_file)
short_symbols = file2list(shortsym_file)
field_list = file2list(flds_file)
field_list = field_list[:-3]
data = pd.DataFrame()

for symbol in short_symbols:
    try:
        result = download_ref(symbol, field_list)
        data = pd.concat([data, result])
    except:
        print (symbol)

print(data)
data.to_csv('bbg_index_info_short.csv')
