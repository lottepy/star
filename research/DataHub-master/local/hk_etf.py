import pandas as pd

symbols = list(pd.read_csv('Summary.csv')['Name'].values)
symbol_iuid = [x.replace('HK','HK_10_') for x in symbols]
with open('your_file.txt', 'w') as f:
    for item in symbol_iuid:
        f.write("%s\n" % item)
print(symbols)