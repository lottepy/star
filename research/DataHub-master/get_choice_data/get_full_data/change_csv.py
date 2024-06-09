import pandas as pd

data1 = pd.read_csv('all_stock_set.csv')
data2 = pd.read_csv('choice_hk_stock.csv', index_col = 0)
data2.columns = ['ticker','name']

data3 = data2[['ticker']]
data3 = data3.sort_values(by="ticker" , ascending=True)
for i in range(len(data3)):
    if data3.iloc[i,0][0] !='0':
        print(data3.iloc[i,0])
data4 = data3[:-1][['ticker']]
data4['ticker'] = data4['ticker'].apply(lambda x:x[1:])
data5 = pd.merge(data1,data4,on=['ticker'], how='outer')
data5.to_csv('merge_stock.csv',index=None)
print(1)