from sklearn import linear_model

from Algo.CombineFactorFiles import *

factors = CombineFactors()
endog = getEndog('2019-03-01')

df = pd.concat([factors, endog],axis=1,join='outer')
df_ = df.copy(deep=True)
k_factors = len(factors.columns)
for ii in range(k_factors):
    df_['f{}_1'.format(ii + 1)] = df_['f{}'.format(ii + 1)].shift(1)
    df_['f{}_2'.format(ii + 1)] = df_['f{}'.format(ii + 1)].shift(2)

df_.dropna(inplace=True)
df_x = df_.drop(['y'], axis=1)
df_y = df_['y']

n = len(df_x)
rslt = []
if n > 20:
    for ii in range(20,n):
        date = df_.index[ii]
        LR = linear_model.LinearRegression()
        res = LR.fit(df_x[:ii],df_y[:ii])
        y = res.predict(df_x[ii:ii+1])
        y = pd.DataFrame({"y":y[-1]},index = [date])
        y.index = pd.to_datetime(y.index)
        rslt.append(y)

if len(rslt)>=1:
    new_y = rslt[0]
    if len(rslt)>=2:
        for tmp in rslt[1:]:
            new_y = pd.concat([new_y,tmp],axis = 0)

new_y.to_csv(RESULTSPATH + '\\rslt4.csv')