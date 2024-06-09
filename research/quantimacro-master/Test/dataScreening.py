from Algo.Load_Data import *
from statsmodels.tsa.stattools import adfuller
from statsmodels.tsa.stattools import adfuller

from Algo.Load_Data import *


def gr(s):
    s = s.diff() / abs(s)
    s.replace([np.inf, -np.inf], np.nan, inplace=True)
    return s
def fo(s):
    s = s.diff()
    return s


qtr_dt, mtr_dt, wkl_dt = load_data(QTR_PATH, MTR_PATH, WKL_PATH, 'US')
df = mtr_dt.copy(deep=True)

s = df['ADP CHNG Index']
x = s.dropna().values
adfuller(x)[1]
x = gr(s).dropna().values
adfuller(x)[1]

s = df['CHPMINDX Index']
x = s.dropna().values
adfuller(x)[1]

s = df['CONSCURR Index']
x = s.dropna().values
adfuller(x)[1]
x = fo(s).dropna().values
adfuller(x)[1]

s = df['CONSSENT Index']
x = s.dropna().values
adfuller(x)[1]
x = fo(s).dropna().values
adfuller(x)[1]

s = df['EMPRGBCI Index']
x = s.dropna().values
adfuller(x)[1]

s = df['IP  CHNG Index']
x = s.dropna().values
adfuller(x)[1]

s = df['LMCILMCC Index']
x = s.dropna().values
adfuller(x)[1]
x = fo(s).dropna().values
adfuller(x)[1]

s = df['NAPMNMI  Index']
x = s.dropna().values
adfuller(x)[1]

s = df['NAPMPMI Index']
x = s.dropna().values
adfuller(x)[1]

s = df['NAPMPRIC Index']
x = s.dropna().values
adfuller(x)[1]

s = df['NFP TCH Index']
x = s.dropna().values
adfuller(x)[1]

s = df['NYPMCURR Index']
x = s.dropna().values
adfuller(x)[1]

s = df['OUTFGAF Index']
x = s.dropna().values
adfuller(x)[1]

s = df['PCE CRCH Index']
x = s.dropna().values
adfuller(x)[1]

s = df['PRUSTOT Index']
x = s.dropna().values
adfuller(x)[1]
x = fo(s).dropna().values
adfuller(x)[1]
x = gr(s).dropna().values
adfuller(x)[1]

s = df['RCHSINDX Index']
x = s.dropna().values
adfuller(x)[1]

s = df['RSTAMOM Index']
x = s.dropna().values
adfuller(x)[1]

s = df['RSTAXMOM Index']
x = s.dropna().values
adfuller(x)[1]

s = df['USTGTTCB Index']
x = s.dropna().values
adfuller(x)[1]

s = df['USURTOT Index']
x = s.dropna().values
adfuller(x)[1]
x = fo(s).dropna().values
adfuller(x)[1]
x = gr(s).dropna().values
adfuller(x)[1]

LIST = MONTHLY_FO_DIFF_LIST+MONTHLY_GROWTH_LIST+MONTHLY_UNCHG_LIST
len(LIST)
df_ = df[LIST]







