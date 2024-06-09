'''This module contains one object and one function
class DFM.
    A class of DFM rather than a function can help fulfill complex functions. For example, filter.
    Though until now, the use of DFM model is very trivial, the structure is kept.

func calcFactors.
INPUT:
date. str. Observation day.
align_no. int.
mtr_dt. dataframe. Monthly raw data.
k_factors. int. number of factors will be extracted.
window. int. Minimum data length.
min_valid. int. Maximum nan.
MONTHLY_UNCHG_LIST. list. Contains the variable names that will be used directly
MONTHLY_FO_DIFF_LIST. list. Contains the variable names that will be used after first order difference.
MONTHLY_GROWTH_LIST. list. Contains the variable names that will be used after taking differences and divided by last value.
OUTPUT:
factors. dataframe. The factors extracted from the available data. The index is the aligned date
    for the data. For example, if align_no =4, observation day is 2019-01-28, then the most up to date
    factors will be assigned an align date of 2018-12-31.
'''
import statsmodels.api as sm

from Algo.Load_Data import *


class DFM():
    def __init__(self, k_factors = 3, factor_order=1, error_order=2,initial = False,init_maxiter = 50,maxiter = 1000):
        self.k_factors = k_factors
        self.factor_order = factor_order
        self.error_order = error_order
        self.initial = initial
        self.init_maxiter = init_maxiter
        self.maxiter = maxiter
        self.factors = None
        self.res = None
        self.insample = None
        self.outsample = None

    def updateInsampleData(self, date = '2018-01-01'):
        pass

    def updateOutsampleData(self, date = '2018-01-01'):
        pass

    def fit(self):
        print("Start Fitting")
        mod = sm.tsa.DynamicFactor(self.insample, k_factors=self.k_factors, factor_order=self.factor_order, error_order=self.error_order)
        if self.initial:
            initial_res = mod.fit(method='powell', disp=False, maxiter=self.init_maxiter)
            try:
                res = mod.fit(initial_res.params, disp=False, maxtier=self.maxiter)
            except:
                res = mod.fit(disp=False, maxiter=self.maxiter)
        else:
            res = mod.fit(disp=False, maxiter=self.maxiter)

        latent_factors = res.factors.filtered
        factors_tmp = latent_factors.transpose()
        columns = ['f{}'.format(i+1) for i in range(self.k_factors)]
        factors = pd.DataFrame(factors_tmp, columns=columns, index=self.insample.index._mpl_repr())

        self.factors = factors
        self.res = res


    def filter(self):
        if self.res == None:
            print("Please fit model first!")
            return

        mod1 = sm.tsa.DynamicFactor(self.outsample, k_factors=self.k_factors, factor_order=self.factor_order, error_order=self.error_order)
        res1 = mod1.filter(self.res.params)

        latent_factors = res1.factors.filtered
        factors_tmp = latent_factors.transpose()
        columns = ['f{}'.format(i + 1) for i in range(self.k_factors)]
        factors = pd.DataFrame(factors_tmp, columns=columns, index=self.outsample.index._mpl_repr())

        self.new_factors = factors

    def save_factors(self,filename):
        self.factors.to_csv(filename)

    def save_new_factors(self,filename):
        self.new_factors.to_csv(filename)

def calcFactors(date, align_no, mtr_dt, k_factors=3, window = 14, min_valid = None,MONTHLY_UNCHG_LIST = [],MONTHLY_FO_DIFF_LIST = [],MONTHLY_GROWTH_LIST = []):
    datain = insample(date,align_no,mtr_dt,window=window,min_valid=min_valid,MONTHLY_UNCHG_LIST=MONTHLY_UNCHG_LIST,MONTHLY_FO_DIFF_LIST=MONTHLY_FO_DIFF_LIST,MONTHLY_GROWTH_LIST=MONTHLY_GROWTH_LIST)

    print("AVIALABLE DATA LENGTH: {}".format(len(datain.columns)))
    dfm = DFM()
    dfm.insample = datain
    dfm.k_factors = k_factors
    # dfm.initial = True
    dfm.error_order = 1


    dfm.fit()
    factors = dfm.factors

    return factors
