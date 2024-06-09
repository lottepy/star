'''This module contains one function
func forecast.
    In a factor path, each file represents the factor calculated on a forecast date. For each file,
    calculate the GDP forecast based on the available factors. The train set is the factors up to last
    quarter, the test set is the current quarter. The forecast is done by linear regression. The
    endogenous data is the GDP, and the exogenous data is the factors, lag-1 month factors, and lag-2
    month factors. Typically, we will have 40 sample and 9 exogenous variables.

'''

from Algo.DFM import *

def forecast(factorPath):
    path = factorPath
    factorFiles = os.listdir(path)
    factorFiles_dict = {}
    date_list = []
    for factorFile in factorFiles:
        file_path = path + "\\" + factorFile
        if os.path.isfile(file_path):
            (name,exts) = os.path.splitext(factorFile)
            if exts == '.csv':
                tmp = pd.read_csv(file_path,index_col=0)
                tmp.index = pd.to_datetime(tmp.index)
                factorFiles_dict[name]=tmp
                date_list.append(name)

    # rslt = []
    # for date in date_list:
    #     factors = factorFiles_dict[date]
    #     end = pd.to_datetime(date) - QuarterEnd()
    #     start = end - MonthEnd() * 2
    #
    #     endog = getEndog(date)
    #
    #     factors_ = factors.resample("Q").sum()
    #     df = pd.concat([factors_,endog],axis = 1)
    #     df = df[df.index<start]
    #     df_ = df.copy(deep=True)
    #
    #
    #     df_.dropna(inplace=True)
    #     df_x = df_.drop(['y'], axis=1)
    #     df_y = df_['y']
    #
    #     df_x = sm.add_constant(df_x)
    #     model = sm.OLS(df_y,df_x)
    #     res = model.fit()
    #
    #     print(res.rsquared)
    #
    #     df_ = factors[factors.index>=start]
    #     df_ = df_[df_.index<=end]
    #
    #     df_ = df_.resample("Q").sum()
    #
    #     new_x = df_.iloc[[-1]]
    #     new_x = sm.add_constant(new_x,has_constant='add')
    #     y= res.predict(new_x)
    #     y = pd.DataFrame({"y":y[-1]},index = [end])
    #     y.index = pd.to_datetime(y.index)
    #     rslt.append(y)

    rslt = []
    for date in date_list:
        factors = factorFiles_dict[date]
        end = pd.to_datetime(date) - QuarterEnd()
        start = end - MonthEnd() * 2

        endog = getEndog(date)

        df = pd.concat([factors,endog],axis = 1)
        df = df[df.index<start]
        df_ = df.copy(deep=True)

        k_factors = len(factors.columns)
        for ii in range(k_factors):
            df_['f{}_1'.format(ii + 1)] = df_['f{}'.format(ii + 1)].shift(1)
            df_['f{}_2'.format(ii + 1)] = df_['f{}'.format(ii + 1)].shift(2)

        df_.dropna(inplace=True)
        df_x = df_.drop(['y'], axis=1)
        df_y = df_['y']

        df_x = sm.add_constant(df_x)
        model = sm.OLS(df_y,df_x)
        res = model.fit()
        print("RSquared: {}".format(res.rsquared))


        df_ = factors[factors.index>=start]
        df_ = df_[df_.index<=end]

        for ii in range(k_factors):
            df_['f{}_1'.format(ii + 1)] = df_['f{}'.format(ii + 1)].shift(1)
            df_['f{}_2'.format(ii + 1)] = df_['f{}'.format(ii + 1)].shift(2)

        new_x = df_.iloc[[-1]]
        new_x = sm.add_constant(new_x,has_constant="add")
        y = res.predict(new_x)
        y = pd.DataFrame({"y":y[-1]},index = [end])
        y.index = pd.to_datetime(y.index)
        rslt.append(y)

    if len(rslt)>=1:
        new_y = rslt[0]
        if len(rslt)>=2:
            for tmp in rslt[1:]:
                new_y = pd.concat([new_y,tmp],axis = 0)

    # new_y.to_csv(RESULTSPATH + '\\f6_rslt2.csv')
    return new_y