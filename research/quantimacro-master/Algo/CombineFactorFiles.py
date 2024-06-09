"""Read factor docs and combine them together
INPUT:
path -string indicates where the factors store

OUTPUT:
factors -pd.DataFrame : For each period, the factors are estimated using most updated data.
NO INSAMPLE FORWARD LOOKING issue.

"""


import os

import pandas as pd
from pandas.tseries.offsets import *

from Constants import *


def CombineFactors():
    path = RESULTSPATH + '\\Factors'
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

    if len(date_list)>=1:
        date = date_list[0]
        tmp = factorFiles_dict[date].copy(deep=True)
        end = pd.to_datetime(date)-QuarterEnd()
        start = end-MonthEnd()*2
        # tmp = tmp[tmp.index >= start]
        tmp = tmp[tmp.index <= end]
        factors = tmp.copy(deep=True)

        k_factors = len(factors.columns)

        if len(date_list)>=2:
            for ii in range(1,len(date_list)):
                date = date_list[ii]
                tmp = factorFiles_dict[date].copy(deep=True)
                end = pd.to_datetime(date) - QuarterEnd()
                start = end - MonthEnd() * 2
                tmp_ = tmp[tmp.index<start]
                last_tmp = factorFiles_dict[date_list[ii-1]].copy(deep=True)
                last_tmp = last_tmp[last_tmp.index<start]

                r1 = []
                for j in range(1,k_factors+1):
                    s1 = last_tmp['f{}'.format(j)]
                    s2 = tmp_['f{}'.format(j)]
                    tmp_df = pd.concat([s1,s2],axis = 1)
                    correlation = tmp_df.corr().iloc[0,1]
                    if correlation < 0:
                        r1.append(-1)
                    else:
                        r1.append(1)

                for j in range(k_factors):
                    factors['f{}'.format(j+1)] = factors['f{}'.format(j+1)] * r1[j]
                # possible_pair = [k+1 for k in range(k_factors)]
                # r1 = []
                # r2 = []
                # for j in range(1,k_factors+1):
                #     for k in range(1,k_factors+1):
                #         if k in possible_pair:
                #             s1 = last_tmp['f{}'.format(j)]
                #             s2 = tmp_['f{}'.format(k)]
                #             tmp_df = pd.concat([s1,s2],axis = 1)
                #             correlation = tmp_df.corr().iloc[0,1]
                #             # print(correlation)
                #             if correlation > 0.5:
                #                 r1.append(k)
                #                 r2.append(1)
                #                 break
                #             if correlation < -0.5:
                #                 r1.append(k)
                #                 r2.append(-1)
                #                 break
                #     if len(r1)< j:
                #         r1.append(j)
                #         r2.append(j)
                #     possible_pair.remove(r1[j-1])
                #
                # for i in range(k_factors):
                #     if r2[i]==1:
                #         factors['nf{}'.format(i+1)] = factors['f{}'.format(i+1)]
                #     else:
                #         factors['nf{}'.format(i+1)] = - factors['f{}'.format(i+1)]
                #
                # for i in range(k_factors):
                #     factors['f{}'.format(i+1)] = factors['nf{}'.format(r1[i])]
                #
                # tmp_list = ['f{}'.format(i+1) for i in range(k_factors)]
                # factors = factors[tmp_list]
                #
                #
                tmp = tmp[tmp.index >= start]
                tmp = tmp[tmp.index <= end]

                factors = pd.concat([factors,tmp],axis=0)



    date = date_list[0]
    end = pd.to_datetime(date) - QuarterEnd()
    start = end - MonthEnd() * 2
    factors = factors[factors.index>=start]

    # factors.to_csv(path+"\\Agg\\Factors1.csv")

    return factors

