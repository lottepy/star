# -*- coding:utf-8 -*-
'''
p.199 Marcos Lopez de Prado. 2018. Advances in Financial Machine Learning (1st. ed.). Wiley Publishing.
'''
import numpy as np

def getHHI(rtn):
    n = len(rtn)
    if n <= 2:
        return np.nan
    wght = rtn/sum(rtn)
    hhi = sum((wght**2))
    hhi = (hhi-n**-1)/(1.-n**-1)
    return hhi