# -*- coding: UTF-8 -*-
import glob
import os
import sys
import pandas as pd
import numpy as np
import matplotlib
import seaborn as sns
import matplotlib.pyplot as plt


from matplotlib.font_manager import FontProperties
# myfont=FontProperties(fname=r'/Users/Ryan/Library/Fonts/NotoSansCJK-Regular.ttc',size=12)
myfont=FontProperties(fname=r'/Users/Ryan/Library/Fonts/NotoSansCJKsc-Light.otf',size=12)
sns.set(font=myfont.get_family())
sns.set_style("whitegrid",{"font.sans-serif":['Noto Sans CJK SC']})

csfont = {'fontname':'Noto Sans CJS SC'}

final_result = pd.read_excel('calendar_year_return.xlsx')*100.
name_map = pd.read_csv('China_ac_mapping.csv', index_col=0)
symbol_list = []
value_list = []
year_list = []
year_num = len(final_result.index)
asset_num  = len(final_result.columns)
for idx, row in final_result.iterrows():
    print (idx)
    row = row.sort_values(ascending=False)
    symbol_list += row.index.tolist()
    value_list += row.tolist()
    year_list += [idx.year]*asset_num

yrows = np.array([[x] * asset_num for x in range(1,year_num +1)]).flatten()
xrows = np.array([range(1,asset_num+1)]*year_num).flatten()

df = pd.DataFrame({'Symbol': symbol_list, 'Value': value_list, 'Yrows': year_list, 'Xrows': xrows})
result = df.pivot(values='Value', index='Yrows', columns='Xrows').T

lables = (np.asarray(["{0} \n {1:.1f}%".format(name, value) for name, value in zip(symbol_list, value_list)])
          ).reshape(year_num, asset_num).T


fig, ax = plt.subplots(figsize =(16,6))
title = "中国市场表现 (2006-2018)"
plt.title(title, fontsize=18)
ax.set_xticks([])
ax.set_yticks([])
ax.yaxis.set_visible(False)
ax.xaxis.label.set_visible(False)


sns.heatmap(result, annot = lables, fmt ="", cmap="RdYlGn", ax=ax, linewidths=0.3)
plt.show()


print ()


