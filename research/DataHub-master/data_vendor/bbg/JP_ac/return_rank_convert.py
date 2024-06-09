# -*- coding: UTF-8 -*-
import glob
import os
import pandas as pd
import numpy as np
import matplotlib
import seaborn as sns
import matplotlib.pyplot as plt

from matplotlib.font_manager import FontProperties
# myfont=FontProperties(fname=r'/Users/Ryan/Library/Fonts/NotoSansCJK-Regular.ttc',size=12)
# myfont=FontProperties(fname=r'/Users/Ryan/Library/Fonts/NotoSansCJKsc-Light.otf',size=12)
# sns.set(font=myfont.get_family())
sns.set(font='Helvetica')
sns.set_style("whitegrid",{"font.sans-serif":['Noto Sans CJK SC']})

# final_result = pd.read_csv('JP_ac_return.csv',index_col=0).iloc[-15:]*100.
# final_result = pd.read_excel('JP_ac_return.xlsx', sheetname='ENG').iloc[-15:]*100.
final_result = pd.read_excel('JP_ac_return.xlsx', sheetname='ENG').iloc[-15:]*100.
name_map = pd.read_csv('JP_ac_mapping.csv', index_col=0)
symbol_list = []
value_list = []
year_list = []
year_num = len(final_result.index)
asset_num = len(final_result.columns)
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


fig, ax = plt.subplots(figsize =(24, 8))
# title = "全球大类资产表现（2004-2018)"
title = "Global Asset Return（2004-2018)"
plt.title(title, fontsize=18)
ax.set_xticks([])
ax.set_yticks([])
ax.yaxis.set_visible(False)
ax.xaxis.label.set_visible(False)

sns.heatmap(result, annot = lables, fmt ="", cmap="RdYlGn", ax=ax, linewidths=0.3,annot_kws={"size": 10})
plt.show()
# y_label = final_result.index.tolist()
# x_label = final_result.columns.tolist()
#
#
# plt.show()
#
# rank_df = final_result.rank(axis=1,ascending = False)[1:]
# rank_dict = rank_dict_chn = rank_value = dict()
# rank_dict_chn = dict()
# rank_value = dict()
# for idx, row in rank_df.iterrows():
#     top = row.sort_values().index.tolist()
#     top_eng = top
#     top_chn = [name_map.loc[x]['Asset CH Name'] for x in top]
#     top_value = [final_result.loc[idx][x] for x in top]
#     rank_dict[idx] = top_eng
#     rank_dict_chn[idx] = top_chn
#     rank_value[idx] = top_value
# pd.DataFrame(rank_dict).T.to_csv('JP_ac_rank_eng.csv')
# pd.DataFrame(rank_dict_chn).T.to_csv('JP_ac_rank_chn.csv')
# pd.DataFrame(rank_value).T.to_csv('JP_ac_value.csv')


