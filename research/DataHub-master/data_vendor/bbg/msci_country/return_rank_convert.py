# -*- coding: UTF-8 -*-
import glob
import os
import pandas as pd

final_result = pd.read_excel('China_ac_return.csv',sheetname='USD')
name_map = pd.read_excel('calendar_year_return.xlsx',sheetname='mapping', index_col=0)
rank_df = final_result.rank(axis=1,ascending = False)

top_dict = {}
bottom_dict = {}
top_country = None
bottom_country = None
for idx, row in final_result.iterrows():
    if top_country:
        # top_dict[(idx, top_country)] = row[top_country]
        top_dict[(idx, top_country)] = rank_df.loc[idx][top_country]
    if bottom_country:
        # bottom_dict[(idx, bottom_country)] = row[bottom_country]
        bottom_dict[(idx, bottom_country)] = rank_df.loc[idx][bottom_country]
    top_country = row.sort_values().tail(1).index[0]
    bottom_country = row.sort_values().head(1).index[0]


rank_dict = rank_dict_chn = dict()
for idx, row in rank_df.iterrows():
    top = row[row<6].index.tolist()
    top_eng = [name_map.loc[x].country for x in top]
    top_chn = [name_map.loc[x].country_ch for x in top]
    rank_dict[idx] = top_eng
    rank_dict_chn[idx] = top_chn
# result = pd.DataFrame(rank_dict).T.to_csv('top5_country_local.csv')
pd.DataFrame(rank_dict).T.to_csv('top5_country_USD_eng.csv')
pd.DataFrame(rank_dict_chn).T.to_csv('top5_country_USD_chn.csv')