# -*- coding: UTF-8 -*-
import glob
import os
import pandas as pd

files = os.listdir()
path_USD = str(os.getcwd()) + '/data/*.csv'
path_local = str(os.getcwd()) + '/msci_country/local/MSCI*.csv'

all_data = []

for fname in glob.glob(path_USD):
    country_name = fname.split('/')[-1][:-4]
    # i.e., Emerging_Markets_Sri_Lanka
    df = pd.read_csv(fname).set_index('date')
    df.rename(columns={'close': country_name, }, inplace=True)
    all_data.append(df)

all_data = pd.concat(all_data, axis=1)


select_year = pd.date_range(start=all_data.first_valid_index(
), end=all_data.last_valid_index(), freq='Y')

# 确定我们所需的时间/index
select_date = [all_data.first_valid_index()] + [d.strftime('%Y-%m-%d')
                                                for d in select_year] + [all_data.last_valid_index()]


# 从all_data中提取有用的数据：
# result_Numerator = all_data.loc[select_date, :]

# 构造分子和分母
result_Numerator = all_data.loc[select_date[1:], :]  # 分子
result_Denominator = all_data.loc[select_date[:-1], :]  # 分母


final_result = result_Numerator.values / (result_Denominator.values)

final_result = pd.DataFrame(
    final_result, index=select_date[1:], columns=result_Numerator.columns)-1.

# 保存文件
final_result.to_csv('JP_ac_return.csv', encoding='utf-8')

