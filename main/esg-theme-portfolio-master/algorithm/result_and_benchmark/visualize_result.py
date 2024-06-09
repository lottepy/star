#%%
import pandas as pd
import numpy as np

from matplotlib import pyplot as plt
from algorithm import addpath
from os.path import join
from os import listdir



strategy_list = listdir( join(addpath.result_path, 'fer_1'))

outperform_rotate_index_vs_index = [pd.read_csv(join(addpath.result_path, 'fer_1', ele, 'outperform.csv'), index_col=[0], parse_dates=[0])['Rotate Index Spread vs Index'] for ele in strategy_list]
outperform_rotate_index_vs_index = pd.concat(outperform_rotate_index_vs_index, axis=1)
outperform_rotate_index_vs_index.columns = strategy_list

outperform_rotate_index_vs_index.plot()

# %%
outperform_rotate_index_vs_index



# %%
