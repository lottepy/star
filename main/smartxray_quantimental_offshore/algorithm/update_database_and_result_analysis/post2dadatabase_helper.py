import pandas as pd
import numpy as np

from algorithm import addpath

from os.path import join

def make_duplicate_007_and_013():
    original_007 = pd.read_csv(join(addpath.result_path, 'todatabase Final Version', 'projection', '0.07_A1_B1.csv'), index_col=[0])
    A_list = ['A1', 'A2', 'A3', 'A4', 'A5']
    B_list = ['B1', 'B2', 'B3', 'B4', 'B5']


    for a in A_list:
        for b in B_list:
            file_name = join(addpath.result_path, 'todatabase Final Version', 'projection', '0.07_' + a + '_' + b + '.csv')
            original_007.to_csv(file_name)
            
    original_013 = pd.read_csv(join(addpath.result_path, 'todatabase Final Version', 'projection', '0.13_A1_B1.csv'), index_col=[0])
    A_list = ['A1', 'A2', 'A3', 'A4', 'A5']
    B_list = ['B1', 'B2', 'B3', 'B4', 'B5']


    for a in A_list:
        for b in B_list:
            file_name = join(addpath.result_path, 'todatabase Final Version', 'projection', '0.13_' + a + '_' + b + '.csv')
            original_013.to_csv(file_name) 
            
            
    post2database = pd.read_csv(join(addpath.result_path, 'todatabase Final Version', 'post2database.csv'))
    original_007 = post2database[post2database['risk_ratio'] == 7]

    for a in A_list:
        for b in B_list:
            if a =='A1' and b=='B1':
                continue
            original_007['sector'] = a
            original_007['region'] = b
            post2database = post2database.append(original_007)
            
    original_013 = post2database[post2database['risk_ratio'] == 13]

    for a in A_list:
        for b in B_list:
            if a =='A1' and b=='B1':
                continue
            original_013['sector'] = a
            original_013['region'] = b
            post2database = post2database.append(original_013)
            
    post2database = post2database.sort_values(['risk_ratio', 'sector', 'region']).reset_index(drop=True)


    post2database.to_csv(join(addpath.result_path, 'todatabase Final Version', 'post2database.csv'), index=False)
