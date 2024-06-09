'''
    update benchmark to local csv file
'''
from datamaster import dm_client
from datetime import datetime
import pandas as pd
import os
import pathlib

def update_benchmark():
    dm_client.refresh_config()
    dm_client.start()
    res = dm_client.get_historical_data(symbols=["000300.SH"],
                                        fields="close",
                                        to_dataframe=True,
                                        start_date="2000-01-01",
                                        end_date=datetime.today().strftime("%Y-%m-%d"))
    data_dir_path = pathlib.Path(__file__).parent.parent.absolute().joinpath('data')
    for symbol in res.keys():
        save_file_path = data_dir_path.joinpath(f'{symbol}.csv')
        tmp = res[symbol]
        tmp.set_index('date',inplace=True)
        tmp.to_csv(save_file_path)
    return

if __name__ == '__main__':
    update_benchmark()