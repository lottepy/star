from algorithm.addpath import data_path, model_path, ref_path
import pandas as pd
from os import makedirs
from os.path import exists, join
from datetime import datetime


def construct_returns_whole(hrz):
    nav_path = join(data_path, 'bundles', 'daily')
    save_path = join(model_path, 'returns')
    if not exists(save_path):
        makedirs(save_path)

    ms_secids = pd.read_csv(ref_path)
    ms_secids = ms_secids['ms_secid'].tolist()

    nav_list = []
    start_time = datetime.now()
    count = 0
    for ms_secid in ms_secids:
        try:
            nav_tmp_path = join(nav_path, str(ms_secid) + ".csv")
            nav_tmp = pd.read_csv(nav_tmp_path, parse_dates=[0], index_col=0)
            nav_tmp = nav_tmp[['close']].rename(columns={'close': ms_secid})
            nav_tmp = nav_tmp.resample('1M').last().ffill()
            nav_list.append(nav_tmp)
        except:
            count += 1
            print("{} Fail for {}".format(count, ms_secid))
    end_time = datetime.now()
    print('Finish reading navs in {}'.format(end_time - start_time))

    nav = pd.concat(nav_list, axis=1)
    # horizon = [1, 3, 6, 12]
    # for hrz in horizon:
    output_path = join(save_path, str(hrz) + 'M_return.csv')
    fwd_returns = nav.shift(-hrz) / nav - 1
    fwd_returns.to_csv(output_path)


if __name__ == "__main__":
    construct_returns_whole(1)
