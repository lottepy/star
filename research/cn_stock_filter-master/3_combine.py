import pandas as pd
import numpy as np
import os
from tqdm import tqdm


all_month_date_set = pd.date_range("2012-01-01", "2020-09-30", freq="M")
all_week_date_set = pd.date_range("2012-01-01", "2020-09-30", freq="W")
all_day_date_set = pd.date_range("2012-01-01", "2020-09-30", freq="D")
all_stock_set = pd.read_csv(
    "data/aqm_cn_stock.csv", index_col=0).iloc[:, 0].values.tolist()
small_stock_set = pd.read_csv(
    "data/aqm_cn_stock_1171.csv", index_col=0).iloc[:, 0].values.tolist()


def combine(universe, filter_list, name):
    for date in tqdm(all_day_date_set):
        date_str = date.strftime("%Y-%m-%d")
        current_df = pd.DataFrame(np.ones(len(universe)),
                                  index=universe,
                                  columns=['status'])
        for m, d in filter_list:
            diff_list = pd.read_csv(os.path.join(d, f'{date_str}.csv'),
                                    index_col=0).iloc[:, 0].values.tolist()
            diff_list = [i for i in diff_list if i in universe]
            if m == 'add':
                current_df.loc[diff_list] = 1
            else:
                current_df.loc[diff_list] = 0
        current_df.to_csv(f'filter_bundle/{name}/{date_str}.csv')
        # print(np.sum(current_df.values))


if __name__ == '__main__':
    # user kwown filter bundle
    combine(all_stock_set,
            [('sub', 'filter/suspension'),
             ('sub', 'filter/last_amount/3'),
             ('sub', 'filter/last_market_cap/3'),
             ('sub', 'filter/price_limit/99'),
             ('sub', 'filter/price_level/2'),
             ('sub', 'filter/small_market_cap')
             ],
            "1")
    combine(small_stock_set,
            [('sub', 'filter/suspension'),
             ('sub', 'filter/last_amount/3'),
             ('sub', 'filter/last_market_cap/3'),
             ('sub', 'filter/price_limit/99'),
             ('sub', 'filter/price_level/2'),
             ('sub', 'filter/small_market_cap')
             ],
            "2")
