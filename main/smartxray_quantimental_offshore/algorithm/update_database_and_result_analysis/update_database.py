from os.path import join
import pandas as pd
from datetime import datetime
from algorithm.addpath import config_path, result_path
from algorithm.strategy_backtest import weight_backtest as wb


if __name__ == '__main__':
    start = datetime.now()
    equity_pct_df = pd.read_csv(join(config_path, 'equity_pct.csv'))
    # 'A1': 无取向
    # 'A2': 金融
    # 'A3': 医疗
    # 'A4': 消费
    # 'A5': 科技
    # 'B1': 无取向
    # 'B2': 美国市场
    # 'B3': 成熟市场（除美国）
    # 'B4': 亚太市场
    # 'B5': 新兴市场
    test_dict = {'equity_pct': equity_pct_df['equity_pct'].tolist(),
            'sector_preference': ['A1', 'A2', 'A3', 'A4', 'A5'],
            'region_preference': ['B1', 'B2', 'B3', 'B4', 'B5']
                 }
    # you may use your own test_dict, say:
    # test_dict = {'equity_pct': [0.2, 0.5],
    #              'sector_preference': ['A2', 'A4'],
    #              'invest_length': ['B2']}

    # if type == 'skip_bundle', then skip downloading and injecting bundles; if type == 'skip_download_bundle', then skip downloading bundles.
    wb.weighting_backtesting(start_date = '2016-02-29', end_date = '2020-11-30', output_path=join(result_path, 'todatabase'),
                          test_dict=test_dict, type = 'skip_bundle')
    end = datetime.now()
    print((end - start).seconds)