import pandas as pd
import os
import datetime
import os
import json, requests
from algorithm import addpath
import multiprocessing

def bundle_helper(symbol, input_path, bundle_path, interest_path, fxrate_path, trading_calendar):
    file = symbol + ".csv"
    tmp = pd.read_csv(input_path + "//" + file, parse_dates=[0], index_col=0)
    tmp = tmp.ffill()
    tmp = tmp[tmp.index.isin(trading_calendar)]
    bundle_data = pd.DataFrame(index=tmp.index)
    bundle_data['open'] = tmp['close']
    bundle_data['high'] = tmp['close']
    bundle_data['low'] = tmp['close']
    bundle_data['close'] = tmp['close']
    bundle_data['volume'] = tmp['volume']
    bundle_data.index.name = 'date'
    bundle_data.to_csv(bundle_path + "//" + symbol + ".CN.csv")

    # reference_data = pd.DataFrame(index=tmp.index)
    # reference_data['mktcap'] = tmp['PX_LAST_RAW'] * tmp['EQY_SH_OUT']
    # reference_data.index.name = 'date'
    # reference_data.to_csv(mktcap_path + "//" + symbol + ".CN.csv")

    interest_data = pd.DataFrame(index=tmp.index)
    interest_data['interest_rate'] = 0
    interest_data.to_csv(interest_path + "//" + symbol + ".CN.csv")

    fxrate_data = pd.DataFrame(index=tmp.index)
    fxrate_data['fx_rate'] = 1
    fxrate_data.to_csv(fxrate_path + "//" + symbol + ".CN.csv")


def create_bundle():
    trading_calendar = pd.read_csv(os.path.join(addpath.bundle_path, "Trading_Calendar.csv"))
    trading_calendar = trading_calendar['data'].tolist()

    input_path = os.path.join(addpath.bundle_path, "daily")
    bundle_path = os.path.join(addpath.data_path, "backtest_data", "bundles")
    interest_path = os.path.join(addpath.data_path, "backtest_data", "interest")
    fxrate_path = os.path.join(addpath.data_path, "backtest_data", "fxrate")
    info_path = os.path.join(addpath.data_path, "backtest_data", "info")

    if os.path.exists(bundle_path):
        pass
    else:
        os.makedirs(bundle_path)

    if os.path.exists(interest_path):
        pass
    else:
        os.makedirs(interest_path)

    if os.path.exists(fxrate_path):
        pass
    else:
        os.makedirs(fxrate_path)

    if os.path.exists(info_path):
        pass
    else:
        os.makedirs(info_path)

    portfolio_path = os.path.join(addpath.data_path, "pool", "GZNSH")

    portfolio_files = os.listdir(portfolio_path)
    symbol_list = []
    for portfolio_file in portfolio_files:
        tmp = pd.read_csv(os.path.join(portfolio_path, portfolio_file))
        tmp_list = tmp['ms_secid'].tolist()
        symbol_list = symbol_list + tmp_list
    symbol_list = list(set(symbol_list))

    pool = multiprocessing.Pool(22)
    for symbol in symbol_list:
        # bundle_helper(symbol, input_path, bundle_path, mktcap_path, interest_path, fxrate_path)
        pool.apply_async(bundle_helper, args=(symbol, input_path, bundle_path, interest_path, fxrate_path, trading_calendar,))
        print('Sub-processes begins!')
    pool.close()  # 关闭进程池，表示不能再往进程池中添加进程，需要在join之前调用
    pool.join()  # 等待进程池中的所有进程执行完毕
    print("Sub-process(es) done.")

    symbol_list = [s + '.CN' for s in symbol_list]
    info_data = pd.DataFrame(index=symbol_list)
    info_data.index.name = 'symbol'
    info_data['csv_name'] = info_data.index
    # info_data['source'] = ''
    info_data['symboltype'] = 'CN_Stock'
    info_data['trading_currency'] = 'CNY'
    info_data['lot_size'] = 1
    info_data['bbg_code'] = ''
    info_data['back_up_bbg_code'] = ''
    info_data.to_csv(os.path.join(info_path, "info.csv"))


if __name__ == "__main__":
    create_bundle()
