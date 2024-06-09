import pandas as pd
import os, requests, json
from time_transfer import *
from ticker_transfer import *
from datamaster import dm_client
dm_client.refresh_config()
dm_client.start()

STRATEGY_KIT_PATH = r'Z:/alioss/Strategy_Kit/'
def get_price_date(start_date, end_date):
    start_date = time_to_str_iso(start_date)
    end_date = time_to_str_iso(str_to_time_iso(end_date) + timedelta(days=1))

    r = requests.get(
        'https://algo-internal.aqumon.com/datamaster/exchange_calendar/?start_date=' + start_date
        + '&end_date=' + end_date + '&ex=' + 'CN')
    user_info = r.content.decode()
    user_dict = json.loads(user_info)

    output = pd.DataFrame(user_dict)
    output.sort_values(by='data', inplace=True)
    output.set_index('data', inplace=True)

    return output


def price_datamaster(iuid_list, start_date, end_date):
    start_date = time_to_str_iso(start_date)
    end_date = time_to_str_iso(end_date)
    # cn_fund_full = pd.read_csv('../../config/cn_fund_full.csv', index_col='iuid')
    # # iuid_list = list(set(iuid_list))
    # instrument_id_list = []
    # for iuid in iuid_list:
    #     # instrument_id = cn_fund_full.loc[iuid]['instrument_id']
    #     # if type(instrument_id) == pd.core.series.Series:
    #     #     instrument_id = instrument_id.iloc[0]
    #     res = dm_client.info(symbols=iuid, fields='instrument_id')
    #     instrument_id_list.append(res['values'][iuid][0])

    price_all = pd.DataFrame()
    for index, iuid in enumerate(iuid_list):
        result = dm_client.historical(symbols=iuid, start_date=start_date, end_date=end_date, fields='fund_fqnav')
        try:
            result = pd.DataFrame(result['values'][iuid])
            result.columns = ['date', iuid]
            result.set_index(['date'], inplace=True)
            price_all = pd.concat([price_all, result], axis=1)
        except:
            print('Cannot get price of ' + iuid)

    price_all = price_all.fillna(method='ffill')

    return price_all


def price_processing(price, weight):
    price = price.astype(float)
    original_date = str_list_to_time_iso(price.index)
    price = price / price.iloc[0]
    price = price.fillna(0)
    pv_new = pd.DataFrame(pd.np.dot(price, weight))
    pv_new.columns = weight.columns
    pv_new = pv_new.sort_index()
    pv_new.index = original_date

    return pv_new


def backtesting_weekly(algo, end_date, output_path):
    pv = pd.read_excel(STRATEGY_KIT_PATH + algo + '.xlsx', sheet_name='pv', index_col=0)
    weight = pd.read_excel(STRATEGY_KIT_PATH + algo + '.xlsx', sheet_name='weighting', index_col=0)
    weight = weight.fillna(0.0)
    weight_benchmark = pd.read_excel(STRATEGY_KIT_PATH + algo + '.xlsx', sheet_name='benchmark_weighting', index_col=0)
    weight_benchmark = weight_benchmark.fillna(0.0)

    start_date_str = time_to_str_iso(pv.index[-1])
    start_date = pv.index[-1]

    if start_date < str_to_time_iso(end_date):
        print('Downloading data!')
        iuid_list = cn_to_aqumon_fund_ticker(list(weight.index))
        # price_date = list(
        #     price_market_client(iuid_list=iuid_list, tag=52, start_date=start_date, end_date=end_date).index)
        price_date = list(get_price_date(start_date, end_date).index)
        price = price_datamaster(iuid_list=iuid_list, start_date=start_date, end_date=end_date).loc[price_date]
        # price = price_market_client(iuid_list=iuid_list, tag=52, start_date=start_date, end_date=end_date)

        if algo == 'xray_lcm_weekly':
            price_benchmark = pd.read_excel(STRATEGY_KIT_PATH + 'wind_index.xlsx', sheet_name='lcm', index_col=0,
                                            header=3)
        else:
            price_benchmark = pd.read_excel(STRATEGY_KIT_PATH + 'wind_index.xlsx', sheet_name='Ashare', index_col=0,
                                            header=3)

        price_benchmark = price_benchmark.loc[start_date:end_date, weight_benchmark.index]
        price_benchmark.index = time_list_to_str_iso(list(price_benchmark.index))
        price_benchmark = price_benchmark[list(weight_benchmark.index)]

        price = price.ffill().bfill()
        pv_new = price_processing(price=price, weight=weight)
        pv_benchmark_new = price_processing(price=price_benchmark, weight=weight_benchmark)
        pv_new = pd.concat([pv_new, pv_benchmark_new], axis=1)

        pv_new = pv_new * pv.iloc[-1]
        pv_all = pd.concat([pv, pv_new])
        pv_all  = pv_all[~pv_all.index.duplicated()]

        if algo == 'xray_huarun_weekly':
            pv_all = pv_all[['稳健型组合', '平衡型组合', '成长型组合', '进取型组合', '稳健型基准', '平衡型基准', '成长型基准',
                             '进取型基准', '中证800', '上证国债']]
        elif algo == 'xray_lcm_weekly':
            pv_all = pv_all[[0, 10, 20, 30, 40, 50, 60, 70, 80, 'bond', 'mm', 'benchmark_0', 'benchmark_10',
                             'benchmark_20', 'benchmark_30', 'benchmark_40', 'benchmark_50', 'benchmark_60',
                             'benchmark_70', 'benchmark_80','benchmark_bond', 'benchmark_mm', '上证综指', '中证全债',
                             '货币市场基金指数']]
        elif algo == 'xray_gznsh_weekly':
            pv_all = pv_all[[20, 40, 50, 60, 70, 'benchmark_20', 'benchmark_40', 'benchmark_50', 'benchmark_60',
                             'benchmark_70']]
            
        writer = pd.ExcelWriter(STRATEGY_KIT_PATH + algo + '.xlsx')
        pv_all.to_excel(writer, sheet_name='pv')
        weight.to_excel(writer, sheet_name='weighting')
        weight_benchmark.to_excel(writer, sheet_name='benchmark_weighting')
        writer.save()

        writer = pd.ExcelWriter(output_path + algo + '.xlsx')
        pv_all.to_excel(writer, sheet_name='pv')
        writer.save()

    else:
        print('No need to download data!')
        writer = pd.ExcelWriter(output_path + algo + '.xlsx')
        pv.to_excel(writer, sheet_name='pv')
        writer.save()


if __name__ == '__main__':
    end_of_week = '2021-01-22'

    output_path = f'weekly_report/{end_of_week}/'
    if not os.path.exists(output_path):
        print(f"Create new folder {output_path}")
        os.makedirs(output_path)
    else:
        print(f"Folder {output_path} already existed. Will overwrite.")

    backtesting_weekly(algo='xray_huarun_weekly', end_date=end_of_week, output_path=output_path)
    backtesting_weekly(algo='xray_lcm_weekly', end_date=end_of_week, output_path=output_path)
    backtesting_weekly(algo='xray_gznsh_weekly', end_date=end_of_week, output_path=output_path)