from algorithm.addpath import data_path
import os
import pandas as pd
from tia.bbg import Terminal

LocalTerminal = Terminal('bbg.algo-api.aqumon.com', 8194)


def download_his(ticker, field_list, start_date, end_date,
                 currency=None, period="DAILY", dpdf=None, normal=True, abnormal=True, split=None, calendar=None, **kwargs):
    raw_df = LocalTerminal.get_historical(
        sids = ticker,
        flds =field_list,
        start=start_date,
        end=end_date,
        period=period, # [DAILY, WEEKLY, MONTHLY, QUARTERLY, SEMI_ANNUALLY, YEARLY]
        # period_adjustment='ACTUAL',# (ACTUAL, CALENDAR, FISCAL)
        currency=currency,
        # override_option=None,pricing_option=None, #(NON_TRADING_WEEKDAYS | ALL_CALENDAR_DAYS | ACTIVE_DAYS_ONLY)
        non_trading_day_fill_method='PREVIOUS_VALUE', #(PREVIOUS_VALUE | NIL_VALUE)
        # max_data_points=None,
        # adjustment_normal=None,adjustment_abnormal=None, adjustment_split=None,
        adjustment_follow_DPDF=dpdf,
        adjustment_split = split,
        adjustment_normal = normal,
        adjustment_abnormal = abnormal,
        calendar_code_override=calendar,
        **kwargs
    )

    df = raw_df.as_frame()
    return df

def generate_bundle(datain, dest, name_list):
    for j in range(datain.shape[1]):
        output = pd.DataFrame([datain.iloc[:,j]]*4).T
        output.columns = ['open', 'high', 'low', 'close']
        output['volume'] = 100
        output.to_csv(os.path.join(dest, name_list[j]))
    return

def generate_fxrate(datain, dest, name_list):
    for j in range(datain.shape[1]):
        output = pd.DataFrame(1, index=datain.index, columns=['fx_rate'])
        output.to_csv(os.path.join(dest, name_list[j]))
    return

def generate_interest(datain, dest, name_list):
    for j in range(datain.shape[1]):
        output = pd.DataFrame(0, index=datain.index, columns=['interest_rate'])
        output.to_csv(os.path.join(dest, name_list[j]))
    return
   

if __name__ == "__main__":
    
    start = '2010-01-01'
    end = '2021-01-08'
    
    symbol_list = ['CRSPTM1 Index', 'DAX Index', 'UKX Index', 'SX5E Index', 'SHCOMP Index', 'SZCOMP Index', 'SHSZ300 Index', 'NKY Index', 'AS51 Index', 'MXEF Index', 'RTY Index', 'FTREEURO Index', 'MXAP Index']
    data = download_his(symbol_list, ['CUR_MKT_CAP'])

    # Benchmark
    benchmark_symbols = ['LG13TRUU Index', 'LGTRTRUU Index', 'MXWD Index']
    
    current_list = benchmark_symbols
    data = download_his(
        current_list, ['PX_LAST'], start, end, period='DAILY', currency='USD', dpdf=None,
        normal=True, abnormal=True, split=True,
        # normal=False, abnormal=False, split=False,
        calendar=None, non_trading_day_fill_option='ALL_CALENDAR_DAYS'
    ).ffill()
    data.columns = data.columns.get_level_values(0)
    data = data[current_list]

    name_list = [ele + '.csv' for ele in benchmark_symbols]
    generate_bundle(data, os.path.join(data_path, 'benchmark', 'bundles'), name_list)
    generate_fxrate(data, os.path.join(data_path, 'benchmark', 'fxrate'), name_list)
    generate_interest(data, os.path.join(data_path, 'benchmark', 'interest'), name_list)

