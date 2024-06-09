from tia.bbg import Terminal
# LocalTerminal = Terminal('192.168.9.109', 18194)
# LocalTerminal = Terminal('47.244.101.245', 18194)
LocalTerminal = Terminal('bbg.algo-api.aqumon.com', 8194)
import pandas as pd
import numpy as np
import datetime as dt
import os
from os import makedirs
from algorithm import addpath

def download_his(ticker, field_list, start_date, end_date,
                 currency=None, period="DAILY", dpdf=None, normal=True, abnormal=True, split=None, calendar=None):
    raw_df = LocalTerminal.get_historical(
        sids = ticker,
        flds =field_list,
        start=start_date,
        end=end_date,
        period=period, # [DAILY, WEEKLY, MONTHLY, QUARTERLY, SEMI_ANNUALLY, YEARLY]
        # period_adjustment='ACTUAL',# (ACTUAL, CALENDAR, FISCAL)
        currency=currency,
        # override_option=None,pricing_option=None,
        non_trading_day_fill_option='ACTIVE_DAYS_ONLY', #(NON_TRADING_WEEKDAYS | ALL_CALENDAR_DAYS | ACTIVE_DAYS_ONLY)
        non_trading_day_fill_method='PREVIOUS_VALUE', #(PREVIOUS_VALUE | NIL_VALUE)
        # max_data_points=None,
        # adjustment_normal=None,adjustment_abnormal=None, adjustment_split=None,
        adjustment_follow_DPDF=dpdf,
        adjustment_split = split,
        adjustment_normal = normal,
        adjustment_abnormal = abnormal,
        calendar_code_override=calendar
    )

    df = raw_df.as_frame()
    return df


def download_index(start, end):
    tsfactor_raw_data_path = os.path.join(addpath.historical_path, 'beta_factor', 'raw')
    daily_data_path = os.path.join(tsfactor_raw_data_path, "daily.csv")
    monthly_data_path = os.path.join(tsfactor_raw_data_path, "monthly.csv")
    quarterly_data_path = os.path.join(tsfactor_raw_data_path, "quarterly.csv")
    volume_data_path = os.path.join(tsfactor_raw_data_path, "volume.csv")

    ts_factor_list_bbg_daily = pd.read_csv(daily_data_path, index_col=0)
    ts_factor_list_bbg_monthly = pd.read_csv(monthly_data_path, index_col=0)
    ts_factor_list_bbg_quarterly = pd.read_csv(quarterly_data_path, index_col=0)
    ts_factor_list_bbg_volume = pd.read_csv(volume_data_path, index_col=0)

    current_list = ts_factor_list_bbg_daily.columns.tolist()
    data = download_his(
        current_list,
        ['PX_LAST'],
        start,
        end,
        period='DAILY',
        # currency='USD',
        dpdf=None,
        normal=True,
        abnormal=True,
        split=True,
        calendar=None
    ).ffill()
    data.columns = data.columns.get_level_values(0)
    data = data[current_list]
    print(data)
    data.to_csv(daily_data_path)

    # current_list = ts_factor_list_bbg_monthly.columns.tolist()
    # data = download_his(
    #     current_list,
    #     ['PX_LAST'],
    #     start,
    #     end,
    #     period='MONTHLY',
    #     # currency='USD',
    #     dpdf=None,
    #     normal=True,
    #     abnormal=True,
    #     split=True,
    #     calendar=None
    # ).ffill()
    # data.columns = data.columns.get_level_values(0)
    # data = data[current_list]
    # print(data)
    # data.to_csv(monthly_data_path)
    #
    # current_list = ts_factor_list_bbg_quarterly.columns.tolist()
    # data = download_his(
    #     current_list,
    #     ['PX_LAST'],
    #     start,
    #     end,
    #     period='QUARTERLY',
    #     # currency='USD',
    #     dpdf=None,
    #     normal=True,
    #     abnormal=True,
    #     split=True,
    #     calendar=None
    # ).ffill()
    # data.columns = data.columns.get_level_values(0)
    # data = data[current_list]
    # print(data)
    # data.to_csv(quarterly_data_path)
    #
    # current_list = ts_factor_list_bbg_volume.columns.tolist()
    # data = download_his(
    #     current_list,
    #     ['PX_VOLUME'],
    #     start,
    #     end,
    #     period='DAILY',
    #     # currency='USD',
    #     dpdf=None,
    #     normal=True,
    #     abnormal=True,
    #     split=True,
    #     calendar=None
    # ).ffill()
    # data.columns = data.columns.get_level_values(0)
    # data = data[current_list]
    # print(data)
    # data.to_csv(volume_data_path)

if __name__ == "__main__":
    download_index(start='2000-01-01', end='2021-01-15')