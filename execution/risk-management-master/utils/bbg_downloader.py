from tia.bbg import Terminal
# LocalTerminal = Terminal('192.168.9.109', 18194)
LocalTerminal = Terminal('47.244.101.245', 18194)
import pandas as pd
import numpy as np
import datetime as dt

def download_his(ticker, field_list, start_date, end_date,
                 currency=None, period="DAILY", dpdf=True):
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
        # calendar_code_override=None
    )

    df = raw_df.as_frame()
    return df


def download_intraday(ticker='700 HK Equity', event='TRADE', start=None, end=None, interval=1):
    #  events: array containing any of (TRADE, BID, ASK, BID_BEST, ASK_BEST, MID_PRICE, AT_TRADE, BEST_BID, BEST_ASK)
    data = LocalTerminal.get_intraday_bar(sid=ticker, event=event,start=start, end=end, interval=interval)
    data = data.as_frame()
    return data


def download_ref(ticker, field_list):
    raw_df = LocalTerminal.get_reference_data(sids = ticker,flds=field_list)
    df = raw_df.as_frame()
    return df

# start = dt.datetime.combine(dt.date.today(), dt.time(9, 30))-dt.timedelta(days=1)
# end = dt.datetime.combine(dt.date.today(), dt.time(9, 59))
# ticker = '2822 HK EQUITY'
# event = 'BID'
# data = download_intraday(ticker,event=event,start=start,end=None)
# print (data)

#
# ticker_list = list(pd.read_csv('data/cur_code.csv',header=None)[0].values)

# ticker_list = ["AGG US Equity", "MUB US Equity", "TIP US Equity", "HYG US Equity",
#                "SHV US Equity", "FLOT US Equity", "BNDX US Equity", "EMB US Equity","LQD US Equity"]
# ticker_list =['BGCI Index','XBTUSD Curncy']
# ticker_list = ['H15T10Y Index','CPI YOY Index']
# ticker_list = ['BNDX US Equity','VTIP US Equity', 'BGRCTRUH Index','LG38TRUH Index','LTP5TRUU Index' ]
# ticker_list = ['GSUSFCI Index','GSEAFCI INDEX','EE0003M Index','US0003M Index']
# ticker_list = ['3141 HK EQUITY','2822 HK EQUITY','3140 HK EQUITY','3101 HK EQUITY','3081 HK EQUITY',
#                '3010 HK EQUITY','3147 HK EQUITY','3115 HK EQUITY','3169 HK EQUITY','2813 HK EQUITY']
# filed_list = ['TURNOVER','30_DAY_AVERAGE_TURNOVER_AT_TIME','DAY_TO_DAY_TOT_RETURN_GROSS_DVDS']
# df0 = download_his(ticker_list,['PX_LAST'],start,end,period="YEARLY")
# print (df0.head())
# df0.to_csv('curr_return.csv')


# data = download_ref(ticker_list,filed_list)
# data.to_csv('turnover_hk.csv')

# data = download_his(['CNYHKD CURNCY'], ['PX_LAST'], start, end, period='DAILY')
# print data