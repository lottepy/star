from tia.bbg import Terminal
from ..setting.network_utils import B_HOST,B_PORT
# https://confluence-algo.aqumon.com/display/TEAM/BBG+API
# LocalTerminal = Terminal('172.31.86.16', 18195) # new jump host
LocalTerminal = Terminal(B_HOST, int(B_PORT)) # new host
import pandas as pd
import numpy as np
import datetime as dt

def download_his(ticker, field_list, start_date, end_date, currency=None, period="DAILY",DPDF=False,
                 calendar_option = 'ACTIVE_DAYS_ONLY', fill_method = 'NIL_VALUE'):
    raw_df = LocalTerminal.get_historical(
        sids = ticker,
        flds =field_list,
        start=start_date,
        end=end_date,
        period=period, # [DAILY, WEEKLY, MONTHLY, QUARTERLY, SEMI_ANNUALLY, YEARLY]
        # period_adjustment='ACTUAL',# (ACTUAL, CALENDAR, FISCAL)
        currency=currency,
        # override_option=None,pricing_option=None,
        non_trading_day_fill_option= calendar_option, #(NON_TRADING_WEEKDAYS | ALL_CALENDAR_DAYS | ACTIVE_DAYS_ONLY)
        non_trading_day_fill_method= fill_method, #(PREVIOUS_VALUE | NIL_VALUE)
        # max_data_points=None,
        # adjustment_normal=None,adjustment_abnormal=None, adjustment_split=None,
        adjustment_follow_DPDF=DPDF,
        # calendar_code_override=None
        )

    df = raw_df.as_frame()
    return df


def download_intraday(ticker='700 HK Equity',event='TRADE',start=None, end=None, interval=1, DPDF = False):
    # ticker: single ticker only
    #  events: array containing any of (TRADE, BID, ASK, BID_BEST, ASK_BEST, MID_PRICE, AT_TRADE, BEST_BID, BEST_ASK)
    data = LocalTerminal.get_intraday_bar(sid=ticker, event=event,start=start, end=end, interval=interval, adjustment_follow_DPDF=DPDF)
    data = data.as_frame()
    return data
 
def download_intraday_tick(ticker='700 HK Equity',event='TRADE',start=None, end=None):
    # ticker: single ticker only
    #  events: array containing any of (TRADE, BID, ASK, BID_BEST, ASK_BEST, MID_PRICE, AT_TRADE, BEST_BID, BEST_ASK)
    data = LocalTerminal.get_intraday_tick(sids=ticker, events=event,start=start, end=end)
    data = data.as_frame()
    return data 

def download_ref(ticker, field_list):
    raw_df = LocalTerminal.get_reference_data(sids = ticker,flds=field_list)
    df = raw_df.as_frame()
    return df

def download_screener(name0,group0='General'):
    data=LocalTerminal.get_screener(name=name0, group=group0, type='GLOBAL', asof=None, language=None)
    data=data.as_frame()
    return data


