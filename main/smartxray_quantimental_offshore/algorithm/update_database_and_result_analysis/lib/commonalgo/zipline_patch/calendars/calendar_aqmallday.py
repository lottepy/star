#
# Copyright 2017 AQUMON, Inc.
#

from datetime import time
from itertools import chain
from pytz import timezone
from pandas import Timestamp
from trading_calendars import TradingCalendar, register_calendar
from pandas import read_csv
import os
from zipline.utils.memoize import remember_last, lazyval
from pandas.tseries.offsets import CustomBusinessDay

filepath = os.path.join(os.path.dirname(__file__), "aqm_holidays_bday.csv")
US_Holidays = read_csv(filepath)['holidays'].tolist()
US_Holidays = [Timestamp(x, tz = 'UTC') for x in US_Holidays]


class AQMALLDAYExchangeCalendar(TradingCalendar):
    """
    Exchange calendar for US

    Open Time: 9:30 AM, US/Eastern
    Close Time: 4:00 PM, US/Eastern
    """

    # regular_early_close = time(13)

    @property
    def name(self):
        return "AQMALLDAY"

    @property
    def tz(self):
        return timezone('UTC')

    @property
    def open_time(self):
        return time(0, 0)

    @property
    def close_time(self):
        return time(23,59)

    @property
    def adhoc_holidays(self):
        return list(chain(
            US_Holidays
        ))

    @lazyval
    def day(self):
        return CustomBusinessDay(
            holidays=self.adhoc_holidays,
            calendar=self.regular_holidays,
            weekmask='1111111'
        )

