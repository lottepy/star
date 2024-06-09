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



class AQMFXExchangeCalendar(TradingCalendar):
    """
    A TradingCalendar for an exchange that is open every minute of every
    weekday.
    """

    @property
    def name(self):
        return "AQMFX"

    @property
    def tz(self):
        return timezone('UTC')

    @property
    def open_time(self):
        return time(21, 1)

    @property
    def close_time(self):
        return time(21)

    @property
    def open_offset(self):
        return -1

    @property
    def weekmask(self):
        """
        String indicating the days of the week on which the market is open.

        Default is '1111100' (i.e., Monday-Friday).

        See Also
        --------
        numpy.busdaycalendar
        """
        return '1111100'

    @lazyval
    def day(self):
        return CustomBusinessDay(
            calendar=self.regular_holidays,
            weekmask=self.weekmask
        )

