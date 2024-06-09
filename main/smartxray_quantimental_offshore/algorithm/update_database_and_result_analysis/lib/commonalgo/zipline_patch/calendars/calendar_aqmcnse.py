#
# Copyright 2018 Quantopian, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from datetime import time
from itertools import chain

from pytz import timezone

# for creating and registering our calendar
from trading_calendars import register_calendar, TradingCalendar
from zipline.utils.memoize import lazyval
import os
import pandas as pd

filepath = os.path.join(os.path.dirname(__file__), "aqm_holidays_cn.csv")
SHSZ_holidays = pd.read_csv(filepath)['holidays'].tolist()
CNSZ_holidays = [pd.Timestamp(x, tz = 'UTC') for x in SHSZ_holidays]
# Useful resources for making changes to this file:
# http://www.nyse.com/pdfs/closings.pdf
# http://www.stevemorse.org/jcal/whendid.html


class AQMCNSExchangeCalendar(TradingCalendar):
    """
    Exchange calendar for the Shanghai/Shenzhen Stock Exchange by AQUMON (CNSE).

    Open Time: 9:31 AM, Asia/Shanghai
    Close Time: 3:00 PM, Asia/Shanghai
    """

    regular_early_close = time(11, 30)

    @property
    def name(self):
        return "CNSE"

    @property
    def tz(self):
        return timezone('Asia/Shanghai')

    @property
    def open_time(self):
        return time(9, 31)

    @property
    def close_time(self):
        return time(15)

    # @property
    # def regular_holidays(self):
    #     return HolidayCalendar([
    #         USNewYearsDay,
    #         USMartinLutherKingJrAfter1998,
    #         USPresidentsDay,
    #         GoodFriday,
    #         USMemorialDay,
    #         USIndependenceDay,
    #         USLaborDay,
    #         USThanksgivingDay,
    #         Christmas,
    #     ])

    @property
    def adhoc_holidays(self):
        return list(chain(
            CNSZ_holidays
        ))

    # @property
    # def special_closes(self):
    #     return [
    #         (self.regular_early_close, HolidayCalendar([
    #             MonTuesThursBeforeIndependenceDay,
    #             FridayAfterIndependenceDayExcept2013,
    #             USBlackFridayInOrAfter1993,
    #             ChristmasEveInOrAfter1993
    #         ])),
    #         (time(14), HolidayCalendar([
    #             ChristmasEveBefore1993,
    #             USBlackFridayBefore1993,
    #         ])),
    #     ]
    #
    # @property
    # def special_closes_adhoc(self):
    #     return [
    #         (self.regular_early_close, [
    #             '1997-12-26',
    #             '1999-12-31',
    #             '2003-12-26',
    #             '2013-07-03'
    #         ])
    #     ]
