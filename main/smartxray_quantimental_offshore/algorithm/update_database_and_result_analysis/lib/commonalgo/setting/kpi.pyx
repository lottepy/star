from cpython.datetime cimport date

import numpy as np
import pandas as pd
import six
from dateutil.relativedelta import FR, SA, relativedelta

from common import convert, utils
from .calendar_utils cimport TradingCalendar

DAY = 86400 * 1000


cpdef object max_drawdown(object price_ts):
    # i = np.argmax(np.maximum.accumulate(price_ts) - price_ts)  # end of the period
    # j = np.argmax(price_ts[:i])
    i = (np.maximum.accumulate(price_ts) - price_ts).astype('float').idxmax()  # end of the period
    j = (price_ts[:i]).astype('float').idxmax()
    # start of period
    return (price_ts[j] - price_ts[i]) / price_ts[j]


@utils.exception_safe(exception_return={})
def kpi_from_series(price_ts, benchmark=None, region=None):
    start_ts = price_ts.index[0]
    end_ts = price_ts.index[-1]
    if region and isinstance(start_ts, six.string_types):
        dt_start = convert.strptime_with_tz(start_ts, region)
        dt_end = convert.strptime_with_tz(end_ts, region)
        days_span = (dt_end.datetime() - dt_start.datetime()).days
    elif isinstance(start_ts, date):
        days_span = (end_ts - start_ts).days
    else:
        days_span = (end_ts - start_ts) / DAY

    rf_rate = 0.00
    return_ts = price_ts.pct_change().fillna(0)

    acc_ret = (price_ts.ix[end_ts] / price_ts.ix[start_ts] - 1)
    ann_ret = (np.power(1 + acc_ret, 365.25 / days_span) - 1)
    ann_vol = (return_ts.std() * np.sqrt(252))

    summary = {
        'acc_ret': acc_ret,
        'ann_ret': ann_ret,
        'ann_vol': ann_vol,
        'sharpe_ratio': (ann_ret - rf_rate) / ann_vol,
        'max_drawdown': max_drawdown(price_ts),
        'information_ratio': 0
    }

    return summary


cdef class PeriodReturn(object):
    def __init__(self, df: pd.DataFrame, base_region):
        new_index = pd.date_range(
            start=df.last_valid_index(),
            end=convert.get_current_datetime(base_region).date(),
        ).map(lambda dt: dt.date())
        df = df[~df.index.duplicated(keep='first')]
        df = df.reindex(df.index.append(new_index).drop_duplicates())
        self.df = df.ffill()
        self.region = base_region
        self.calendar = TradingCalendar(base_region)

    cpdef date get_last(self, datetime asof=None):
        dt = asof or convert.get_current_datetime(self.region)
        market_open = dt.replace(hour=9, minute=0, second=0)
        # market_close = dt.replace(hour=16, minute=0, second=0)
        if self.calendar.is_holiday(dt):
            dt = self.calendar.previous_business_day(dt)
        elif dt < market_open:
            dt = self.calendar.previous_business_day(dt)
        else:
            pass
        return self.df.loc[dt.date():].iloc[0].name
        # return self.df.iloc[-1].name

    cpdef date get_last_close(self, datetime asof=None):
        dt = asof or convert.get_current_datetime(self.region)
        market_open = dt.replace(hour=9, minute=0, second=0)
        market_close = dt.replace(hour=16, minute=0, second=0)
        if self.calendar.is_holiday(dt):
            dt = self.calendar.previous_business_day(dt)
        elif dt < market_open:
            dt = self.calendar.previous_business_day(dt)
        # elif market_open <= dt <= market_close:
        #     dt = self.calendar.previous_business_day(dt)
        else:
            pass
        return self.df.loc[dt.date():].iloc[0].name

    cpdef date get_yesterday_close(self, datetime asof=None):
        dt = asof or convert.get_current_datetime(self.region)
        market_open = dt.replace(hour=9, minute=0, second=0)
        market_close = dt.replace(hour=16, minute=0, second=0)
        if self.calendar.is_holiday(dt):
            dt = self.calendar.multi_previous_business_day(dt, 2)
        elif dt < market_open:
            dt = self.calendar.multi_previous_business_day(dt, 2)
        elif market_open <= dt <= market_close:
            dt = self.calendar.multi_previous_business_day(dt, 1)
        else:
            dt = self.calendar.multi_previous_business_day(dt, 1)
        return self.df.loc[dt.date():].iloc[0].name

    cpdef date get_day_before_yesterday_close(self, datetime asof=None):
        dt = asof or convert.get_current_datetime(self.region)
        market_open = dt.replace(hour=9, minute=0, second=0)
        market_close = dt.replace(hour=16, minute=0, second=0)
        if self.calendar.is_holiday(dt):
            dt = self.calendar.multi_previous_business_day(dt, 3)
        elif dt < market_open:
            dt = self.calendar.multi_previous_business_day(dt, 3)
        elif market_open <= dt <= market_close:
            dt = self.calendar.multi_previous_business_day(dt, 2)
        else:
            dt = self.calendar.multi_previous_business_day(dt, 2)
        return self.df.loc[dt.date():].iloc[0].name

    cpdef today(self):
        return self.get_last(), self.get_yesterday_close()

    cpdef yesterday(self):
        # return self.x_day(1)[1], self.x_day(2)[1]
        return self.get_yesterday_close(), self.get_day_before_yesterday_close()

    cpdef x_day(self, int x=1):
        dt = convert.get_current_datetime(self.region)
        for i in range(x):
            dt = self.calendar.previous_business_day(dt)
        return self.get_last(), self.get_last_close(dt)

    cpdef x_month(self, int x=1):
        dt = convert.get_current_datetime(self.region)
        dt -= relativedelta(months=x)
        if self.calendar.is_holiday(dt):
            dt = self.calendar.previous_business_day(dt)
        return self.get_last(), self.get_last_close(dt)

    cpdef x_year(self, int x=1):
        dt = convert.get_current_datetime(self.region)
        dt -= relativedelta(years=x)
        if self.calendar.is_holiday(dt):
            dt = self.calendar.previous_business_day(dt)
        return self.get_last(), self.get_last_close(dt)

    cpdef this_week(self):
        dt = convert.get_current_datetime(self.region)
        market_open = dt.replace(hour=9, minute=0, second=0)
        if dt.weekday() in [5, 6]:  # Saturday, Sunday
            dt += relativedelta(weekday=FR(-2))
        elif dt.weekday() in [0, ] and dt < market_open:  # dt is Monday morning before market_open time.
            dt += relativedelta(weekday=SA(-2))
        else:
            dt += relativedelta(weekday=SA(-1))
        return self.get_last(), self.get_last_close(dt)

    cpdef last_week(self):
        dt = convert.get_current_datetime(self.region)
        market_open = dt.replace(hour=9, minute=0, second=0)
        if dt.weekday() in [5, 6]:  # Saturday, Sunday
            dt += relativedelta(weekday=FR(-2))
            dt_1 = dt + relativedelta(weekday=FR(-3))
        elif dt.weekday() in [0, ] and dt < market_open:  # dt is Monday morning before market_open time.
            dt += relativedelta(weekday=SA(-2))
            dt_1 = dt + relativedelta(weekday=SA(-2))
        else:
            dt += relativedelta(weekday=SA(-1))
            dt_1 = dt + relativedelta(weekday=SA(-2))
        return self.get_last_close(dt), self.get_last_close(dt_1)

    cpdef ytd(self):
        dt = convert.get_current_datetime(self.region)
        dt = dt.replace(month=1, day=1)
        return self.get_last(), self.get_last_close(dt)

    cpdef inception(self):
        return self.df.iloc[-1].name, self.df.iloc[0].name

    cpdef list ret(self, date date_to, date date_from):
        return (self.df.loc[date_to] / self.df.loc[date_from] - 1).tolist()

    cpdef str desc(self, date date_to, date date_from):
        return '{} to {} ({})'.format(date_from, date_to, self.region)

    cpdef dict to_dict(self):
        return {
            'today': self.ret(*self.today()),
            'yesterday': self.ret(*self.yesterday()),
            'this week': self.ret(*self.this_week()),
            'last week': self.ret(*self.last_week()),
            '5 day': self.ret(*self.x_day(5)),
            '1 month': self.ret(*self.x_month(1)),
            '3 month': self.ret(*self.x_month(3)),
            '6 month': self.ret(*self.x_month(6)),
            'YTD': self.ret(*self.ytd()),
            '1 year': self.ret(*self.x_year(1)),
            '3 year': self.ret(*self.x_year(3)),
            '5 year': self.ret(*self.x_year(5)),
            'inception': self.ret(*self.inception())
        }

    cpdef list dates(self):
        return [
            self.desc(*self.today()),
            self.desc(*self.yesterday()),
            self.desc(*self.this_week()),
            self.desc(*self.last_week()),
            self.desc(*self.x_day(5)),
            self.desc(*self.x_month(1)),
            self.desc(*self.x_month(3)),
            self.desc(*self.x_month(6)),
            self.desc(*self.ytd()),
            self.desc(*self.x_year(1)),
            self.desc(*self.x_year(3)),
            self.desc(*self.x_year(5)),
            self.desc(*self.inception())
        ]


cpdef object cal_period_metrics(object source_df, str region):
    period_return = PeriodReturn(source_df, region)
    rets = period_return.to_dict()
    df = pd.DataFrame(
        columns=source_df.columns,
        index=list(rets.keys())
    )
    for i in df.index:
        df.loc[i] = rets[i]

    df['dates'] = period_return.dates()
    return df


cpdef object get_all_ticker_return(object price_df, str region):
    ticker_return_overview = cal_period_metrics(price_df, region)
    ticker_return_overview.loc['price'] = price_df.iloc[-1]
    return ticker_return_overview
