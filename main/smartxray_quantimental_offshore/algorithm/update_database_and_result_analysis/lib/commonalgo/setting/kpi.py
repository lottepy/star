from datetime import date

import numpy as np
import pandas as pd
import six
from dateutil.relativedelta import FR, SA, relativedelta

from . import convert, utils
from .calendar_utils import TradingCalendar

DAY = 86400 * 1000


def max_drawdown(price_ts):
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


class PeriodReturn(object):
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

    def get_last(self, asof=None):
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

    def get_last_close(self, asof=None):
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
        # return self.df.loc[dt.date():].iloc[0].name
        # return self.df[self.df >= dt.date()].iloc[0].name # MC
        if dt.date() < self.df.iloc[0].name:
            # 比如 df 是从2019-01-01开始，但是6 months ago, dt是2018-06-01那么则取不到2018-06-01的数据。
            # 为防止报错，应该采用 df 的第一天数据。
            return self.df.iloc[0].name
        else:
            return self.df.loc[dt.date():].iloc[0].name

    def get_yesterday_close(self, asof=None):
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

    def get_day_before_yesterday_close(self, asof=None):
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

    def today(self):
        return self.get_last(), self.get_yesterday_close()

    def yesterday(self):
        # return self.x_day(1)[1], self.x_day(2)[1]
        return self.get_yesterday_close(), self.get_day_before_yesterday_close()

    def x_day(self, x=1):
        dt = convert.get_current_datetime(self.region)
        for i in range(x):
            dt = self.calendar.previous_business_day(dt)
        return self.get_last(), self.get_last_close(dt)

    def x_month(self, x=1):
        dt = convert.get_current_datetime(self.region)
        dt -= relativedelta(months=x)
        if self.calendar.is_holiday(dt):
            dt = self.calendar.previous_business_day(dt)
        return self.get_last(), self.get_last_close(dt)

    def x_year(self, x=1):
        dt = convert.get_current_datetime(self.region)
        dt -= relativedelta(years=x)
        if self.calendar.is_holiday(dt):
            dt = self.calendar.previous_business_day(dt)
        return self.get_last(), self.get_last_close(dt)

    def this_week(self):
        dt = convert.get_current_datetime(self.region)
        market_open = dt.replace(hour=9, minute=0, second=0)
        if dt.weekday() in [5, 6]:  # Saturday, Sunday
            dt += relativedelta(weekday=FR(-2))
        elif dt.weekday() in [0, ] and dt < market_open:  # dt is Monday morning before market_open time.
            dt += relativedelta(weekday=SA(-2))
        else:
            dt += relativedelta(weekday=SA(-1))
        return self.get_last(), self.get_last_close(dt)

    def last_week(self):
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

    def ytd(self):
        dt = convert.get_current_datetime(self.region)
        dt = dt.replace(month=1, day=1)
        return self.get_last(), self.get_last_close(dt)

    def inception(self):
        return self.df.iloc[-1].name, self.df.iloc[0].name

    def ret(self, date_to, date_from):
        return (self.df.loc[date_to] / self.df.loc[date_from] - 1).tolist()

    def desc(self, date_to, date_from):
        return '{} to {} ({})'.format(date_from, date_to, self.region)

    def to_dict(self):
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

    def dates(self):
        return {
            'today': self.desc(*self.today()),
            'yesterday': self.desc(*self.yesterday()),
            'this week': self.desc(*self.this_week()),
            'last week': self.desc(*self.last_week()),
            '5 day': self.desc(*self.x_day(5)),
            '1 month': self.desc(*self.x_month(1)),
            '3 month': self.desc(*self.x_month(3)),
            '6 month': self.desc(*self.x_month(6)),
            'YTD': self.desc(*self.ytd()),
            '1 year': self.desc(*self.x_year(1)),
            '3 year': self.desc(*self.x_year(3)),
            '5 year': self.desc(*self.x_year(5)),
            'inception': self.desc(*self.inception())
        }


def cal_period_metrics(source_df, region):
    period_return = PeriodReturn(source_df, region)
    rets = period_return.to_dict()
    df = pd.DataFrame(
        columns=source_df.columns,
        index=list(rets.keys())
    )
    for i in df.index:
        df.loc[i] = rets[i]

    dates_df = pd.DataFrame.from_dict(period_return.dates(), orient='index')
    dates_df.rename(columns={0: 'dates'}, inplace=True)
    resp_df = pd.concat([df.round(6), dates_df], axis=1)
    return resp_df


def get_all_ticker_return(price_df, region):
    ticker_return_overview = cal_period_metrics(price_df, region)
    ticker_return_overview.loc['price'] = price_df.iloc[-1]
    return ticker_return_overview
