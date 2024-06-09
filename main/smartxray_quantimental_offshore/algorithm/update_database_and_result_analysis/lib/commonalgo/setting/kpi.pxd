from cpython.datetime cimport datetime, date

cdef int DAY

cpdef object max_drawdown(object price_ts)
cpdef object cal_period_metrics(object source_df, str region)
cpdef object get_all_ticker_return(object price_df, str region)


cdef class PeriodReturn(object):
    cdef object df
    cdef str region
    cdef object calendar

    cpdef date get_last(self, datetime asof=*)
    cpdef date get_last_close(self, datetime asof=*)
    cpdef date get_yesterday_close(self, datetime asof=*)
    cpdef date get_day_before_yesterday_close(self, datetime asof=*)

    cpdef today(self)
    cpdef yesterday(self)
    cpdef x_day(self, int x=*)
    cpdef x_month(self, int x=*)
    cpdef x_year(self, int x=*)
    cpdef this_week(self)
    cpdef last_week(self)
    cpdef ytd(self)
    cpdef inception(self)

    cpdef list ret(self, date date_to, date date_from)
    cpdef str desc(self, date date_to, date date_from)
    cpdef dict to_dict(self)
    cpdef list dates(self)
