from cpython.datetime cimport datetime, date, timedelta

cpdef dict OFFICIAL_HOLIDAYS
cpdef dict ADHOC_HOLIDAYS
cdef list WEEKEND
cdef str DATE_FORMAT


# Monday is 1, Sunday is 7. date.isoweekday()
cdef class TradingCalendar(object):
    cdef set holidays

    cpdef str to_string(self, object dt)

    cpdef datetime shift(self, datetime dt, int days)

    cpdef datetime next_business_day(self, datetime dt)

    cpdef datetime previous_business_day(self, datetime dt)

    cpdef datetime multi_previous_business_day(self, datetime dt, int num)

    cpdef datetime end_of_month(self, datetime dt)

    cpdef int is_holiday(self, datetime dt)

    cpdef int is_weekend(self, object dt)

    cpdef int is_leap_year(self, datetime dt)

    cpdef int is_first_business_day(self, datetime dt)

    cpdef int is_last_business_day(self, datetime dt)
