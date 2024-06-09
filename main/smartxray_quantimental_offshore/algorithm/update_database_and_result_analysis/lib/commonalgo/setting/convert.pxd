from cpython.datetime cimport datetime, date

cdef dict REGION_TZ_MAP
cdef int SECOND
cdef int MINUTE
cdef int HOUR
cdef int DAY
cdef int WEEK

cpdef int ip_to_int(str)

cpdef str int_to_ip(int)

cpdef str timestamp_to_string(long)

cpdef str get_tz(str)

cpdef object get_tzinfo(str)

cpdef long datetime_to_timestamp(datetime, str unit=*)

cpdef long now(str unit=*)

cpdef long parse_as_timestamp(str, str unit=*)

cpdef datetime timestamp_to_datetime(long, str)

cpdef datetime get_current_datetime(str)

cpdef object convert_tz(datetime, str)

cpdef datetime strptime_with_tz(str, str)

cpdef datetime parse_localtime(str, str)

cpdef str format_timestamp(long, str, str)

cpdef date format_date(str)

cpdef datetime localize(datetime, str)

cpdef long get_when_timestamp(str, str, str unit=*)
