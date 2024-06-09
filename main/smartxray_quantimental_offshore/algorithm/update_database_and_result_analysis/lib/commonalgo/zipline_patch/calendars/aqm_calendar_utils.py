import trading_calendars
from .calendar_aqmcnse import AQMCNSExchangeCalendar
from .calendar_aqmhkse import AQMHKSExchangeCalendar
from .calendar_aqmnyse import AQMNYSExchangeCalendar
from .calendar_aqmallday import AQMALLDAYExchangeCalendar
from .calendar_aqmbday import AQMBIZDAYExchangeCalendar
from .calendar_aqmfx import AQMFXExchangeCalendar

def register_aqm_calendars(force = True):
	trading_calendars.register_calendar(
		name='CNSE',
		calendar=AQMCNSExchangeCalendar(),
		force=force
	)
	trading_calendars.register_calendar(
		name='HKSE',
		calendar=AQMHKSExchangeCalendar(),
		force=force
	)
	trading_calendars.register_calendar(
		name='USSE',
		calendar=AQMNYSExchangeCalendar(),
		force=force
	)
	trading_calendars.register_calendar(
		name='ALLDAY',
		calendar=AQMALLDAYExchangeCalendar(),
		force=force
	)
	trading_calendars.register_calendar(
		name='BIZDAY',
		calendar=AQMBIZDAYExchangeCalendar(),
		force=force
	)
	trading_calendars.register_calendar(
		name='AQMUK',
		calendar=trading_calendars.get_calendar('LSE'),
		force=force
	)
	trading_calendars.register_calendar(
		name='AQMFX',
		calendar=AQMFXExchangeCalendar(),
		force=force
	)


	aqm_calendar_alias = {
		'AQMCN': 'CNSE',
		'AQMHK': 'HKSE',
		'AQMUS': 'USSE',
		'SHSE': 'CNSE',
		'SZSE': 'CNSE',
		'HKEX': 'HKSE',
		'AQMALLDAY': 'ALLDAY',
		'WEEKDAY':'BIZDAY',
		'AQMFXGLOBAL':'AQMFX'
	}

	for k, v in aqm_calendar_alias.items():
		trading_calendars.register_calendar_alias(
			alias=k,
			real_name=v,
			force=True
		)
