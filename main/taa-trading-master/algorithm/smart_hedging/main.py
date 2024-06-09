from algorithm.smart_hedging.hedging_live import SmartHedging
from datetime import datetime
from pytz import timezone
import time

ONE_HOUR = 3600
HALF_HOUR = 1800


if __name__ == '__main__':
	sh = SmartHedging(
		account='U3245801',
		currency='USD',
		region='US',
		oss_key='sh',
		hedging_status='HEDGING',
		hedging_asset='VXX',
		safe_asset='JPST',
		entry_pctl=40,
		exit_pctl=96,
		hedging_ratio=0.05
	)

	while True:
		dt = datetime.now(tz=timezone('US/Eastern'))
		am_open = dt.replace(hour=9, minute=30, second=0)
		pm_close = dt.replace(hour=16, minute=0, second=0)

		if dt > am_open and dt < pm_close:
			sh.run()
			time.sleep(HALF_HOUR)
		else:
			sh.logger.info('Market close.')
			time.sleep(ONE_HOUR)
