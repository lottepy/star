from sh532_live_engine_v2 import SmartHedgingEngine
from utils.crontask import register_task, run
from subprocess import call

# SmartHedgingEngine.update_vix_info()

@register_task('0 0 9 * * * HK')
def sh_live():
	# sh_engine.run_live_trading()
	SmartHedgingEngine.update_vix_info()

run()