import itertools
import multiprocessing
import os
import sys
import time


class Consumer(multiprocessing.Process):
	def __init__(self, queue):
		self.queue = queue
		super(Consumer, self).__init__()

	def run(self):
		while True:
			sec, reg, rat, algo_type_class,db_sync_setting = self.queue.get()
			if algo_type_class is None:
				self.queue.task_done()
				break
			try:
				execute(sec, reg, rat,algo_type_class,db_sync_setting)
			except:
				print sys.exc_info()
			self.queue.task_done()
		return


def run_combinations(algo_type_class, all_combinations, db_sync_setting):  # 0: no update db, # 1: update last weight  #:2:update historical weight

	concurrency = multiprocessing.cpu_count()
	tasks = multiprocessing.JoinableQueue()
	consumers = [Consumer(tasks) for i in xrange(concurrency)]
	for w in consumers:
		w.start()
	# ------------------------- #
	# sector_list = ALGOTYPE_BTSECTOR_MAP[algotype]
	# region_list = ALGOTYPE_BTREGION_MAP[algotype]
	# ratio_list = ALGOTYPE_RISK_RATIO_MAP[algotype]
	# sector_list = ['NON']
	# region_list =['NON']
	# all_combinations = list(itertools.product(sector_list, region_list, ratio_list))

	# single process
	# total = len(all_combinations)
	# count = 0
	# for sec, reg, rat in all_combinations:
	# 	execute(sec, reg, rat, algotype)
	# 	count += 1
	# 	print "%.0f%% completed" % (count * 100.0 / total)

	# multi processes
	for sec, reg, rat in all_combinations:
		tasks.put((sec, reg, rat,algo_type_class,db_sync_setting))

	t = time.time()
	for i in xrange(concurrency):
		tasks.put((None, None, None, None,None))
		tasks.join()
	print 'Total time: %ss' % (time.time() - t)
	return

def execute(sec, reg, rat,algo_type_class,db_sync_setting):
	# set benchmark, calendar, timezone
	pass