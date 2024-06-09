# -*- coding:utf-8 -*-



import syntheticDataGenerator
from core.performanceAnalyser import *
from utils.recordParser import *


if __name__ == '__main__':
    recordExample = syntheticDataGenerator.generateARecord()

    # p = SinglePortfolioPerformance()
    # p.loadData(recordParser(recordExample))
    # p.calculate()

    p = SinglePortfolioPerformance()
    a = PerformanceAnalyser()
    p.loadBenchmark(a.benchmark)
    p.loadData(recordParser(recordExample))
    p.set_obs_window(start_date='2014-01-06')
    p.calculate()
    p.print()

    hf = HFPortfolioPerformance()
    hf.loadBenchmark(a.benchmark)
    hf.loadData(recordParser(recordExample))
    hf.set_obs_window(start_date='2014-01-06')
    hf.calculate()
    hf.print()

    hf.loadHFParameters(frequency=2)
    hf.applyHighFrequency()
    hf.print()