# -*- coding:utf-8 -*-
from core.performanceAnalyser import *

# Examples
# 1.
# Analyse portfolio total pnl
Anaylyser = PerformanceAnalyser()
Anaylyser.analysePortfolioPerformanceInFolder(dirPath='./data/examples',
                                              disp=True,
                                              start_date='2014-01-10',
                                              startswith='result_eventCombo',
                                              within = 'takeProfit_100_takeLoss_-1'
                         )
Anaylyser.aggregatedMetrics.to_csv('./result/metrics_portfolio.csv')

# 2.
# Analyse trade history
Anaylyser = PerformanceAnalyser()
Anaylyser.analysePairedTradeRecordInFolder(dirPath='./data/examples',
                                              disp=True,
                                              startswith='historyTrade',
                                              within = 'takeProfit_100_takeLoss_-1'
                         )
Anaylyser.aggregatedMetrics.to_csv('./result/metrics_trade.csv')