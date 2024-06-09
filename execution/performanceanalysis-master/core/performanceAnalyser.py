# -*- coding:utf-8 -*-
import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from collections import OrderedDict
from pandas.plotting import register_matplotlib_converters

from utils.calculateHHI import getHHI

class BasePerformance(object):
    def __init__(self,name = None):
        '''
        The basic idea of a performance:
            1. take in a dataframe of portfolio value OR trade record OR paired trade record OR other data
            2. (transform)
            3. calculate metrics
            4. plot and print and output metrics
        :param name: the name assigned.
        :param data: if data has been input, transform the data. the structure of data can be different for different subclasses.
        '''
        self.name = None  # str: the name of the portfolio
        self.date = None  # list: store the date the portfolio exists. 现在并没有用到. 设计来做一些日历上的校验.

        self.start_date = None  # datetime: store the start date for further analysis.
        self.end_date = None  # datetime: store the end date for further analysis

        self.benchmark = None  # DF: store the benchmark. ['benchmarkClose','benchmarkLogRtn','benchmarkSimpleRtn']

        if name is None:
            self.name = 'Default'
        else:
            self.name = name

        self.metrics= OrderedDict() # store metrics

    def metrics2df(self):
        '''
        transform the metrics to dataframe. If some metrics are not calculated, then omit them.
        :return:
        '''
        columns = []
        values = []
        for key, value in self.metrics.items():
            if value is not None:
                columns.append(key)
                values.append(value)
        self.metrics_df = pd.DataFrame([values], columns=columns)

    def loadData(self,data,**kwargs):
        # default load data
        print("***Default loadData hasn't been implemented yet!***")

    def loadBenchmark(self,benchmark):
        '''
        load benchmark if needed.
        :param benchmark:
        :return:
        '''
        benchmark.index = pd.to_datetime(benchmark.index) # 确保benchmark的index是datetime格式
        self.benchmark = benchmark

    def loadCalendar(self,calendar,offset=0):
        '''
        录入交易日历. 一方面是double check是否有反常的交易记录(未实现). 一方面是需要给出index和具体日期(datetime)之间的相互转换的字典,
        来方便一些操作.
        :param calendar: list. 包含所有的交易日
        :param offset: int. 若输入的日历和所需的日历之间是错位的, 用offset来调整位置.
        :return:
        '''
        ix = np.arange(len(calendar))+offset
        self.ix2Calendar = {i:j for i,j in zip(ix,calendar)}
        self.calendar2ix = {j:i for i,j in zip(ix,calendar)}

    def set_obs_window(self,start_date=None,end_date=None):
        '''
        set observation windows.
        will update start_date/end_date
        :param start_date: datetime. 开始分析的日期
        :param end_date: datetime. 结束分析的日期
        :return:
        '''
        if start_date is not None:
            self.start_date = pd.to_datetime(start_date)

        if end_date is not None:
            self.end_date = pd.to_datetime(end_date)

    def clear_obs_window(self):
        '''
        Clear the start_date and end_date
        Will update start_date, end_date
        :return:
        '''
        self.start_date = None
        self.end_date = None

    def calculate(self):
        # default calculate
        print("***Default calculate hasn't been implemented yet!***")

    def plot(self):
        # default plot
        print("***Default plot hasn't been implemented yet!***")

    def print(self):
        # default print
        print("***Default print hasn't been implemented yet!***")

    def evalMetrics(self,metric_key):
        # return the value of specific metric
        return self.metrics.get(metric_key,np.nan)


class SinglePortfolioPerformance(BasePerformance):
    '''
    Input: a single portfolio's total value, position value, cash value
           a benchmark
    Output: some metrics / plots
    '''
    def __init__(self,name = None):
        super(SinglePortfolioPerformance,self).__init__(name=name)

        # 初始化attr
        self.standardHolding = None  # DF: store the date/portfolioValue/cash/positionValue together
        self.holdingInUse = None  # DF: truncated holding record
        self.holdingInUsewithBenchmark = None  # DF: truncated holding record with benchmark performance
        self.portfolioValue = None  # np.array: store the total value of the portfolio
        self.cash = None  # np.array: store the remaining cash of the portfolio
        self.positionValue = None  # np.array: store the position value of the portfolio
        # Metrics:
        self.metrics = OrderedDict((
            ('totalReturn', None),  # float
            ('annualizedReturn', None),  # float
            ('annualizedVolatility', None),  # float
            ('sharpeRatio', None),  # float
            ('sharpeRatio(Annualized)', None),  # float
            ('maxDrawdown', None),  # float
            ('maxSingleDrop', None),  # float
            ('VaR', None),  # float
            ('calmarRatio', None),  # float
            ('totalExcessRtn', None),  # float
            ('annualizedExcessRtn', None),  # float
            ('annulizedTrackingError', None),  # float
            ('informationRatio(Annualized)', None),  # float
        ))
        self.metrics_df = None

    def loadData(self,data,**kwargs):
        '''
        Will update standardHolding, portfolioValue, cash, positionValue.
        The input data must contain the performance of a single portfolio. must contain a date column,
            and at least two of (a portfolio value column, a cash value column or a position value column)
        :param data: A dataframe contains the performance of a portfolio
        :param kwargs:
        :param date_col: the column number of date. -1 means index
        :param portfolio_col: the column number of portfolioValue
        :param cash_col: the column number of cash
        :param position_col: the column number of positionValue
        :return:
        '''
        date_col = -1  # default as index
        portfolio_col = 0  # default
        cash_col = 1  # default
        position_col = 2  # default

        if 'date_col' in kwargs.keys():
            date_col = kwargs['date_col']

        if 'portfolio_col' in kwargs.keys():
            portfolio_col = kwargs['portfolio_col']

        if 'cash_col' in kwargs.keys():
            cash_col = kwargs['cash_col']

        if 'position_col' in kwargs.keys():
            position_col = kwargs['position_col']

        if date_col == -1:
            data.loc[:,'date'] = pd.to_datetime(data.index) # 将date添加入data的最后一列, 之后仍会作为index
        self.standardHolding = data.iloc[:,[date_col,portfolio_col,cash_col,position_col]]
        self.standardHolding.columns = ['date','portfolioValue','cash','positionValue']
        self.standardHolding = self.standardHolding.set_index('date',drop=True)

        self.date = self.standardHolding.index.tolist() # 生成全时段的日期列表
        self.portfolioValue = self.standardHolding.loc[:,'portfolioValue'] # 生成全时段的组合价值
        self.cash = self.standardHolding.loc[:,'cash'] # 生成全时段的现金价值
        self.positionValue = self.standardHolding.loc[:,'positionValue'] # 生成全时段的持仓价值

    def calculate(self):
        # 计算 metrics
        # ***IMPORTANT***
        # 计算metrics时使用simple return还是log return还需要进一步明确. 目前是混用的.
        # Copy values
        holdingInUse = self.standardHolding.copy(deep=True) # The record of changes in holding values & portfolio values & cash values

        # Truncate the date
        # 将全时段的表现记录截断成所需要的时间, 如过去1年, 过去3年, 2018年等
        if self.start_date is not None:
            holdingInUse = holdingInUse.loc[holdingInUse.index >= self.start_date]
        if self.end_date is not None:
            holdingInUse = holdingInUse.loc[holdingInUse.index <= self.end_date]

        # Calculate basics
        n = len(holdingInUse) # 交易天数
        holdingInUse.loc[:,'holdingWeight'] = holdingInUse.loc[:,'positionValue']/holdingInUse.loc[:,'portfolioValue'] # 仓位
        holdingInUse.loc[:,'simpleRtn'] = holdingInUse.loc[:,'portfolioValue'].pct_change() # simple return
        holdingInUse.loc[:,'logRtn'] = np.log(holdingInUse.loc[:,'portfolioValue']).diff() # log return
        holdingInUse = holdingInUse.fillna(0) # 主要是第一天.

        # Calculate sharpe ratio
        # 使用log return/ 可以考虑用simple return
        self.metrics['sharpeRatio'] = holdingInUse.loc[:,'simpleRtn'].mean()/(holdingInUse.loc[:,'simpleRtn'].std()+1e-6)

        # Calculate total return
        # self.metrics['totalReturn'] = holdingInUse.loc[:,'logRtn'].sum()
        # 不使用log return的sum, 来避免可能的误差.
        self.metrics['totalReturn'] = holdingInUse.loc[holdingInUse.index[-1],'portfolioValue'] / holdingInUse.loc[holdingInUse.index[0],'portfolioValue'] - 1

        # Calculate annualized return
        # 年化复利计算方法:
        self.metrics['annualizedReturn'] = np.power(self.metrics['totalReturn'] + 1., 252. / n) - 1

        # Calculate maximum drawdown
        pv = holdingInUse.loc[:,'portfolioValue']
        pv = pv/pv.iloc[0]
        self.metrics['maxDrawdown'] = (pv.div(pv.cummax()) - 1.).min()

        # Calculate annualized volatility
        # calculate through the volatility of daily simple return
        self.metrics['annualizedVolatility'] = holdingInUse.loc[:,'simpleRtn'].std() * np.sqrt(252.)

        # Calcualte annualized sharpe ratio
        # annualized return (simple return) / annualized volatility
        # annualized sharpe ratio 和 daily sharpe ratio 的关系约为sqrt(252)倍. 但是要注意上面计算的daily sharpe ratio 用的是
        #   log return
        self.metrics['sharpeRatio(Annualized)'] = self.metrics['annualizedReturn']/(self.metrics['annualizedVolatility']+1e-6)

        # Calculate maximum daily drop
        self.metrics['maxSingleDrop'] = holdingInUse.loc[:,'simpleRtn'].min()

        # Calculate 99% VaR
        # 使用的是截取时段的所有记录.
        # 可以考虑添加一个500 days VaR
        self.metrics['VaR'] = - holdingInUse.loc[:,'simpleRtn'].mean() + holdingInUse.loc[:,'simpleRtn'].std() * 2.32

        # Calculate Calmar Ratio
        self.metrics['calmarRatio'] = self.metrics['annualizedReturn'] / (abs(self.metrics['maxDrawdown']) + 1e-6)

        # Calculate benchmard related metrics
        if self.benchmark is not None:
            # make sure that the benchmark has enough length
            assert holdingInUse.index[0] >= self.benchmark.index[0]
            assert holdingInUse.index[-1] <= self.benchmark.index[-1]

            holdingInUsewithBenchmark = pd.concat([holdingInUse,self.benchmark],axis=1)
            # 去除多余的benchmark
            holdingInUsewithBenchmark = holdingInUsewithBenchmark.loc[holdingInUsewithBenchmark.index >= holdingInUse.index[0]]
            holdingInUsewithBenchmark = holdingInUsewithBenchmark.loc[holdingInUsewithBenchmark.index <= holdingInUse.index[-1]]

            holdingInUsewithBenchmark.fillna(0,inplace=True) # 非必要, 一般情况下benchmark长度会比较长, 且不应该有nan

            # 计算机会成本
            # 机会成本的定义为
            #       将投资到组合的资金投资到benchmark上. 每日rebalance.
            # 机会成本适用于并不是时刻都满仓的策略(择时策略)
            holdingInUsewithBenchmark.loc[:,'opportunityCost'] = holdingInUsewithBenchmark.loc[:,'holdingWeight'] * holdingInUsewithBenchmark.loc[:,'benchmarkLogRtn']
            holdingInUsewithBenchmark.loc[:,'opportunityCost_simple'] = holdingInUsewithBenchmark.loc[:,'holdingWeight'] * holdingInUsewithBenchmark.loc[:,'benchmarkSimpleRtn']

            # 计算超额收益
            holdingInUsewithBenchmark.loc[:,'excessRtn'] = holdingInUsewithBenchmark.loc[:,'logRtn'] - holdingInUsewithBenchmark.loc[:,'opportunityCost']
            holdingInUsewithBenchmark.loc[:,'excessRtn_simple'] = holdingInUsewithBenchmark.loc[:,'simpleRtn'] - holdingInUsewithBenchmark.loc[:,'opportunityCost_simple']


            # Calculate total excess return
            # 这里使用了log return形式的超额收益的累和. 和使用simple return形式的超额收益的累乘计算会有误差.
            self.metrics['totalExcessRtn'] = holdingInUsewithBenchmark.loc[:,'excessRtn'].sum()

            # Calculate total excess return p.a.
            self.metrics['annualizedExcessRtn'] = np.power(self.metrics['totalExcessRtn'] + 1., 252. / n) - 1

            # Calculate tracking error
            self.metrics['annualizedTrackingError'] = holdingInUsewithBenchmark.loc[:,'excessRtn'].std() * np.sqrt(252.)

            # Calculate information ratio
            self.metrics['informationRatio(Annualized)'] = self.metrics['annualizedExcessRtn']/(self.metrics['annualizedTrackingError'] + 1e-6)

            self.holdingInUsewithBenchmark = holdingInUsewithBenchmark # 保存处理后的数据

        self.holdingInUse = holdingInUse # 保存处理后的数据

        self.metrics2df() # 生成用于输出的dataframe

    def plot(self,disp=False, saveToDefault=False,saveToPath=None):
        '''
        plot cumulative return (benchmark)
        plot excess return
        plot holding weight
        :return:
        '''
        # if benchmark is attached
        register_matplotlib_converters()

        # 只实现了带有benchmark的情况
        if self.holdingInUsewithBenchmark is not None:
            temp = self.holdingInUsewithBenchmark
            # Use cumsum of single day's log return
            # temp.loc[:,'cRtn_portfolio'] = temp.loc[:,'logRtn'].cumsum()
            # temp.loc[:,'cRtn_benchmark'] = temp.loc[:,'opportunityCost'].cumsum()
            # temp.loc[:,'cExcessRtn'] = temp.loc[:,'excessRtn'].cumsum()

            # Use cumproduct of single day's return
            temp.loc[:, 'cRtn_portfolio'] = (1+temp.loc[:, 'dailyRtn']).cumprod()-1
            temp.loc[:, 'cRtn_benchmark'] = (1+temp.loc[:, 'opportunityCost_simple']).cumprod()-1
            temp.loc[:, 'cExcessRtn'] = (1+temp.loc[:, 'excessRtn_simple']).cumprod()-1

            x_time = temp.index

            fig, (ax1, ax2, ax3) = plt.subplots(3, 1)
            plt.suptitle(self.name)
            ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
            ax1.xaxis.set_major_locator(mdates.YearLocator())
            ax1.plot(x_time, temp.loc[:,'cRtn_portfolio'], color='red', label='portfolio')
            ax1.plot(x_time, temp.loc[:,'cRtn_benchmark'], color='blue', label='benchmark')
            ax1.legend()

            ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
            ax2.xaxis.set_major_locator(mdates.YearLocator())
            ax2.set_ylabel('cExcessRtn',color='red')
            ax2.plot(x_time, temp.loc[:, 'cExcessRtn'], color='red', label='cumulativeExcessRtn')
            ax2_ = ax2.twinx()
            ax2_.set_ylabel('ExcessRtn',color='blue')
            ax2_.plot(x_time, temp.loc[:, 'excessRtn'], color='blue', label='ExcessRtn',linewidth=0.5)
            # ax2.legend()
            # ax2_.legend()
            ax3.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
            ax3.xaxis.set_major_locator(mdates.YearLocator())
            ax3.plot(x_time, temp.loc[:,'holdingWeight'], color='pink', label='holdingWeight',linewidth=0.5)
            ax3.legend()
            # plt.savefig(os.path.join(pic_save_path, 'figure_' + str(self.pic_count) + '.png'))

            if saveToDefault:
                if not os.path.exists(f'./result/pic'):
                    os.makedirs(f'./result/pic')
                print(f'Saved to ./result/pic/{self.name}.png')
                plt.savefig(f'./result/pic/{self.name}.png')
            elif saveToPath is not None:
                plt.savefig(os.path.join(saveToPath,f'{self.name}.png'))
                print(f'Saved to {os.path.join(saveToPath,self.name)}.png')
            if disp:
                plt.show()

    def print(self):
        '''
        print metrics calculated
        :return:
        '''
        print(self.metrics_df.to_string())


class SinglePairedTradeRecordPerformance(BasePerformance):
    '''
    根据已经配对的交易记录进行分析
    Input Data: paired trade record from single portfolio
                Must contains buy date, sell date, buy price, sell price, symbol, amount
    Output: some metrics and plots.
    '''
    def __init__(self,name = None):
        super(SinglePairedTradeRecordPerformance,self).__init__(name=name)
        # 初始化attr
        self.pairedTradeRecord = pd.DataFrame(columns=['record_num',
                                                       'symbol',
                                                       'volume',
                                                       'bDay',
                                                       'sDay',
                                                       'bPrice',
                                                       'sPrice',
                                                       'PnL',
                                                       'PnL(%)',
                                                       'commission',
                                                       ])
        self.metrics = OrderedDict((
            ('Total PnL',None),
            ('Total Cases',None),
            ('Total PnL from Win', None),
            ('Total PnL from Lose', None),
            ('Win Cases',None),
            ('Win Rate',None),
            ('Lose Cases',None),
            ('Lose Rate',None),
            ('Max Win(amount)',None),
            ('Max Win(rate)',None),
            ('Max Lose(amount)',None),
            ('Max Lose(rate)',None),
            ('Avg PnL(amount)',None),
            ('Avg PnL(rate)',None),
            ('Avg Win(amount)',None),
            ('Avg Win(rate)',None),
            ('Avg Lose(amount)',None),
            ('Avg Lose(rate)',None),
            ('HHI for Win(rate)',None),
            ('HHI for Win(time)',None),
            ('HHI for Lose(rate)', None),
            ('HHI for Lose(time)', None),
        ))

    def loadData(self,
                 data,
                 commission_col=None,commission=0,
                 pnl_col=None,
                 record_num_col = -1,
                 symbol_col = 0,
                 volume_col = 1,
                 bDay_col = 2,
                 sDay_col = 3,
                 bPrice_col = 4,
                 sPrice_col = 5,
                 ):
        '''
        paired trade record must contains:
            symbol & volume & buy/sell price & buy/sell time
        if commission_col is given, the commission will be directly used
        if not, a rate of <commission> is calculated for both directions
        if pnl_col is given, the pnl will be directly used
        if not, volume*(sell-buy) - commission will be used to calculate the pnl
        :param data:
        :param commission_col:
        :param commission:
        :param pnl_col:
        :param record_num_col:
        :param symbol_col:
        :param volume_col:
        :param bDay_col:
        :param sDay_col:
        :param bPrice_col:
        :param sPrice_col:
        :return:
        '''
        self.pairedTradeRecord.loc[:,'symbol'] = data.iloc[:,symbol_col]
        self.pairedTradeRecord.loc[:,'volume'] = data.iloc[:,volume_col]
        self.pairedTradeRecord.loc[:,'bDay'] = data.iloc[:,bDay_col]
        self.pairedTradeRecord.loc[:,'sDay'] = data.iloc[:,sDay_col]
        self.pairedTradeRecord.loc[:,'bPrice'] = data.iloc[:,bPrice_col]
        self.pairedTradeRecord.loc[:,'sPrice'] = data.iloc[:,sPrice_col]

        if commission_col is not None: # 计算commission
            self.pairedTradeRecord.loc[:,'commission'] = data.iloc[:,commission_col]
        else:
            self.pairedTradeRecord.loc[:,'commission'] = (data.iloc[:,bPrice_col] + data.iloc[:,sPrice_col]) * data.iloc[:,volume_col] * commission

        if pnl_col is not None: # 计算pnl
            self.pairedTradeRecord.loc[:,'PnL'] = data.iloc[:,pnl_col]
        else:
            self.pairedTradeRecord.loc[:,'PnL'] = (data.iloc[:,sPrice_col] - data.iloc[:,bPrice_col]) * data.iloc[:,volume_col] - commission

        # 计算pnl(%)
        self.pairedTradeRecord.loc[:,'PnL(%)'] = self.pairedTradeRecord.loc[:,'PnL']/self.pairedTradeRecord.loc[:,'volume']/self.pairedTradeRecord.loc[:,'bPrice']
        # ***WARNINGS***
        # If a trade is like "Sell short -- Cover", then the pnl(%) is not calculated correctly.
        # For that kind of trade, should divide sPrice instead of bPrice

        if record_num_col == -1:
            self.pairedTradeRecord.loc[:,'record_num'] = data.index
        else:
            self.pairedTradeRecord.loc[:,'record_num'] = data.iloc[:,record_num_col]

    def calculate(self):
        # 计算metrics
        # copy value
        recordInUse = self.pairedTradeRecord.copy(deep=True)

        # truncate
        # 截取部分时间段内的数据
        if self.start_date is not None:
            recordInUse = recordInUse.loc[recordInUse.loc[:,'bDay']>=self.start_date]
            recordInUse = recordInUse.loc[recordInUse.loc[:,'sDay']>=self.start_date]
        if self.end_date is not None:
            recordInUse = recordInUse.loc[recordInUse.loc[:,'bDay']<=self.end_date]
            recordInUse = recordInUse.loc[recordInUse.loc[:,'sDay']<=self.end_date]

        # calculation
        n = len(recordInUse)
        if n == 0:
            print("*** NO RECORD! ***")
            return

        self.metrics['Total Cases'] = n
        self.metrics['Total PnL'] = np.nansum(recordInUse.loc[:,'PnL']) # 总的PnL

        winRecord = recordInUse.loc[recordInUse.loc[:,'PnL']>0] # 获胜记录
        loseRecord = recordInUse.loc[recordInUse.loc[:,'PnL']<0] # 失败记录

        self.metrics['Win Cases'] = len(winRecord) # 胜场
        self.metrics['Win Rate'] = self.metrics['Win Cases'] / self.metrics['Total Cases'] # 胜率
        self.metrics['Lose Cases'] = len(loseRecord) # 负场
        self.metrics['Lose Rate'] = self.metrics['Lose Cases'] / self.metrics['Total Cases'] # 负率

        self.metrics['Total PnL from Win'] = np.nansum(winRecord.loc[:,'PnL']) # 获胜总收益
        self.metrics['Total PnL from Lose'] = np.nansum(loseRecord.loc[:,'PnL']) # 失败总损失

        self.metrics['Max Win(amount)'] = np.nanmax(winRecord.loc[:,'PnL']) # 最大单笔回报
        self.metrics['Max Win(rate)'] = np.nanmax(winRecord.loc[:,'PnL(%)']) # 最大单笔回报率

        self.metrics['Max Lose(amount)'] = np.nanmin(loseRecord.loc[:,'PnL']) # 最大单笔损失
        self.metrics['Max Lose(rate)'] = np.nanmin(loseRecord.loc[:,'PnL(%)']) # 最大单笔损失率

        self.metrics['Avg PnL(amount)'] = np.nanmean(recordInUse.loc[:,'PnL']) # 平均每笔PnL
        self.metrics['Avg PnL(rate)'] = np.nanmean(recordInUse.loc[:,'PnL(%)']) # 平均每笔收益率

        self.metrics['Avg Win(amount)'] = np.nanmean(winRecord.loc[:,'PnL']) # 获胜平均每笔回报
        self.metrics['Avg Win(rate)'] = np.nanmean(winRecord.loc[:,'PnL(%)'])  # 获胜平均每笔回报率

        self.metrics['Avg Lose(amount)'] = np.nanmean(loseRecord.loc[:,'PnL']) # 失败平均每笔损失
        self.metrics['Avg Lose(rate)'] = np.nanmean(loseRecord.loc[:,'PnL(%)']) # 失败平均每笔损失率

        # calculate HHI
        self.metrics['HHI for Win(rate)'] = getHHI(winRecord.loc[:,'PnL'].values) # Herfindahl-Hirschman Index
        self.metrics['HHI for Lose(rate)'] = getHHI(loseRecord.loc[:,'PnL'].values) # Herfindahl-Hirschman Index

        # generate bins for histogram plot
        steps = 0.01
        n_positive_bins = int(np.ceil(self.metrics['Max Win(rate)']/steps))
        n_negative_bins = int(np.ceil((-self.metrics['Max Lose(rate)'])/steps))

        self.hist_win = np.histogram(winRecord.loc[:,'PnL(%)'],bins=20)
        self.hist_lose = np.histogram(loseRecord.loc[:,'PnL(%)'],bins=20)

        all_bins = np.arange(-min(n_negative_bins,20),
                             min(n_positive_bins,20)+1) * steps
        self.hist_all = np.histogram(recordInUse.loc[:,'PnL(%)'],bins=all_bins)

        # 将index的日期转换为datetime的日期, 以便用于作图
        temp = recordInUse.loc[:,['bDay','PnL(%)']]
        temp.loc[:,'bDate'] = temp.loc[:,'bDay'].apply(lambda x: self.ix2Calendar[x])
        temp.drop('bDay',axis=1,inplace =True)
        temp.set_index('bDate',drop=True,inplace=True)
        temp.index = pd.to_datetime(temp.index)
        self.PnL_resampled_num = temp.resample("M").apply(lambda x: len(x)) # resample到每月的trade数量
        self.PnL_resampled_num.columns = ['Num']
        self.recordInUse = recordInUse
        self.metrics2df()

    def plot(self,disp=False, saveToDefault=False,saveToPath=None):

        fig,axs = plt.subplots(2,2)
        plt.suptitle(f'{self.name}')
        # axs[0,0] total pnl hist
        hist = self.hist_all[0]
        bins = self.hist_all[1]
        width = np.diff(bins)
        center = (bins[1:] + bins[:-1])/2
        axs[0,0].bar(center,hist,align='center',width=width)
        axs[0,0].set_xticks([-0.2,-0.15,-0.1,-0.05,0,0.05,0.1,0.15,0.2])
        for tick in axs[0,0].get_xticklabels():
            tick.set_rotation(45)
        # axs[0,1] bDay plot
        axs[0,1].xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        axs[0,1].xaxis.set_major_locator(mdates.YearLocator())
        axs[0,1].plot(self.PnL_resampled_num.index, self.PnL_resampled_num.iloc[:, 0], color='red')
        for tick in axs[0,1].get_xticklabels():
            tick.set_rotation(45)

        # axs[1,0] lose pnl hist
        hist = self.hist_lose[0]
        bins = self.hist_lose[1]
        width = np.diff(bins)
        center = (bins[1:] + bins[:-1]) / 2
        axs[1, 0].bar(center, hist, align='center', width=width)
        # axs[1,1] win pnl hist
        hist = self.hist_win[0]
        bins = self.hist_win[1]
        width = np.diff(bins)
        center = (bins[1:] + bins[:-1]) / 2
        axs[1, 1].bar(center, hist, align='center', width=width)

        if saveToDefault:
            if not os.path.exists(f'./result/pic'):
                os.makedirs(f'./result/pic')
            print(f'Saved to ./result/pic/1-{self.name}.png')
            plt.savefig(f'./result/pic/1-{self.name}.png')
        elif saveToPath is not None:
            plt.savefig(os.path.join(saveToPath, f'1-{self.name}.png'))
            print(f'Saved to {os.path.join(saveToPath,"1-"+self.name)}.png')
        if disp:
            plt.show()

        fig, ax1 = plt.subplots(1,1)
        plt.suptitle(f'{self.name}')
        x = self.recordInUse.loc[:,'bDay'].apply(lambda x:self.ix2Calendar[x])
        y = self.recordInUse.loc[:,'PnL(%)']
        holdingdays = abs(self.recordInUse.loc[:,'bDay']-self.recordInUse.loc[:,'sDay'])
        abnormalix = np.where(holdingdays > holdingdays.mode().values[0])[0]
        ax1.scatter(x[abnormalix],y[abnormalix],s = 10,alpha=0.4,c='blue',label='greater than mode')
        abnormalix = np.where(holdingdays < holdingdays.mode().values[0])[0]
        ax1.scatter(x[abnormalix],y[abnormalix],s = 10,alpha=0.4,c='orange',label='less than mode')
        abnormalix = np.where(holdingdays == holdingdays.mode().values[0])[0]
        ax1.scatter(x[abnormalix],y[abnormalix],s = 1,alpha=0.4,c='red',label='equal to mode')
        plt.legend()

        if saveToDefault:
            if not os.path.exists(f'./result/pic'):
                os.makedirs(f'./result/pic')
            print(f'Saved to ./result/pic/2-{self.name}.png')
            plt.savefig(f'./result/pic/2-{self.name}.png')
        elif saveToPath is not None:
            plt.savefig(os.path.join(saveToPath, f'2-{self.name}.png'))
            print(f'Saved to {os.path.join(saveToPath,"2-"+self.name)}.png')
        if disp:
            plt.show()


class HFPortfolioPerformance(SinglePortfolioPerformance):
    def __init__(self,name = None, frequency = 1):
        super(HFPortfolioPerformance,self).__init__(name=name)
        self.frequency = frequency

    def loadHFParameters(self,frequency=1):
        self.frequency = frequency

    def applyHighFrequency(self):
        """
            如果频率不为1, 也就是说一天不止1个交易点, 需要对一些年化指标进行重新计算.
        :return:
        """
        if self.frequency ==1:
            return # 不用更改

        n = len(self.holdingInUse) # 交易天数

        self.metrics['annualizedReturn'] = np.power(self.metrics['totalReturn'] + 1., 252. * self.frequency / n) - 1
        self.metrics['annualizedVolatility'] = self.metrics['annualizedVolatility'] * np.sqrt(self.frequency)
        self.metrics['sharpeRatio(Annualized)'] = self.metrics['annualizedReturn']/(self.metrics['annualizedVolatility']+1e-6)

        if self.holdingInUsewithBenchmark is not None:
            self.metrics['annualizedExcessRtn'] = np.power(self.metrics['totalExcessRtn'] + 1., 252. * self.frequency / n) - 1

            self.metrics['annualizedTrackingError'] = self.metrics['annualizedTrackingError'] * np.sqrt(self.frequency)

            self.metrics['informationRatio(Annualized)'] = self.metrics['annualizedExcessRtn']/(self.metrics['annualizedTrackingError'] + 1e-6)


        self.metrics2df()






class PerformanceAnalyser(object):
    '''
    分析某一个文件夹下符合特定名称的文件(每个文件都是单个的策略表现, etc.)来比较不同策略的表现
    '''
    def __init__(self,loadDefaultSetting = True):
        self.trading_day = None  # list: store the trading days in pandas timestamp format
        self.trading_day_ = None  # list: store trading days in str format (%Y-%m-%d)
        self.allSymbols = None  # list: store all trading symbols
        self.benchmark_date = None  # list: store the dates of available benchmark in pandas timestamp format
        self.benchmark = None  # df: 'close' store the close price of benchmark, 'rtn' store the daily return of benchmark
        self.aggregatedMetrics = None  # df: metrics for portfolios
        if loadDefaultSetting: # 加载默认的一些配置
            self._load_calendar()
            self._load_symbols()
            self._load_benchmark()

    def _load_calendar(self):
        '''
        Default method of loading trading days.
        Will update trading_day and trading_day_.
        The default calendar contains the trading days of China A share from 2013/7/1 to 2019/11/12.
        :return:
        '''
        print("-----LOADING DEFAULT CALENDAR-----")
        trading_day = pd.read_csv('./data/calendar/A_share_calendar.csv',index_col=0,parse_dates=True)
        self.trading_day = pd.to_datetime(trading_day.loc[:,'Time']).tolist()
        self.trading_day_ = [i.strftime("%Y-%m-%d") for i in self.trading_day]
        print(f"From {self.trading_day_[0]} To {self.trading_day_[-1]}")
        print()

    def _load_symbols(self):
        '''
        Default method of loading all trading symbols
        Will update allSymbols
        The default symbols contains China A share stocks listed before 2018. Total number is 3109.
        :return:
        '''
        print("-----LOADING DEFAULT SYMBOLS-----")
        allSymbols = pd.read_csv('./data/symbol/A_share_symbol_list.csv',index_col=0)
        self.allSymbols = allSymbols.loc[:,'Code'].tolist()
        print(f'Total Num of Symbols: {len(self.allSymbols)}')
        print()

    def _load_benchmark(self):
        '''
        Default method of loading benchmark performance
        Will update benchmark_date, benchmark
        The default benchmark is 000905.SH from 2014/1/6 to 2019/11/22
        :return:
        '''
        print("-----LOADING DEFAULT BENCHMARK-----")
        benchmark = pd.read_csv('./data/benchmark/000905.SH.csv',index_col=0,parse_dates=True)
        benchmark.loc[:,'benchmarkLogRtn'] = np.log(benchmark.loc[:,'close']).diff()
        benchmark.loc[:,'benchmarkSimpleRtn'] = benchmark.loc[:,'close'].pct_change()
        benchmark.columns = ['benchmarkClose','benchmarkVolume','benchmarkLogRtn','benchmarkSimpleRtn']
        benchmark = benchmark.dropna() # drop the first row which contains nan
        self.benchmark_date = benchmark.index.tolist()
        self.benchmark = benchmark
        print(f"Default Benchmark: 000905.SH  \nFrom {self.benchmark_date[0].strftime('%Y-%m-%d')} To {self.benchmark_date[-1].strftime('%Y-%m-%d')}")
        print()

    def analysePortfolioPerformanceInFolder(self,dirPath, disp = False, startswith = "",within="", exts='.csv',start_date = None, end_data = None):
        '''
        分析一个文件夹下不同策略的表现, 分别作图, 并汇总metrics
        :param dirPath: 文件夹根目录
        :param disp: 是否显示
        :param startswith: 文件名起始
        :param within: 文件名内含
        :param exts: 文件扩展名
        :param start_date: 分析的开始日期
        :param end_data: 分析的结束日期
        :return:
        '''
        self._dirWalker(dirPath,startswith,within,exts)

        metrics_list = []
        for file in self.file_list:
            file_name,file_exts = os.path.splitext(file)
            df = pd.read_csv(os.path.join(dirPath,file),index_col=0,parse_dates=True)
            p = SinglePortfolioPerformance(file_name)
            p.loadData(df)
            p.set_obs_window(start_date,end_data)
            p.loadBenchmark(self.benchmark)
            p.calculate()
            p.plot(disp=disp, saveToDefault=True)

            metrics = p.metrics_df.copy(deep=True)
            metrics.index = [file_name]
            metrics_list.append(metrics)

        self.aggregatedMetrics = pd.concat(metrics_list,axis=0)

    def analysePairedTradeRecordInFolder(self,dirPath, disp = False, startswith = "",within="",exts=".csv", start_date = None, end_data = None):
        '''
        分析一个文件夹下不同策略的表现, 分别作图, 并汇总metrics
        :param dirPath: 文件夹根目录
        :param disp: 是否显示
        :param startswith: 文件名起始
        :param within: 文件名内含
        :param exts: 文件扩展名
        :param start_date: 分析的开始日期
        :param end_data: 分析的结束日期
        :return:
        '''
        self._dirWalker(dirPath,startswith,within,exts)

        metrics_list = []
        for file in self.file_list:
            file_name,file_exts = os.path.splitext(file)
            df = pd.read_csv(os.path.join(dirPath,file),index_col=0,parse_dates=True)
            p = SinglePairedTradeRecordPerformance(file_name)
            p.loadCalendar(self.trading_day, offset=-55)
            p.loadData(df)
            p.set_obs_window(start_date,end_data)
            p.calculate()
            p.plot(disp=disp, saveToDefault=True)

            metrics = p.metrics_df.copy(deep=True)
            metrics.index = [file_name]
            metrics_list.append(metrics)

        self.aggregatedMetrics = pd.concat(metrics_list,axis=0)

    def _dirWalker(self, dirPath, startswith:str="", within:str="", exts:str = ".csv"):
        '''
        给定文件夹根目录和文件起始,内含,扩展名等信息, 返回符合条件的文件名
        :param dirPath:
        :param startswith:
        :param within:
        :param exts:
        :return:
        '''
        self.file_list = []

        files = os.listdir(dirPath)
        for file in files:
            if file.startswith(startswith) and within in file:
                file_name,file_exts = os.path.splitext(file)
                if file_exts == exts:
                    self.file_list.append(file)
