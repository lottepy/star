import pytest
import os, json, random, time
from inputs.sampleBase import *
from subtasks import AqumonSubTask
from collections.abc import Iterable

if not os.path.exists('testcase/'):
    os.mkdir('testcase/')

class AqumonTestCase(AqumonSample):
    region = RegionEnum.HK
    currency = CurrencyEnum.HKD

POSSIBLE_TASK_SUBTYPES = ['BUY', 'PARTIALSELL', 'REBALANCE']
POSSIBLE_LOT_SIZE = []
N_TESTS = 10000

class GeneratedTestCase():
    def __init__(self):
        self.contents = []
    def add(self, item):
        self.contents.append(item)
    def serialize(self):
        string = ''
        for c in self.contents:
            if (len(string)) != 0:
                string += ";"
            if type(c) in [list, tuple, np.ndarray]:
                string += ",".join([str(x) for x in c])
            else:
                string += json.dumps(c).strip('"')
        return string

def test_BUY():
    f = open('testcase/aqumon.csv', 'w+')
    for i in range(N_TESTS):
        n_symbols = np.random.randint(1,8)
        data = AqumonTestCase
        data.symbol = np.asarray(['randomSample-' + str(i) for i in range(n_symbols)])
        data.targetWeight = np.random.dirichlet(np.ones(n_symbols),size=1).reshape(n_symbols)
        data.brokerType = random.choice([BrokerTypeEnum.AYERS, BrokerTypeEnum.IB])
        taskSubType = 'BUY'
        data.deltaAmount = np.random.rand() * 100000
        data.availableCashAmount = np.float('0')
        data.mvTradingHalt = np.float('0')
        data.tradingHaltDrift = np.float('0')
        data.askPrice = np.random.rand(n_symbols) * 100 + 20
        data.currentPosition = np.random.randint(1,101,n_symbols,dtype=int)
        data.filledQuantity = np.minimum(np.random.randint(-20,90,n_symbols), data.currentPosition, dtype=int)
        
        # 前半部分的正态分布是模拟成交价格与当前价格的波动。成交价格有 95.4% 的概率落在当前价格的 [99%, 101%] 的区间内。有 99.7% 的概率落在 [98.5%, 101.5%]。跟实际情况相似。
        # 但是如果底层是 ETF 的话这个波动应该足够可以测出来一些极端情况了。因为 ETF 的波动很少能到这么大，即使我们给一个 subtask 拖了很久才下完。
        # 因为如果这个 ETF 的价格是持续变化的话，那 average price 是会一直跟着实时价格走的，只不过有一个 lag，不能完全追上，但是应该也不会差太太多。
        # 如果底层是股票的话，这个波动可能偏小，因为股票一分钟波动 2~3% 实在是太常见了。因此，如果这个要测股票的底层的话，那 sigma 可能要改成 1% ~ 1.5% 这样可能会比较合适。现在 sigma 是 0.5%。
        data.filledAvgPrice = ( np.random.randn() * 0.005 + 1 ) * data.askPrice
        data.filledAmount = data.filledQuantity * data.filledAvgPrice
        data.longVolumeHistory = data.currentPosition - data.filledQuantity
        assert np.all(data.longVolumeHistory >= 0)
        data.shortVolumeToday = np.abs(np.minimum(data.filledQuantity, 0))
        amountForOneLot = np.random.rand(n_symbols) * 3000 + 3000
        data.lotSize = (amountForOneLot // data.askPrice).astype(int)
        data.oddLotTrade = False

        subTask = AqumonSubTask(data)
        deltaPosition = subTask.calc()
        
        testCase = GeneratedTestCase()
        testCase.add(data.symbol)
        testCase.add(data.targetWeight)
        testCase.add(data.currency)
        testCase.add(data.brokerType)
        testCase.add("BUY")
        testCase.add(data.deltaAmount)
        testCase.add(data.availableCashAmount)
        testCase.add(data.mvTradingHalt)
        testCase.add(data.tradingHaltDrift)
        testCase.add(data.currentPosition)
        testCase.add(data.filledQuantity)
        testCase.add(data.filledAvgPrice)
        testCase.add(data.filledAmount)
        testCase.add(data.longVolumeHistory)
        testCase.add(data.shortVolumeToday)
        testCase.add(data.lotSize)
        testCase.add(data.askPrice)
        testCase.add(deltaPosition)

        f.write(testCase.serialize())
        if i != N_TESTS - 1:
            f.write("\n")

    f.close()