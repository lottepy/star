# PerformanceAnalysis



## Object: Performance

每一个Performance对象的子类可以处理某一种特定的策略表现文件. 
例如SinglePortfolioPerformance可以以单一策略的持仓价值, 组合价值, 
现金价值的时间序列作为输入, 计算例如Sharpe Ratio等指标, 并且画出持仓比例, 
超额收益等图

结构:

```python

class SomePerformance(BasePerformance):

    def __init__

    def calculate # 计算metrics并保存

    def plot # 画图

    def print # 输出
```

### SinglePortfolioPerformance

以单一策略的持仓,组合,现金价值和基准的收益作为输入.

输出指标包括: 总收益, 年化收益, 日波动率, (日)夏普比率, (年化)夏普比率,
最大回撤, 最大单日跌幅, VaR, Calmar比率, 等.

若基准的收益有输入, 还会计算机会成本, 超额收益, Tracking Error, 信息比率, 等.

作图包括: 全时段(分时段)组合回报率, 全时段(分时段)超额收益(若有), 全时段(分时段)
持仓比例

### SinglePairedTradeRecordPerformance

以单一策略的配对交易作为输入.

一条配对交易记录的定义为有购买日,购买价,卖出日,卖出价,标的代码,交易量,佣金(可选),
PnL(可选)的记录.

输出指标包括: 总交易次数, 胜率, 最大(平均)获胜收益, 最大(平均)失败损失, 交易分散度, 等.

作图包括: 全时段(分时段)的从-0.2到0.2的PnL分布直方图, 全时段(分时段)的获胜收益分布直方图,
全时段(分时段)的失败损失分布直方图, 按月统计的交易次数分布图.

以及交易概览图(x轴为买入时间,y轴为该笔交易PnL,按照持仓时长分类)

## Object: Analyser

包装Performance类, 来实现分析某一个文件夹下特定类型文件的分析.(目前没有使用正则表达式)

结构:

```python
class Analyser():
    def _dirWalker # walk through directory
    def analyseFolderUsingPerformanceA
    def analyseFolderUsingPerformanceB
```

## Examples

参考main.py