from subtasks import *
from inputs.sample import *

# data = parseTestCaseString("randomSample-0,randomSample-1;0.9884193598720119,0.011580640127988179;HKD;IB;BUY;61214.56012798881;0.0;0.0;0.0;89,59;75,2;89.5173829521945,101.87766945308098;6713.803721414587,203.75533890616197;14,57;0,0;58,39;89.39057454626385,101.73335173022961;665,0")
data = SampleAqumon10
SubTask = AqumonSubTask

subTask = SubTask(data)
deltaPosition = subTask.calc()
print(deltaPosition)
print(subTask.calculator.calcDrift(SubTaskTypeEnum.AQUMON, BrokerTypeEnum.AYERS, (data.currentPosition + deltaPosition), data.askPrice, data.targetWeight, data.availableCashAmount, 1-sum(data.targetWeight), sum((data.currentPosition + deltaPosition) * data.askPrice) + data.availableCashAmount, 0))

# from core.handleLimitUpLimitDownTradingHalt import LimitUpLimitDownTradingHaltHandler
# import numpy as np

# # h = LimitUpLimitDownTradingHaltHandler(
# #         backupRecords={},
# #         backupSymbols=np.array(["1105002238","1105000772","1105001810","1105000268","1105002382","1105001177"]),
# #         symbols=np.array(["1105000777","1105009999","1105000981","1105000285","1105002331","1105009618","1105002238","1105000772","1105001810","1105000268","1105002382","1105001177"]),
# #         targetWeight=np.array([0.1245,0.163836,0.16375,0.14725,0.15775,0.242914,0,0,0,0,0,0]),
# #         currentPosition=np.array([1500,200,1500,500,500,150,0,0,0,0,0,0]),
# #         askPrice=np.array([17.4,148.8,21.6,41.6,53.35,343.6,8.68,59.85,35.25,30.4,177.1,7.71]),
# #         calcTargetPV=lambda : 200305,
# #         limitUps=np.array([0,0,0,0,0,0,0,0,0,0,0,0]),
# #         limitDowns=np.array([0,0,0,0,0,0,0,0,0,0,0,0]),
# #         tradingHalts=np.array([1,1,0,0,0,0,0,1,0,0,0,0])
# #     )
# h = LimitUpLimitDownTradingHaltHandler(
#         backupRecords={"F":"G"},
#         backupSymbols=np.array(["G"]),
#         symbols=np.array(["A","B","C","D","E","F","G"]),
#         targetWeight=np.array([0.1,0,0,0,0.4,0.5,0]),
#         currentPosition=np.array([15,25,60,0,0,0,0]),
#         askPrice=np.array([10,10,10,10,10,10,10]),
#         calcTargetPV=lambda : 1000,
#         limitUps=np.array([0,0,0,0,0,1,0]),
#         limitDowns=np.array([1,1,1,0,0,0,0]),
#         tradingHalts=np.array([0,0,0,0,0,0,0])
#     )

# # h = LimitUpLimitDownTradingHaltHandler(
# #         backupRecords={"F":"G","E":"B"},
# #         backupSymbols=np.array(["B","G","A"]),
# #         symbols=np.array(["A","B","C","D","E","F","G"]),
# #         targetWeight=np.array([0,0,0,0.4,0.1,0.5,0]),
# #         currentPosition=np.array([15,25,60,0,0,0,0]),
# #         askPrice=np.array([10,10,10,10,10,10,10]),
# #         calcTargetPV=lambda : 1000,
# #         limitUps=np.array([0,0,0,1,1,1,1]),
# #         limitDowns=np.array([1,1,1,0,0,0,0]),
# #         tradingHalts=np.array([0,0,0,0,0,0,0])
# #     )

# h.handle()
# print(h.targetWeight)
# print(h.canNotSell)
# print(h.canNotBuy)
# print(h.targetWeight)
# print(h.MVHalted)