# -*- coding:utf-8 -*-

from datetime import timedelta, datetime

from lib.commonalgo.data.choice_proxy_client import choice_client

#http://quantapi.eastmoney.com/Manual
# data = choice_client.cmc("000001.SZ", "OPEN,CLOSE,HIGH", '2018-09-27', '2018-09-28',"Ispandas=0","Period=5")

# 2.1 序列函数：csd,支持多代码输入
data = choice_client.csd("000001.SZ,000005.SZ", "open,close", "2018-08-01", "2018-08-16", "RowIndex=1,period=1,adjustflag=1,curtype=1,pricetype=1,year=2016,Ispandas=0")
# data = choice_client.csd("RB1905.SHF,I1905.DCE", "open,close,clear,preclose,preclear", "2019-01-01", "2019-01-30", "RowIndex=1,period=1,adjustflag=0,curtype=1,pricetype=1")

# 2.2 截面函数：css,支持多代码输入
# 不复权--1 ；
# 后复权--2 ；
# 前复权--3 .
# data = choice_client.css("000001.SZ,600425.SH", "open,close")
# data = choice_client.css("000001.SZ,000651.SZ,600595.SH,600594.SH","TRADEMARKET,BLIMPORTINDEXORNOT,SIZE,BLCSRCINDCODE,BLSWSIND,BLSWSINDCODE,ISSTSTOCK,ISXSTSTOCK","BelongIndex=000016,Year=2019,ClassiFication=1,EndDate=2018-01-21")
data = choice_client.css("00001.HK, 00002.HK","open,close","ReportDate=2019-02-12")



# 2.3 历史分钟k线：cmc, 只支持单一代码输入
data = choice_client.cmc("000001.SZ", "OPEN,CLOSE,HIGH", '2018-08-15', '2018-08-21',"Ispandas=0","Period=5")

# 2.5 行情快照函数：csqsnapshot,支持多代码输入
data = choice_client.csqsnapshot("000001.SZ,000005.SZ,", "PRECLOSE,OPEN,HIGH,LOW, ")

# 2.7 专题报表函数：ctr

data = choice_client.ctr("INDEXCOMPOSITION", "", "IndexCode=000001.SH,EndDate=2018-01-13")

# 2.8 条件选股函数：cps

# data = choice_client.cps(cpsCodes ="B_001004",
#                           cpsIndicators =" s1,OPEN,2017/2/27,1; s2,NAME",
#                           cpsConditions ="[ s1]>0",
#                           cpsOptions = " orderby=rd([s1]),top=max([s1],100)")
data = choice_client.cps("B_001004", "s0,OPEN,2017/2/27,1;s1,NAME", "[s0]>0", "orderby=rd([s0])")

# 2.9 板块数据
data = choice_client.sector("702011", "2018-12-26")




# data = choice_client.cps(cpsCodes ="B_001004",
#                           cpsIndicators ="s1,NAME;s2,MV2",
#                           cpsConditions ="",
#                           cpsOptions = "orderby=rd([s2])")
