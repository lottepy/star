# -*- coding:utf-8 -*-

from datetime import timedelta, datetime

from lib.commonalgo.data.choice_proxy_client import choice_client

#http://quantapi.eastmoney.com/Manual
# data = choice_client.cmc("000001.SZ", "OPEN,CLOSE,HIGH", '2018-09-27', '2018-09-28',"Ispandas=0","Period=5")

# 2.1 序列函数：csd,支持多代码输入
# data = choice_client.csd("000001.SZ,000005.SZ", "open,close", "2018-08-01", "2018-08-16", "RowIndex=1,period=1,adjustflag=1,curtype=1,pricetype=1,year=2016,Ispandas=0")
# data = choice_client.csd("RB1905.SHF,I1905.DCE", "open,close,clear,preclose,preclear", "2019-01-01", "2019-01-30", "RowIndex=1,period=1,adjustflag=0,curtype=1,pricetype=1")

# 2.2 截面函数：css,支持多代码输入
# 不复权--1 ；
# 后复权--2 ；
# 前复权--3 .
# data = choice_client.css("000001.SZ,600425.SH", "open,close")
# data = choice_client.css("000001.SZ,000651.SZ,600595.SH,600594.SH","TRADEMARKET,BLIMPORTINDEXORNOT,SIZE,BLCSRCINDCODE,BLSWSIND,BLSWSINDCODE,ISSTSTOCK,ISXSTSTOCK","BelongIndex=000016,Year=2019,ClassiFication=1,EndDate=2018-01-21")
# data = choice_client.css("00001.HK, 00002.HK","open,close","ReportDate=2019-02-12")
# data = choice_client.css("002258.SZ","DIVPLANANNCDATE,DIVAGMANNCDATE,DIVIMPLANNCDATE,DIVRECORDDATE,"
#                                      "DIVLASTTRDDATESHAREB,DIVEXDATE,DIVBONUSLISTEDDATE,DIVPAYDATE,"
#                                      "DIVPRENOTICEDATE,DIVCASHDATE,DIVNEW","ReportDate=2017-12-31")

# date
# data = choice_client.csqsnapshot("RB1905.SHF,", "PRECLOSE,OPEN,HIGH,LOW")
data = choice_client.css("002258.SZ", "DIVORNOT,DIVPROGRESS,DIVOBJ","ReportDate=2009-06-30,AssignFeature=1,YesNo=1")

print (data.T)
data = choice_client.css("002258.SZ","DIVPRENOTICEDATE,DIVPLANANNCDATE,DIVAGMANNCDATE,DIVIMPLANNCDATE,DIVRECORDDATE,"
                                                "DIVLASTTRDDATESHAREB,DIVEXDATE,DIVBONUSLISTEDDATE,"
                                              "DIVPAYDATE,","ReportDate=2009-6-30,YesNo=1")

print (data.T)
# value
data = choice_client.css("002258.SZ","DIVCASHPSBFTAX,DIVCASHPSAFTAX,DIVSTOCKPS,DIVCAPITALIZATIONPS","ReportDate=2009-6-30,AssignFeature=1,YesNo=1")



print (data.T)



