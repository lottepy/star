import pandas as pd
import os
from algorithm import addpath
import numpy as np
import datetime
from choice_client import c

symbol_str="010107.SH,010303.SH,010504.SH,010609.SH,010619.SH,010706.SH,010713.SH,019003.SH,019009.SH,019014.SH,019018.SH,019023.SH,019026.SH,019029.SH,019037.SH,019040.SH,019041.SH,019102.SH,019105.SH,019108.SH,019110.SH,019112.SH,019115.SH,019116.SH,019119.SH,019123.SH,019124.SH,019204.SH,019206.SH,019208.SH,019209.SH,019212.SH,019213.SH,019215.SH,019218.SH,019220.SH,019221.SH,019305.SH,019309.SH,019310.SH,019311.SH,019316.SH,019318.SH,019319.SH,019324.SH,019325.SH,019403.SH,019405.SH,019406.SH,019409.SH,019410.SH,019412.SH,019413.SH,019416.SH,019417.SH,019421.SH,019424.SH,019425.SH,019427.SH,019429.SH,019502.SH,019505.SH,019507.SH,019508.SH,019510.SH,019514.SH,019516.SH,019517.SH,019521.SH,019523.SH,019525.SH,019526.SH,019528.SH,019530.SH,019532.SH,019534.SH,019535.SH,019536.SH,019538.SH,019541.SH,019542.SH,019543.SH,019545.SH,019547.SH,019548.SH,019549.SH,019551.SH,019553.SH,019554.SH,019555.SH,019558.SH,019559.SH,019560.SH,019561.SH,019564.SH,019565.SH,019567.SH,019568.SH,019569.SH,019572.SH,019574.SH,019575.SH,019576.SH,019577.SH,019580.SH,019581.SH,019582.SH,019583.SH,019584.SH,019586.SH,019587.SH,019588.SH,019589.SH,019591.SH,019593.SH,019594.SH,019595.SH,019596.SH,019598.SH,019599.SH,019601.SH,019602.SH,019603.SH,019605.SH,019606.SH,019607.SH,019609.SH,019610.SH,019612.SH,019613.SH,019614.SH,019616.SH,019617.SH,019618.SH,019619.SH,019620.SH,019621.SH,019623.SH,019624.SH,019625.SH,019626.SH,019627.SH,019628.SH,019629.SH,019630.SH,019631.SH,019632.SH,019633.SH,019634.SH,019635.SH,019636.SH,019637.SH,019638.SH,019639.SH,019640.SH,019641.SH,019642.SH,019643.SH,019644.SH,019645.SH,019646.SH,019647.SH,019648.SH,019802.SH,019806.SH,019813.SH,019820.SH,019823.SH,019902.SH,019905.SH,019911.SH,019920.SH,019925.SH,019930.SH,020366.SH,020371.SH,020373.SH,020376.SH,020378.SH,020379.SH,020380.SH,020381.SH,020382.SH,020383.SH,020384.SH,020385.SH,020386.SH,020387.SH,020388.SH,020389.SH,020390.SH,020391.SH,020392.SH,020393.SH,020394.SH,018003.SH,018006.SH,018008.SH,018009.SH,018011.SH,018012.SH,018013.SH,018014.SH,018010.SH,018015.SH,018017.SH,018062.SH,018082.SH,018083.SH,018084.SH,100303.SZ,100504.SZ,100609.SZ,100619.SZ,100706.SZ,100713.SZ,100802.SZ,100806.SZ,100813.SZ,100820.SZ,100823.SZ,100902.SZ,100905.SZ,100911.SZ,100920.SZ,100925.SZ,100930.SZ,101003.SZ,101009.SZ,101014.SZ,101018.SZ,101023.SZ,101026.SZ,101029.SZ,101037.SZ,101040.SZ,101041.SZ,101102.SZ,101105.SZ,101108.SZ,101110.SZ,101112.SZ,101115.SZ,101116.SZ,101119.SZ,101123.SZ,101124.SZ,101204.SZ,101206.SZ,101208.SZ,101209.SZ,101212.SZ,101213.SZ,101215.SZ,101218.SZ,101220.SZ,101221.SZ,101305.SZ,101309.SZ,101310.SZ,101311.SZ,101316.SZ,101318.SZ,101319.SZ,101324.SZ,101325.SZ,101403.SZ,101405.SZ,101406.SZ,101409.SZ,101410.SZ,101412.SZ,101413.SZ,101416.SZ,101417.SZ,101421.SZ,101424.SZ,101425.SZ,101427.SZ,101429.SZ,101502.SZ,101505.SZ,101507.SZ,101508.SZ,101510.SZ,101514.SZ,101516.SZ,101517.SZ,101521.SZ,101523.SZ,101525.SZ,101526.SZ,101528.SZ,101602.SZ,101604.SZ,101606.SZ,101607.SZ,101608.SZ,101610.SZ,101613.SZ,101614.SZ,101615.SZ,101617.SZ,101619.SZ,101620.SZ,101621.SZ,101623.SZ,101625.SZ,101626.SZ,101701.SZ,101704.SZ,101705.SZ,101706.SZ,101707.SZ,101710.SZ,101711.SZ,101713.SZ,101714.SZ,101715.SZ,101718.SZ,101720.SZ,101721.SZ,101722.SZ,101725.SZ,101726.SZ,101727.SZ,101799.SZ,101801.SZ,101802.SZ,101804.SZ,101805.SZ,101806.SZ,101807.SZ,101809.SZ,101811.SZ,101812.SZ,101813.SZ,101814.SZ,101816.SZ,101817.SZ,101819.SZ,101820.SZ,101821.SZ,101823.SZ,101824.SZ,101825.SZ,101827.SZ,101828.SZ,101902.SZ,101906.SZ,101907.SZ,101908.SZ,101909.SZ,101911.SZ,101914.SZ,101915.SZ,101916.SZ,101917.SZ,101983.SZ,101984.SZ,101986.SZ,101988.SZ,102001.SZ,102002.SZ,102003.SZ,102004.SZ,102005.SZ,102006.SZ,102007.SZ,102008.SZ,102009.SZ,102010.SZ,102011.SZ,102012.SZ,102013.SZ,102014.SZ,102015.SZ,102016.SZ,102017.SZ,102018.SZ,102061.SZ,102062.SZ,102063.SZ,102064.SZ,108365.SZ,108370.SZ,108372.SZ,108375.SZ,108378.SZ,108379.SZ,108380.SZ,108381.SZ,108382.SZ,108383.SZ,108384.SZ,108385.SZ,108386.SZ,108387.SZ,108388.SZ,108389.SZ,108390.SZ,108391.SZ,108392.SZ,108393.SZ,108394.SZ,108604.SZ,108605.SZ,108606.SZ,108610.SZ,108611.SZ,108612.SZ,108802.SZ"
symbol_list=symbol_str.split(",")

# data_all=[]
# symbol_nadata=[]
# for symbol in symbol_list:
#     try:
#         data=c.css(symbol,"CODE,BONDCODEMARKET,FNAME,PAR,TERM,ISSUEAMT,INTSDATE,MATURITYDATE,INTTYPE,PMTTYPE,INTFREQUENCY,CREDITRATING,CREDITRATINGAGENCY,LISTDATE,LISTMKT,RATEDES,COUPONRULE,PAYEXPLAIN,PMTDAYS,YTM,DURATION,MODIFIEDDURATION,CONVEXITY,YTC,YTP","EndDate=2020-12-15")[symbol]
#         print(symbol,'have date')
#         data.index=[symbol]
#     except:
#         symbol_nadata.append(symbol)
#         print(symbol,'have no date')
#
#     data_all.append(data)
# data_all=pd.concat(data_all,axis=0)
# data_all.to_csv(os.path.join(addpath.data_path,"bond",'CS_data.csv'),encoding="utf_8_sig")
# symbol_nadata=pd.DataFrame(symbol_nadata)
# symbol_nadata.to_csv(os.path.join(addpath.data_path,"bond",'symbol_noCS_data.csv'))

trading_data_path=os.path.join(addpath.data_path,'bond','trading_6m')
if os.path.exists(trading_data_path):
    pass
else:
    os.makedirs(trading_data_path)

symbol_nadata=[]
for symbol in symbol_list:
    try:
        data = c.csd(symbol, "CLOSE,VOLUME,AMOUNT", "2020-06-15", "2020-12-15",
                 "type=2,period=1,adjustflag=1,curtype=1,order=1")[symbol]
        print(data)
        data.to_csv(os.path.join(trading_data_path,symbol+'.csv'))
        print(symbol,'data has been written to path.')
    except:
        symbol_nadata.append(symbol)
        print(symbol,'have no date')



