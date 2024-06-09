import pandas as pd
import os
from algorithm import addpath
import numpy as np
import datetime
from choice_client import c

bond_list_path=os.path.join(addpath.config_path,'2013-2020国债筛选.xlsx')
bond_list=pd.read_excel(bond_list_path,sheet_name='combined')
bond_list=bond_list[bond_list['总入选次数']>=1]['Ticker']
bond_trading_data_path=os.path.join(addpath.data_path,'bond','trading')
if os.path.exists(bond_trading_data_path):
    pass
else:
    os.makedirs(bond_trading_data_path)
for symbol in bond_list:
    print(symbol)
    try:
        data = c.csd(symbol, "CLOSE", "2012-12-31", "2020-12-15","type=2,period=1,adjustflag=2,curtype=1,order=1")[symbol]
        data=data.dropna()
        data.to_csv(os.path.join(bond_trading_data_path,symbol+'.csv'))
    except:
        print(symbol,'fail to search data from choice!')


bond_dividend_data_path=os.path.join(addpath.data_path,'bond','dividend')
if os.path.exists(bond_dividend_data_path):
    pass
else:
    os.makedirs(bond_dividend_data_path)
bond_list_path=os.path.join(addpath.config_path,'2013-2020国债筛选.xlsx')
bond_list=pd.read_excel(bond_list_path,sheet_name='combined')
bond_list_unfreq=[]

year_list=['2013','2014','2015','2016','2017','2018','2019','2020']
for year in year_list:
    bond_inyear=bond_list[bond_list[year+'入选']>=1]['Ticker']
    for symbol in bond_inyear:
        data=c.css(symbol,"COUPONRATECURRENT,PMTTYPE,INTFREQUENCY,INTSDATE,MATURITYDATE,PAR","ENDDATE="+year+'-12-31')[symbol]
        trading_date=pd.read_csv(os.path.join(bond_trading_data_path,symbol+'.csv'),parse_dates=[0])
        trading_date=trading_date.iloc[:,0].dropna()
        trading_years=np.arange(trading_date.min().year+1,trading_date.max().year+1,1)
        payment_date_list=[]
        if (data.loc['2020-12-15','PMTTYPE']=='单利') & (data.loc['2020-12-15','INTFREQUENCY']==1):
            print(symbol)
            for trading_year in trading_years:
                payment_date=datetime.datetime.strptime(str(trading_year)+data.loc['2020-12-15','INTSDATE'][4:],'%Y/%m/%d')
                if payment_date>trading_date.max():
                    pass
                else:
                    payment_date=trading_date[trading_date>=payment_date].tolist()[0]
                    payment_date_list.append(payment_date)
            data_out = pd.DataFrame(index=trading_date.tolist())
            data_out['div'] = 0
            PMT=data.loc['2020-12-15', 'COUPONRATECURRENT']/100*data.loc['2020-12-15', 'PAR']
            data_out.loc[payment_date_list,'div']=PMT
            data_out.to_csv(os.path.join(bond_dividend_data_path,symbol+'.csv'))

        elif(data.loc['2020-12-15','PMTTYPE']=='单利') & (data.loc['2020-12-15','INTFREQUENCY']==2):
            print(symbol)
            for trading_year in trading_years:
                payment_date=datetime.datetime.strptime(str(trading_year)+data.loc['2020-12-15','INTSDATE'][4:],'%Y/%m/%d')
                if payment_date.month>6:
                    payment_date1 = datetime.datetime(payment_date.year,payment_date.month-6,payment_date.day)
                else:
                    payment_date1 = datetime.datetime(payment_date.year,payment_date.month+6,payment_date.day)

                payment_date_max=max(payment_date,payment_date1)
                payment_date_min=min(payment_date,payment_date1)

                if payment_date_min > trading_date.max():
                    pass
                elif payment_date_max>trading_date.max():
                    payment_date_min=trading_date[trading_date>=payment_date_min].tolist()[0]
                    payment_date_list.append(payment_date_min)
                    pass
                else:
                    payment_date=trading_date[trading_date>=payment_date].tolist()[0]
                    payment_date_list.append(payment_date)
                    payment_date1=trading_date[trading_date>=payment_date1].tolist()[0]
                    payment_date_list.append(payment_date1)

            data_out = pd.DataFrame(index=trading_date.tolist())
            data_out['div'] = 0
            PMT=data.loc['2020-12-15', 'COUPONRATECURRENT']/100*data.loc['2020-12-15', 'PAR']
            data_out.loc[payment_date_list,'div']=PMT/2
            data_out.to_csv(os.path.join(bond_dividend_data_path,symbol+'.csv'))
        else:
            print(symbol,'计息方式非单利或计息频率非1和2')

            # print(data.loc['2020-12-14',['PMTTYPE','INTFREQUENCY']])
            # bond_list_unfreq.append(symbol)
            # print(symbol,'计息方式非单利或计息频率非一年')
# bond_list_unfreq=pd.DataFrame(bond_list_unfreq)
# bond_list_unfreq.to_csv(os.path.join(addpath.data_path,'bond','bond_list_unfreq.csv'),index=False)
