from tia.bbg import Terminal
# LocalTerminal = Terminal('192.168.9.109', 18194)
# LocalTerminal = Terminal('47.244.101.245', 18194)
# LocalTerminal = Terminal('bbg.algo-api.aqumon.com', 28194)
LocalTerminal = Terminal('bbg.algo-api.aqumon.com', 8194)
import pandas as pd
import numpy as np
import datetime as dt
import time
from algorithm import addpath

import pandas as pd
import numpy as np
import os
from os.path import join

def download_his(ticker, field_list, start_date, end_date,
                 currency=None, period="DAILY", dpdf=None, normal=True, abnormal=True, split=None, calendar=None, **kwargs):
    raw_df = LocalTerminal.get_historical(
        sids = ticker,
        flds =field_list,
        start=start_date,
        end=end_date,
        period=period, # [DAILY, WEEKLY, MONTHLY, QUARTERLY, SEMI_ANNUALLY, YEARLY]
        # period_adjustment='ACTUAL',# (ACTUAL, CALENDAR, FISCAL)
        currency=currency,
        # override_option=None,pricing_option=None, #(NON_TRADING_WEEKDAYS | ALL_CALENDAR_DAYS | ACTIVE_DAYS_ONLY)
        non_trading_day_fill_method='PREVIOUS_VALUE', #(PREVIOUS_VALUE | NIL_VALUE)
        # max_data_points=None,
        # adjustment_normal=None,adjustment_abnormal=None, adjustment_split=None,
        adjustment_follow_DPDF=dpdf,
        adjustment_split = split,
        adjustment_normal = normal,
        adjustment_abnormal = abnormal,
        calendar_code_override=calendar,
        **kwargs
    )

    df = raw_df.as_frame()
    return df

def download_ref(ticker, field_list, override=None):
    raw_df = LocalTerminal.get_reference_data(ticker,field_list, override)
    df = raw_df.as_frame()
    return df

def generate_bundle(datain, dest, name_list):
    for j in range(datain.shape[1]):
        output = pd.DataFrame([datain.iloc[:,j]]*4).T
        output.columns = ['open', 'high', 'low', 'close']
        output['volume'] = 100
        output.to_csv(os.path.join(dest, name_list[j]))
    return

def generate_fxrate(datain, dest, name_list):
    for j in range(datain.shape[1]):
        output = pd.DataFrame(1, index=datain.index, columns=['fx_rate'])
        output.to_csv(os.path.join(dest, name_list[j]))
    return

def generate_interest(datain, dest, name_list):
    for j in range(datain.shape[1]):
        output = pd.DataFrame(0, index=datain.index, columns=['interest_rate'])
        output.to_csv(os.path.join(dest, name_list[j]))
    return
   
def generate_ref(datain, dest, name_list, ref_name):
    for j in range(datain.shape[1]):
        output = pd.DataFrame(datain.iloc[:,j], index=datain.index)
        output.columns = [ref_name]
        output.to_csv(os.path.join(dest, name_list[j]))
    return


def generate_data(start, end, mode):
    
    match_dates = pd.read_csv(join(addpath.data_path, 'bundles', 'Smart_Rotation.csv'), index_col=[0], parse_dates=[0])
    match_dates['meaningless'] = 1

    print('Downloading SGU ETF data ...')
    ###################################################################################
    # SGU Bond & Alternatives
    ###################################################################################
    bond_etf_list = ['AGG US Equity','SHV US Equity','VTIP US Equity',
                    'JPST US Equity','HYG US Equity','EMB US Equity']
    alternative_etf_list = ['IAU US Equity','VNQ US Equity']
    
    current_list = bond_etf_list + alternative_etf_list

    data = download_his(
        current_list, ['PX_LAST'], start, end, period='DAILY', currency='USD', dpdf=None,
        normal=True, abnormal=True, split=True,
        # normal=False, abnormal=False, split=False,
        calendar=None, non_trading_day_fill_option='ALL_CALENDAR_DAYS'
    ).ffill()
    data.columns = data.columns.get_level_values(0)
    data = data[current_list]

    name_list = ['ETF_' + ele.split(' ')[0] + '.US.csv' for ele in data.columns]

    original_data = [pd.read_csv(join(addpath.data_path, 'bundles', ele), index_col=[0], parse_dates=[0])['close'] for ele in name_list]
    original_data = pd.concat(original_data, axis=1)
    original_data.columns = name_list 
    
    data.columns = name_list
    
    new_data = []
    for col in original_data:
        tmp_original = original_data.loc[:,col]
        tmp_new = data.loc[:,col]
        
        tmp_original = (tmp_original.dropna().pct_change() + 1).cumprod()
        tmp_new = tmp_new.loc[tmp_new.index >= tmp_original.index[-1]]
        tmp_original = tmp_original/tmp_original.iloc[-1] * tmp_new.iloc[0]
        
        tmp_new_data = pd.concat([tmp_original, tmp_new])
        tmp_new_data = tmp_new_data.loc[~tmp_new_data.index.duplicated()]
        
        new_data.append(tmp_new_data)
        
    new_data = pd.concat(new_data, axis=1).ffill()
    new_data = new_data.merge(match_dates['meaningless'], how='right', left_index=True, right_index=True).ffill().iloc[:,:-1]
    

    generate_bundle(new_data, join(addpath.data_path, 'bundles'), name_list)
    generate_fxrate(new_data, join(addpath.data_path, 'fxrate'), name_list)
    generate_interest(new_data, join(addpath.data_path, 'interest'), name_list)

    print('Finished downloading SGU ETF data')

    # Generate FER Data
    if mode in ['full', 'no_sr']:
        print('downloading Futures ETF rotation data')
        ###################################################################################
        # FER
        ###################################################################################
        futures_name_list = ['SXO1', 'NH1', 'XP1', 'MES1', 'ACC1', 'HI1', 'SP1', 'KM1', 'PT1']

        fer_futures_list = ['SXO1 Index', 'NH1 Index', 'XP1 Index', 'MES1 Index', 'ACC1 Index', 'HI1 Index', 'SP1 Index', 'KM1 Index', 'PT1 Index']
        fer_second_futures_list = ['SXO2 Index', 'NH2 Index', 'XP2 Index', 'MES2 Index', 'ACC2 Index', 'HI2 Index', 'SP2 Index', 'KM2 Index', 'PT2 Index']

        fer_underlying_index = ['SXXP Index', 'NKY Index', 'AS51 Index', 'MXEF Index', 'M1CNX Index', 'HSI Index', 'SPX Index', 'KOSPI2 Index', 'SPTSX60 Index']
        fer_return_index = ['SXXR Index', 'NKYNTR Index', 'AS51T Index', 'M1EF Index', 'M1CNX Index', 'HSI1N Index', 'SPTR500N Index', 'KSP2NTR Index', 'SPTSX60N Index']

        # original version
        # fer_etf_list = ['EXSA TH Equity', '1329 JP Equity', 'IOZ AU Equity', 'EEM US Equity', 'MCHI US Equity', '2800 HK Equity', 'SPY US Equity', '069500 KS Equity', 'XIU CN Equity']
        # Simple version
        fer_etf_list = ['VGK US Equity', 'EWJ US Equity', 'IOZ AU Equity', 'EEM US Equity', 'MCHI US Equity', '2800 HK Equity', 'SPY US Equity', '069500 KS Equity', 'XIU CN Equity']

        current_list = fer_underlying_index + fer_return_index + fer_etf_list

        data = download_his(
            current_list, ['PX_LAST'], start, end, period='DAILY', currency='USD', dpdf=None,
            normal=True, abnormal=True, split=True,
            # normal=False, abnormal=False, split=False,
            calendar=None, non_trading_day_fill_option='ALL_CALENDAR_DAYS'
        ).ffill()
        data.columns = data.columns.get_level_values(0)
        data = data[current_list]

        ############## ETF ###############
        bundle_df = data[fer_etf_list]
        name_list = [ele + '_ETF.csv' for ele in futures_name_list]

        generate_bundle(bundle_df, join(addpath.data_path, 'future_etf_rotation', 'bundles'), name_list)
        generate_fxrate(bundle_df, join(addpath.data_path, 'future_etf_rotation', 'fxrate'), name_list)
        generate_interest(bundle_df, join(addpath.data_path, 'future_etf_rotation', 'interest'), name_list)

        ############## Index ###############
        bundle_df = data[fer_underlying_index]
        bundle_df = bundle_df.loc[:,~bundle_df.columns.duplicated()]
        name_list = [ele + '_Index.csv' for ele in futures_name_list]

        generate_bundle(bundle_df, join(addpath.data_path, 'future_etf_rotation', 'bundles'), name_list)
        generate_fxrate(bundle_df, join(addpath.data_path, 'future_etf_rotation', 'fxrate'), name_list)
        generate_interest(bundle_df, join(addpath.data_path, 'future_etf_rotation', 'interest'), name_list)

        ############## Total Return Index ###############
        bundle_df = data[fer_return_index]
        bundle_df = bundle_df.loc[:,~bundle_df.columns.duplicated()]
        name_list = [ele + '_TR_Index.csv' for ele in futures_name_list]

        generate_bundle(bundle_df, join(addpath.data_path, 'future_etf_rotation', 'for_calculation', 'TR_Index'), name_list)

    
        ############## DTM ###############
        current_list = fer_futures_list

        data = download_his(
            current_list, ['FUT_CUR_GEN_TICKER'], start, end, period='DAILY', currency='USD', dpdf=None,
            normal=True, abnormal=True, split=True,
            # normal=False, abnormal=False, split=False,
            calendar=None, non_trading_day_fill_option='ALL_CALENDAR_DAYS'
        ).ffill()
        data.columns = data.columns.get_level_values(0)
        data = data[current_list]

        maturity_df = pd.DataFrame()
        for col in data:
            tmp_df = data[col].to_frame()
            tmp_df['date'] = tmp_df.index
            tmp_df['date'] = tmp_df['date'].shift(1)
            tmp_df.loc[tmp_df[col].duplicated(),'date'] = None
            tmp_df['date'] = tmp_df['date'].shift(-1)
            tmp_df['date'] = tmp_df['date'].bfill()
            col_maturity_date = tmp_df['date']
            col_maturity_date.name = tmp_df.columns[0]
            
            maturity_df = pd.concat([maturity_df, col_maturity_date], axis=1)
            
        maturity = download_ref(current_list, ['LAST_TRADEABLE_DT'])
        for col in data:
            maturity_df.loc[maturity_df[col].isna(), col] = maturity.loc[col].iloc[0]
            
        maturity_df.index = pd.to_datetime(maturity_df.index)

        dtm = maturity_df.subtract(maturity_df.index, axis=0).apply(lambda x: x.dt.days)
        dtm[dtm > 100] = 1000

        name_list = [ele + '.csv' for ele in futures_name_list]
        generate_ref(dtm, join(addpath.data_path, 'future_etf_rotation', 'reference_data', 'dtm'), name_list, 'dtm')

        name_list = [ele + '_ETF.csv' for ele in futures_name_list] + [ele + '_Index.csv' for ele in futures_name_list]
        bundle_df = pd.DataFrame(0, index=dtm.index, columns=dtm.columns.tolist() * 2)

        generate_ref(bundle_df, join(addpath.data_path, 'future_etf_rotation', 'reference_data', 'dtm'), name_list, 'dtm')


        ############## DVD Yield ###############
        current_list = fer_underlying_index

        data = download_his(
            current_list, ['EQY_DVD_YLD_EST'], start, end, period='DAILY', currency='USD', dpdf=None,
            normal=True, abnormal=True, split=True,
            # normal=False, abnormal=False, split=False,
            calendar=None, non_trading_day_fill_option='ALL_CALENDAR_DAYS'
        ).ffill()
        data.columns = data.columns.get_level_values(0)
        data = data[current_list]
        data = data/100

        name_list = [ele + '.csv' for ele in futures_name_list]
        generate_ref(data, join(addpath.data_path, 'future_etf_rotation', 'for_calculation', 'dvd_yield'), name_list, 'dvd_yield')

        ############## Future Raw Price ###############
        LocalTerminal = Terminal('bbg.algo-api.aqumon.com', 28194)

        current_list = fer_futures_list
        data = download_his(
            current_list, ['PX_LAST'], start, end, period='DAILY', currency='USD', dpdf=None,
            normal=True, abnormal=True, split=True,
            # normal=False, abnormal=False, split=False,
            calendar=None, non_trading_day_fill_option='ALL_CALENDAR_DAYS'
        ).ffill()
        data.columns = data.columns.get_level_values(0)
        data = data[current_list]

        raw_price = data

        current_list = fer_second_futures_list
        data = download_his(
            current_list, ['PX_LAST'], start, end, period='DAILY', currency='USD', dpdf=None,
            normal=True, abnormal=True, split=True,
            # normal=False, abnormal=False, split=False,
            calendar=None, non_trading_day_fill_option='ALL_CALENDAR_DAYS'
        ).ffill()
        data.columns = data.columns.get_level_values(0)
        data = data[current_list]

        second_raw_price = data

        ############## Future Adjusted Price ###############
        LocalTerminal = Terminal('bbg.algo-api.aqumon.com', 18194)

        current_list = fer_futures_list
        data = download_his(
            current_list, ['PX_LAST'], start, end, period='DAILY', currency='USD', dpdf=None,
            normal=True, abnormal=True, split=True,
            # normal=False, abnormal=False, split=False,
            calendar=None, non_trading_day_fill_option='ALL_CALENDAR_DAYS'
        ).ffill()
        data.columns = data.columns.get_level_values(0)
        data = data[current_list]

        adjusted_price = data

        ##### Remember to check if it is actually raw and adjusted
        raw_return = (raw_price.pct_change() + 1).cumprod()
        adjusted_return = (adjusted_price.pct_change() + 1).cumprod()
        ##### Adjusted return should be way higher than raw return

        name_list = [ele + '.csv' for ele in futures_name_list]
        generate_bundle(raw_price, join(addpath.data_path, 'future_etf_rotation', 'for_calculation', 'futures_raw_price'), name_list)

        name_list = [ele + '.csv' for ele in futures_name_list]
        generate_ref(second_raw_price, join(addpath.data_path, 'future_etf_rotation', 'reference_data', 'second_futures_price'), name_list, 'price')

        name_list = [ele + '_ETF.csv' for ele in futures_name_list] + [ele + '_Index.csv' for ele in futures_name_list]
        generate_ref(pd.DataFrame(0, index=second_raw_price.index, columns=second_raw_price.columns.tolist() * 2), join(addpath.data_path, 'future_etf_rotation', 'reference_data', 'second_futures_price'), name_list, 'price')

        name_list = [ele + '.csv' for ele in futures_name_list]
        generate_bundle(adjusted_price, join(addpath.data_path, 'future_etf_rotation', 'bundles'), name_list)

        ###################################################################################
        # Interest Rate
        ###################################################################################
        LocalTerminal = Terminal('bbg.algo-api.aqumon.com', 8194)
        OIS_curve = ['FEDL01 Index', 'USSO1Z Curncy', 'USSO2Z Curncy', 'USSO3Z Curncy', 'USSOA Curncy', 'USSOB Curncy', 
                    'USSOC Curncy', 'USSOD Curncy', 'USSOE Curncy', 'USSOF Curncy', 'USSOI Curncy', 'USSO1 Curncy', 'USSO1F Curncy']
        JPY_OIS_curve = ['MUTKCALM Index', 'JYSO1Z Curncy', 'JYSO2Z Curncy', 'JYSO3Z Curncy', 'JYSOA Curncy', 'JYSOB Curncy', 
                        'JYSOC Curncy', 'JYSOD Curncy', 'JYSOE Curncy', 'JYSOF Curncy', 'JYSOI Curncy', 'JYSO1 Curncy', 'JYSO1F Curncy']
        EUR_OIS_curve = ['EONIA Index', 'EUSWE1Z Curncy', 'EUSWE2Z Curncy', 'EUSWEA Curncy', 'EUSWEB Curncy', 'EUSWEC Curncy', 
                        'EUSWED Curncy', 'EUSWEE Curncy' ,'EUSWEG Curncy' ,'EUSWEH Curncy' ,'EUSWEI Curncy' ,'EUSWE1 Curncy' ,'EUSWE1F Curncy']
        AUD_OIS_curve = ['RBACOR Index', 'ADSOA Curncy', 'ADSOB Curncy', 'ADSOC Curncy', 'ADSOD Curncy', 
                        'ADSOE Curncy', 'ADSOF Curncy', 'ADSOI Curncy', 'ADSO1 Curncy']
        CAD_OIS_curve = ['CAONREPO Index', 'CDSO1Z Curncy', 'CDSO2Z Curncy', 'CDSOA Curncy', 'CDSOB Curncy', 'CDSOC Curncy', 'CDSOD Curncy', 'CDSOE Curncy', 'CDSOF Curncy', 'CDSOI Curncy', 'CDSO1 Curncy']
        HIBOR_curve = ['HIHDO/N Index', 'HIHD1W Index', 'HIHD2W Index', 'HIHD01M Index', 'HIHD02M Index', 'HIHD03M Index', 'HIHD06M Index', 'HIHD12M Index']

        data_list = []
        for current_list in [OIS_curve, JPY_OIS_curve, EUR_OIS_curve, AUD_OIS_curve, CAD_OIS_curve, HIBOR_curve]:
            data = download_his(
                current_list, ['PX_LAST'], start, end, period='DAILY', currency='USD', dpdf=None,
                normal=True, abnormal=True, split=True,
                # normal=False, abnormal=False, split=False,
                calendar=None, non_trading_day_fill_option='ALL_CALENDAR_DAYS'
            ).ffill()
            data.columns = data.columns.get_level_values(0)
            data = data[current_list]
            
            data_list.append(data)

        region_list = ['US', 'JPY', 'EUR', 'AUD', 'CAD', 'HKD']
        
        writer = pd.ExcelWriter(join(addpath.data_path, 'future_etf_rotation', 'for_calculation', 'interest_rate', 'interest_rate.xlsx'))
        for i in range(len(data_list)):
            data_list[i].to_excel(writer, sheet_name=region_list[i])
        writer.save()

        tmp_dtm = dtm.copy()
        tmp_dtm[tmp_dtm > 90] = 0

        ir_list = []
        for i in range(len(data_list)):
            data = data_list[i]
            region = region_list[i]
            
            data = data/100
            empty_set = np.empty([18 * 30,dtm.shape[0]])
            empty_set[:] = np.NaN
            ir = pd.DataFrame(empty_set, columns=dtm.index)
            
            if region == 'HKD':
                dates = [0, 6, 13, 29, 59, 89, 179, 364] # HKD
            elif region == 'AUD':
                dates = [0, 29, 59, 89, 119, 149, 179, 269, 364] # AUD
            elif region == 'CAD':
                dates = [0, 6, 13, 29, 59, 89, 119, 149, 179, 269, 364] #CAD
            else:
                dates = [0, 6, 13, 20, 29, 59, 89, 119, 149, 179, 269, 364, 539] # Others

            mapped_index = dict(zip(data.columns, dates))
            
            for key, value in mapped_index.items():
                ir.loc[value] = data[key]
                
            ir = ir.interpolate().T

            r = []
            for index, row in tmp_dtm.iterrows():
                tmp_ser = ir.loc[row.name,list(row.fillna(0).values)]
                tmp_ser.index = dtm.columns
                r.append(tmp_ser)
                
            ir = pd.concat(r, axis=1).T
            ir_list.append(ir)

        usd_ir, jpy_ir, eur_ir, aud_ir, cad_ir, hkd_ir = ir_list[0], ir_list[1], ir_list[2], ir_list[3], ir_list[4], ir_list[5]

        correct_ir = pd.concat([eur_ir['SXO1 Index'], jpy_ir['NH1 Index'], aud_ir['XP1 Index'], usd_ir['MES1 Index'], usd_ir['ACC1 Index'],
                                hkd_ir['HI1 Index'], usd_ir['SP1 Index'], usd_ir['KM1 Index'], cad_ir['PT1 Index']], axis=1).ffill()

        name_list = [ele + '.csv' for ele in futures_name_list]
        generate_ref(correct_ir, join(addpath.data_path, 'future_etf_rotation', 'for_calculation', 'interest_rate'), name_list, 'ir')

        ###################################################################################
        # Net Dividend
        ###################################################################################
        current_list = fer_underlying_index

        data = download_his(
            current_list, ['INDX_NET_DAILY_DIV'], start, end, period='DAILY', currency='USD', dpdf=None,
            normal=True, abnormal=True, split=True,
            # normal=False, abnormal=False, split=False,
            calendar=None, non_trading_day_fill_option='ALL_CALENDAR_DAYS'
        ).ffill()
        data.columns = data.columns.get_level_values(0)
        data = data[current_list]

        name_list = [ele + '.csv' for ele in futures_name_list]
        generate_ref(data, join(addpath.data_path, 'future_etf_rotation', 'for_calculation', 'net_dividend'), name_list, 'net_dvd')

    # Generate SR Data
    if mode in ['full', 'no_fer']:
        print('downloading Smart Rotation data')
        ###################################################################################
        # Smart Rotation bundles
        ###################################################################################
        SMART_ROTATION_ETF = ['ASHR US Equity', 'EWA US Equity', 'EWC US Equity','EWG US Equity', 'EWH US Equity', 'EWJ US Equity', 
                            'EWL US Equity', 'EWU US Equity', 'EWY US Equity', 'EWZ US Equity', 'INDA US Equity']

        SMART_ROTATION_INDEX = ['SHSZ300 Index', 'MXAU Index', 'MXCA Index', 'MXDE Index', 'HSI Index', 'MXJP Index', 
                                'MXCH Index', 'MXGB Index', 'MXKR Index', 'MXBR Index', 'MXIN Index']

        current_list = SMART_ROTATION_ETF

        data = download_his(
            current_list, ['PX_OPEN', 'PX_HIGH', 'PX_LOW', 'PX_LAST', 'PX_VOLUME'], '2012-12-31', end, period='DAILY', currency='USD', dpdf=None,
            normal=True, abnormal=True, split=True,
            # normal=False, abnormal=False, split=False,
            calendar=None, non_trading_day_fill_option='ALL_CALENDAR_DAYS'
        ).ffill()
        data.columns = data.columns.get_level_values(0)
        data = data[current_list]

        sr_file_list = os.listdir(join(addpath.data_path, 'smart_rotation', 'Trading'))

        for i in range(len(sr_file_list)):
            sr_file = sr_file_list[i]
            col_data = data[data.columns.unique()[i]]
            col_data.columns = ['open', 'high', 'low', 'close', 'volume']
            tmp = pd.read_csv(os.path.join(addpath.data_path, 'smart_rotation', 'Trading', sr_file), index_col=[0], parse_dates=[0])
            tmp = tmp.iloc[:,:5]
            
            if col_data.isna().sum().iloc[0] != 0:
                last_na = col_data.isna()[::-1].idxmax().iloc[0]
            else:
                last_na = col_data.index[0] - pd.DateOffset(days=1)
            col_data = col_data.dropna()
            
            tmp = tmp.loc[:last_na + pd.DateOffset(days=1)]
            tmp.columns = ['open', 'high', 'low', 'close', 'volume']
            tmp = tmp/tmp.iloc[-1]
            tmp = tmp * col_data.loc[last_na:].iloc[0]
            
            output = pd.concat([tmp, col_data])
            output = output.loc[~output.index.duplicated()]
            output = output['close'].to_frame()
            
            generate_bundle(output, join(addpath.data_path, 'smart_rotation', 'bundles'), [sr_file.split('.')[0] + '.US.csv'])
            generate_fxrate(output, join(addpath.data_path, 'smart_rotation', 'fxrate'), [sr_file.split('.')[0] + '.US.csv'])
            generate_interest(output, join(addpath.data_path, 'smart_rotation', 'interest'), [sr_file.split('.')[0] + '.US.csv'])



            
        ###################################################################################
        # Smart Rotation Mkt cap
        ###################################################################################
        current_list = SMART_ROTATION_INDEX

        data = download_his(
            current_list, ['CUR_MKT_CAP'], start, end, period='DAILY', currency='USD', dpdf=None,
            normal=True, abnormal=True, split=True,
            # normal=False, abnormal=False, split=False,
            calendar=None, non_trading_day_fill_option='ALL_CALENDAR_DAYS'
        ).ffill()
        data.columns = data.columns.get_level_values(0)
        data = data[current_list]

        sr_file_list = os.listdir(join(addpath.data_path, 'smart_rotation', 'mktcap'))
                                
        for i in range(len(sr_file_list)):
            sr_file = sr_file_list[i]
            col_data = data[data.columns.unique()[i]]
            
            tmp = pd.read_csv(os.path.join(addpath.data_path, 'smart_rotation', 'mktcap', sr_file), index_col=[0], parse_dates=[0])
            first_index = tmp.index[-1] + pd.DateOffset(days=1)
            
            col_data = col_data.loc[first_index:].to_frame()
            col_data.columns = ['mktcap']
            
            output = pd.concat([tmp, col_data])
            generate_ref(output, join(addpath.data_path, 'smart_rotation', 'mktcap'), [sr_file.split('.')[0] + '.US.csv'], 'mktcap')
            
        ###################################################################################
        # Smart Rotation BPS
        ###################################################################################

        current_list = SMART_ROTATION_INDEX

        data = download_his(
            current_list, ['BOOK_VAL_PER_SH'], '2020-10-27', end, period='DAILY', currency='USD', dpdf=None,
            # normal=True, abnormal=True, split=True,
            normal=False, abnormal=False, split=False,
            calendar=None, non_trading_day_fill_option='ALL_CALENDAR_DAYS'
        ).ffill()
        data.columns = data.columns.get_level_values(0)
        data = data[current_list]
        data

        tmp = pd.read_csv(join(addpath.data_path, 'smart_rotation', 'Factors_Fundamental', 'index_BPS.csv'), index_col=[0], parse_dates=[0])
        data = data[tmp.columns]

        first_index = tmp.index[-1] + pd.DateOffset(days=1)
        data = data.loc[first_index:]

        output = pd.concat([tmp, data])
        output.to_csv(join(addpath.data_path, 'smart_rotation', 'Factors_Fundamental', 'index_BPS.csv'))


        ###################################################################################
        # Smart Rotation close
        ###################################################################################
        current_list = SMART_ROTATION_INDEX

        data = download_his(
            current_list, ['PX_LAST'], start, end, period='DAILY', currency='USD', dpdf=None,
            normal=True, abnormal=True, split=True,
            # normal=False, abnormal=False, split=False,
            calendar=None, non_trading_day_fill_option='ALL_CALENDAR_DAYS'
        ).ffill()
        data.columns = data.columns.get_level_values(0)
        data = data[current_list]

        tmp = pd.read_csv(join(addpath.data_path, 'smart_rotation', 'Factors_Fundamental', 'index_close.csv'), index_col=[0], parse_dates=[0])
        data = data[tmp.columns]

        first_index = tmp.index[-1] + pd.DateOffset(days=1)
        data = data.loc[first_index:]

        output = pd.concat([tmp, data])
        output.to_csv(join(addpath.data_path, 'smart_rotation', 'Factors_Fundamental', 'index_close.csv'))

        ###################################################################################
        # Smart Rotation EPS
        ###################################################################################

        current_list = SMART_ROTATION_INDEX

        data = download_his(
            current_list, ['TRAIL_12M_EPS'], '2020-10-27', end, period='DAILY', currency='USD', dpdf=None,
            # normal=True, abnormal=True, split=True,
            normal=False, abnormal=False, split=False,
            calendar=None, non_trading_day_fill_option='ALL_CALENDAR_DAYS'
        ).ffill()
        data.columns = data.columns.get_level_values(0)
        data = data[current_list]

        tmp = pd.read_csv(join(addpath.data_path, 'smart_rotation', 'Factors_Fundamental', 'index_EPS.csv'), index_col=[0], parse_dates=[0])
        data = data[tmp.columns]

        first_index = tmp.index[-1] + pd.DateOffset(days=1)
        data = data.loc[first_index:]

        output = pd.concat([tmp, data])
        output.to_csv(join(addpath.data_path, 'smart_rotation', 'Factors_Fundamental', 'index_EPS.csv'))

                
if __name__ == "__main__":
    start = '2010-01-01'
    end = '2020-12-31'
    mode = 'no_fer_sr' # full | no_sr | no_fer | no_fer_sr
    generate_data(start, end, mode)