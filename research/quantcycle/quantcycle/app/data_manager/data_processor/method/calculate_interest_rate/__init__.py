import pathlib
from typing import List
from datetime import datetime

import numpy as np
import pandas as pd

from quantcycle.app.data_manager.data_loader.data_center.mapping import (
    CCY2USDCCP, FWD_SCALE, IS_NDF)

from ..method_base import MethodBase

from quantcycle.utils.time_manipulation import timestamp2datetimestring

CALENDAR_PATH = pathlib.Path(__file__).parent.joinpath(
    'calendar_data.xlsx').absolute()


class MethodINT(MethodBase):
    '''
        action == 'INT' : construct df method
        To output a df with interest rate 

        1. construct df with necessary info 
        2. call method
        3. construct output df

        Ref. V21 get_dm_fx_daily_dividends
    '''

    def __init__(self):
        super().__init__()
        self.is_final_step = True
        self.output_data_type = 'df'

    def on_bundle(self, data_bundle: dict, data_collection) -> None:
        ''' To get necessary data from DataProcessor

        Args:
            data_bundle (dict): The data bundle info from DataProcessor
            data_collection (DataCollection): The data collection info from DataProcessor
        '''
        # # assumption: data_group have one asset type only
        # symbol = data_bundle['Symbol'][0]
        # sym_type = data_collection.get(symbol).info['symboltype']
        # self.is_stock_type = sym_type=='US_STOCK' or sym_type=='HK_STOCK' or sym_type=='CN_STOCK'
        # self.is_fx_type = sym_type=='FX'

        # create data mapping
        self.create_data_mapping(data_bundle, data_collection)
        # collet mapped data
        self.map_data(data_collection)

    def create_data_mapping(self, data_bundle, data_collection):
        self.sym_type = data_bundle['SymbolArgs'].get('AssetType', 'FX')
        self.symbols = data_bundle['Symbol']
        self.cn_symbols = data_bundle['SymbolArgs'].get('DividendAvailable', data_bundle['Symbol'])
        self.is_needed = data_bundle['SymbolArgs'].get('is_needed', False)
        self.start = data_bundle['StartTS']
        self.end = data_bundle['EndTS']
        
        if self.sym_type == 'STOCK' and self.is_needed:
            '''
            GET choice_cash_dividend_fin2
            '''
            dividend_df = pd.DataFrame()
            dividend_list = []
            for symbol in self.symbols:
                if symbol in self.cn_symbols:
                    dividend_df = data_collection[symbol].dividend_related.choice_cash_dividend_fin2
                    dividend_list.append(dividend_df)
                else:
                    dividend_list.append(pd.DataFrame())
            self.dividends = dividend_list
            # check = 0
            
        elif self.sym_type == 'FX':
            self.all_ccy, self.all_ccy_non_usd, self.ccy_matrix_df = _symbol(
                self.symbols)
            data_source = data_bundle.get("SymbolArgs", {}).get(
                "UnderlyingSource", "BGNL")
            self.data_mapping["USD INT"] = "USD INT"
            self.data_mapping.update(
                {ccy+" TN": ccy+" TN" for ccy in self.all_ccy})
            self.data_mapping.update(
                {CCY2USDCCP[ccy]: CCY2USDCCP[ccy]+" "+data_source+"/fxrate" for ccy in self.all_ccy_non_usd})
        elif self.sym_type == 'FUTURES':
            self.data_mapping.update(
                {sym+' data': sym+"/data" for sym in self.symbols})
            self.data_mapping.update(
                {sym+' rollover_data': sym+"/rollover_data" for sym in self.symbols})

    def run(self):
        if self.sym_type == 'STOCK':
            rate_df, sym_list, fields = self._STOCK_run()
        elif self.sym_type == 'FX':
            rate_df, sym_list, fields = self._FX_run()
        elif self.sym_type == 'FUTURES':
            rate_df, sym_list, fields = self._FUTURES_run()

        return rate_df, sym_list, fields
   
    def _FUTURES_run(self):
        list_df = []
        empty_df = pd.DataFrame(columns=['interest_rate_last'])
        for sym in self.symbols:
            data_df = self.data_dict.get(f"{sym} data", pd.DataFrame())
            if 'field' in data_df.columns:
                # FOR settlement type == 'open'
                data_df = data_df[['data']][data_df['field'] == 1] # select close ONLY
                data_df.columns = ['close']
            rollover_df = self.data_dict.get(f"{sym} rollover_data", pd.DataFrame())
            rollover_df = rollover_df.bfill()
            if data_df.empty or rollover_df.empty or 'next_close' not in list(rollover_df.columns): 
                list_df.append(empty_df)
                continue
            df = pd.DataFrame(index=data_df.index, columns=[sym])
            tmp = pd.concat([data_df,rollover_df], join='inner', axis=1)
            df[sym] = tmp.next_close.where(tmp['next_close'].fillna(0) != tmp['next_close'].shift(-1).fillna(0))
            df[sym] = ((data_df['close'] - df[sym]).fillna(0)) #/data_df['close'])
            list_df.append(df[sym])

        rate_df = pd.concat(list_df,axis=1)        
        return rate_df, self.symbols, ['interest_rate_last']

    def _FX_run(self) -> tuple:
        # prepare data frame
        lst_df_CCY = []
        for ccy in self.all_ccy_non_usd:
            df = _construct_cal_interest_input_df(self.data_dict, ccy)
            lst_df_CCY.append(df)
        # calculate interest rate for all non usd ccy
        df_tn = _ccys_interest_rate(
            ccys=self.all_ccy_non_usd, lst_df_CCY=lst_df_CCY)
        # get usd interest rate
        df_usd = self.data_dict["USD INT"]
        df_usd.columns = ["USD"]
        # get interest rate for ccps
        rate_df = _ccy2ccp_interest(
            df_tn, df_usd, self.all_ccy, self.ccy_matrix_df, self.symbols)
        
        return rate_df, self.symbols, ['interest_rate_last']

    def _STOCK_run(self):
        '''
            1. A share: update, others: no changes
            2. MAIN: LocalCSV, FXRATE or INT: Datamaster

            Dataframe Start to end date:
            1. fill 0 if no dividend
            
        '''

        
        # column = self.symbols
        # pre_merged_dividends = []

        
        # # Generate a dataframe with only dates where dividend events occur for each symbol
        # for i in range(len(self.dividends)):
        #     non_zero_dates = self.dividends[i].index
        #     non_zero_values = self.dividends[i].values
        #     dividend_dataframe = pd.DataFrame(non_zero_dates.values.tolist(),columns=['timestamp'])
        #     dividend_dataframe[column[i]] = pd.Series(non_zero_values.tolist(),index=dividend_dataframe.index)
        #     pre_merged_dividends.append(dividend_dataframe)

        # # Merge all dataframes on timestamp
        # post_merged_dividends = pd.DataFrame(0,index=[self.start],columns=column) #includes starting date
        # post_merged_dividends.index.name = 'timestamp'
        # #concat here
        # for i in range(len(pre_merged_dividends)):
        #     post_merged_dividends = (pd.merge(post_merged_dividends, pre_merged_dividends[i], on=['timestamp',column[i]], how='outer')
        #                             .fillna(0)
        #                             .sort_values('timestamp')
        #                             .set_index('timestamp'))
        # post_merged_dividends = post_merged_dividends.groupby(['timestamp']).mean().reset_index()
        # n_dates = len(post_merged_dividends)
        # # Include filler dates that succeed event dates e.g. Jan 15th has an event, Jan 20th has an event, we need to
        # # generate a row filled with zeros for Jan 16th
        # # Also need to include first row of dividends based on the starting date
        # zero_dates=pd.DataFrame(0,index=[self.start],columns=column)
        # zero_dates.index.name='timestamp'
        # for i in range(len(post_merged_dividends)-2):
        #     next_day = post_merged_dividends['timestamp'][i+1]+86400
        #     if next_day != post_merged_dividends['timestamp'][i+2]:
        #         zero_row = pd.DataFrame([np.zeros((1,len(self.symbols))).tolist()[0]],
        #         index=[next_day],columns=column.tolist())
        #         zero_dates=zero_dates.append(zero_row)
        # zero_dates=zero_dates.reset_index()
        # post_merged_dividends = (pd.merge(post_merged_dividends, zero_dates, on=['timestamp'], how='outer')
        #                         .fillna(0)
        #                         .sort_values('timestamp')
        #                         .set_index('timestamp'))
        # #includes the day after the most recent dividend date if it is not over the end date
        # if post_merged_dividends.index[-1]+86400<self.end:
        #     post_merged_dividends=post_merged_dividends.append(pd.DataFrame([np.zeros((1,len(self.symbols))).tolist()[0]],
        #                           index=[post_merged_dividends.index[-1]+86400],columns=column.tolist()))

        # return post_merged_dividends, self.symbols, ['interest_rate_last']

                #Check if symbols are a_shares, shortcut now is to just find 'S' in 2nd last place
        column = self.symbols
        # Since datamaster returns timestamps at 6:57am, we must adjust the start and end time (default 00:00am)
        start_timestamp = self.start

        if self.is_needed:
            start_dt = datetime.fromtimestamp(start_timestamp)
            adjust_start_dt = start_dt.replace(hour=14,minute=57)
            adjust_start_timestamp = int(datetime.timestamp(adjust_start_dt))
            end_timestamp = self.end
            end_dt = datetime.fromtimestamp(end_timestamp)
            adjust_end_dt = end_dt.replace(hour=14,minute=57)
            adjust_end_timestamp = int(datetime.timestamp(adjust_end_dt))
            timestamp_range = []
            while adjust_start_timestamp <= adjust_end_timestamp:
                timestamp_range.append(adjust_start_timestamp)
                adjust_start_timestamp+=86400
            # First create an empty dataframe with all daily timestamps
            non_fx_Rate_df = pd.DataFrame({},columns=column,index=timestamp_range).ffill(axis=0,).fillna(0)

            # Then fill in entries where cash diviend occurs
            for i in range(len(self.dividends)):
                non_zero_dates = self.dividends[i].index
                non_zero_values = self.dividends[i].values
                for j in range(len(non_zero_dates)):
                    non_fx_Rate_df.loc[non_zero_dates[j],column[i]] = non_zero_values[j]
        else:
            non_fx_Rate_df = pd.DataFrame(0,columns=column,index=[self.start])
        
        return non_fx_Rate_df, self.symbols, ['interest_rate_last']


def _symbol(symbols):
    '''
        parse symbol
    '''
    all_ccy = set()
    for sym in symbols:
        all_ccy.add(sym[:3])
        all_ccy.add(sym[3:])
    all_ccy = list(sorted(all_ccy))

    ccy_matrix_df = pd.DataFrame(
        np.zeros((len(all_ccy), len(symbols))), index=all_ccy, columns=symbols)
    for symbol in symbols:
        left_ccy, right_ccy = [symbol[:3], symbol[3:]]
        ccy_matrix_df.loc[left_ccy][symbol] = 1
        ccy_matrix_df.loc[right_ccy][symbol] = -1

    all_ccy_non_usd = set(all_ccy)
    all_ccy_non_usd.discard('USD')
    all_ccy_non_usd = list(sorted(all_ccy_non_usd))

    return all_ccy, all_ccy_non_usd, ccy_matrix_df


def _ccy2ccp_interest(ccy_int_df, usd_int_df, all_ccy, ccy_matrix_df, symbols):
    all_ccy_int = pd.concat(
        [ccy_int_df, usd_int_df], axis=1).fillna(method='ffill')

    all_ccy_int = all_ccy_int[all_ccy]
    rate_value = np.matmul(all_ccy_int.values, ccy_matrix_df.values)[1:]
    time_index = all_ccy_int.index[:-1]
    rate_df = pd.DataFrame(rate_value, index=time_index,
                           columns=symbols)
    rate_df.ffill(axis=0, inplace=True)

    time_index = pd.to_datetime(rate_df.index, unit='s')
    time_index_weekday = (time_index.weekday.values == 2).astype(int)
    rate_multiple = (time_index_weekday*2 +
                     np.ones(len(time_index_weekday))) / 36000
    for col in rate_df.columns:
        rate_df[col] = rate_df[col]*rate_multiple

    # output dataframe
    return rate_df


def _construct_cal_interest_input_df(data, ccy: str):
    '''
        construct the df for calculate_interest_rate
    '''
    ccp = CCY2USDCCP[ccy]

    lst_columns = [data[f'{ccy} TN'], data[f'{ccp}'].close, data['USD INT']]
    df = pd.concat(lst_columns, axis=1)
    
    # Checking if the last two dates have enough data to calculate the dividend
    # Exclude the rows that have not enough data 
    if len(df.index) >= 2:
        for i in range(-2,0):
            if df.iloc[i].isnull().any():
                df = df.iloc[:i]
                break

    # from timestamp to datetime
    list_calendar_dates = pd.to_datetime(
        df.index, unit='s').to_list()
    df['delta_T'] = _get_delta_T(ccy, list_calendar_dates)

    df['FWD_SCALE'] = _get_fwd_scale(ccy)

    df.columns = [f'TN', 'fx_last', 'USD_rate', 'delta_T', 'FWD_SCALE']
    return df


def _ccys_interest_rate(ccys: List[str], lst_df_CCY: List[pd.DataFrame]) -> pd.DataFrame:
    '''
    To calculate interest rate
    Index of list obj should be corresponding to the same ccy
    e.g. ccys[i] -> lst_df_CCY[i]

    Parameters:
        ccys: Set[str]
            set of currencies 
        lst_df_CCY: 
            list of df for different ccy
            first column must be 'TN' or 'FP'
            e.g.
                    FP/TN fx_last USD_rate delta_T FW_SCALE
            date

            (delta_T = 30 for FP)
    '''
    rate_df_list = []
    for i in range(len(ccys)):
        rate_df = pd.DataFrame()
        df_ccp = lst_df_CCY[i]

        ccp = CCY2USDCCP[ccys[i]]
        direction = 1 if 'USD' == ccp[:3] else -1

        # calculation of interest rate from TN and FX spot
        rate_df[f'{ccys[i]}'] = (direction * 360 * df_ccp['TN'] / 10 ** df_ccp[f'FWD_SCALE'] /
                                 df_ccp['delta_T'] / df_ccp['fx_last'] + df_ccp['USD_rate'] / 100) * 100

        #rate_df.index = pd.to_datetime(df_ccp.index, format='%Y-%m-%d')
        rate_df.index = df_ccp.index
        rate_df_list.append(rate_df)

    rate_spot = pd.concat(rate_df_list, axis=1)
    rate_spot.ffill(inplace=True)
    return rate_spot


def _get_delta_T(ccy: str, list_calendar_dates: List):
    list_buzdays = list(pd.bdate_range(start='1990-01-01', end='2025-12-31'))
    ccp = CCY2USDCCP[ccy]
    if IS_NDF.get(ccp, False):
        return 30

    # TODO: get calendar from dm???
    df_calendar = pd.read_excel(CALENDAR_PATH).iloc[1:]
    ccy_calendar = df_calendar[f'{ccy} Curncy'].tolist()
    usd_calendar = df_calendar['USD Curncy'].tolist()
    list_holidays = list(set(ccy_calendar + usd_calendar))
    list_non_holidays = list(set(list_buzdays).difference(set(list_holidays)))

    list_calendar_dates.sort()
    list_non_holidays.sort()
    list_delta_T = [_search_next(x, list_non_holidays)
                    for x in list_calendar_dates]
    return list_delta_T


def _search_next(x, list_non_holidays: List):
    list_non_holidays_copy = list_non_holidays.copy()
    if x not in list_non_holidays:
        list_non_holidays_copy.append(x)
        list_non_holidays_copy.sort()
    next_index = list_non_holidays_copy.index(x) + 2
    if next_index < len(list_non_holidays_copy):
        return (list_non_holidays_copy[next_index] - list_non_holidays_copy[next_index - 1]).days
    else:
        return None


def _get_fwd_scale(ccy: str):
    return FWD_SCALE[CCY2USDCCP[ccy]]