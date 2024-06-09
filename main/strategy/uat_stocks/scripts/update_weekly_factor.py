import os
import sys
from collections import defaultdict
from datetime import datetime

sys.path.append(".")

import numpy as np
import pandas as pd
import statsmodels.api as sm
from datamaster import dm_client
from dateutil.relativedelta import relativedelta
from retrying import retry
from strategy.commonalgo.data.choice_proxy_client import choice_client as c
from tqdm import tqdm



dm_client.start()
RAW_DATA_ROOT = 'strategy/stocks/data/weekly_factor/'


class EquityWeeklyFactor():
    def __init__(self):

        self.start_date = '2014-01-01'
        self.start_date_4y = '2010-01-01'
        self.end_date = datetime.today().strftime('%Y-%m-%d')

        self.all_stock_set = pd.read_csv(
            'strategy/stocks/stock_universe/aqm_cn_stock.csv')['SECUCODE'].tolist()
        self.min_root_path = 'https://hk-qb-data01.aqumonx.xyz:8000/cn/k_min/'

        self.benchmark_df = pd.read_csv(
            '//192.168.9.170/share/alioss/1_StockStrategy/data/CSI300TR.csv',
            index_col=0, parse_dates=True)
        self.shibor_df = pd.read_csv(
            '//192.168.9.170/share/alioss/1_StockStrategy/data/SHIBOR.csv',
            index_col=0, parse_dates=True)

    def _get_log(self, x):
        if x > 0:
            return np.log(x)
        else:
            return np.nan

    def _get_datetime(self, date):
        return datetime(date.year, date.month, date.day)

    @retry
    def _get_css_data(self, stock_set, factor, params):
        data = c.css(stock_set, factor, params)
        data.columns = stock_set
        return data

    def get_equity_daily_data(self):
        if not os.path.exists(RAW_DATA_ROOT + 'daily_data'):
            os.mkdir(RAW_DATA_ROOT + 'daily_data')
        for ticker in tqdm(self.all_stock_set):
            res = dm_client.historical(
                symbols=ticker,
                start_date=self.start_date_4y,
                end_date=self.end_date,
                fields='open,high,low,close,volume,amount,choice_turnover',
                calendar='SSE',
                fill_method='ffill')
            daily_df = pd.DataFrame(res['values'][ticker], columns=res['fields'])
            daily_df.to_csv(RAW_DATA_ROOT + f'daily_data/{ticker}.csv',
                            index=False)
        return

    def get_equity_daily_data_adjust(self):
        if not os.path.exists(RAW_DATA_ROOT + 'daily_data_adjust'):
            os.mkdir(RAW_DATA_ROOT + 'daily_data_adjust')
        for ticker in tqdm(self.all_stock_set):
            res = dm_client.historical(
                symbols=ticker,
                start_date=self.start_date_4y,
                end_date=self.end_date,
                fields='open,high,low,close,volume,amount,choice_turnover',
                calendar='SSE',
                fill_method='ffill',
                adjust_type=3)
            daily_df = pd.DataFrame(res['values'][ticker], columns=res['fields'])
            daily_df.to_csv(RAW_DATA_ROOT + f'daily_data_adjust/{ticker}.csv',
                            index=False)
        return

    def get_equity_minute_data(self):
        if not os.path.exists(RAW_DATA_ROOT + 'minute_data_new'):
            os.mkdir(RAW_DATA_ROOT + 'minute_data_new')
        for ticker in tqdm(self.all_stock_set):
            minute_df_list = []
            for year in range(int(self.start_date[:4]), 2021, 1):
                try:
                    minute_df_sub = pd.read_csv(
                        self.min_root_path + ticker + '/{}.csv'.format(str(year)),
                        index_col=2)
                except FileNotFoundError:
                    minute_df_sub = pd.DataFrame()
                minute_df_list.append(minute_df_sub)
            minute_df = pd.concat(minute_df_list, axis=0)
            minute_df.index = pd.to_datetime(minute_df.index, format='%Y%m%d%H%M%S')
            minute_df.to_csv(RAW_DATA_ROOT + f'minute_data_new/{ticker}.csv',
                             index=True)
        return

    def get_fundamental_data(self):
        if not os.path.exists(RAW_DATA_ROOT + 'info'):
            os.mkdir(RAW_DATA_ROOT + 'info')
        all_date_set = pd.date_range(self.start_date, self.end_date, freq='1w')
        for factor in ['PB', 'LIQASHARE']:
            try:
                factor_df = pd.read_csv(RAW_DATA_ROOT + f'info/{factor}.csv',
                                        index_col=0, parse_dates=True)
            except FileNotFoundError:
                factor_df = pd.DataFrame()
            for date in tqdm(all_date_set):
                date_str = date.strftime("%Y-%m-%d")
                if date in factor_df.index:
                    continue
                date_df = pd.DataFrame()
                for i in range(len(self.all_stock_set) // 100 + 1):
                    params = f'EndDate={date_str}, TradeDate={date_str}'
                    data = self._get_css_data(
                        self.all_stock_set[i * 100: (i + 1) * 100], factor, params)
                    date_df = pd.concat([date_df, data], axis=1)
                date_df.columns = self.all_stock_set
                factor_df = pd.concat([factor_df, date_df], axis=0)
            factor_df.index = all_date_set
            factor_df.to_csv(RAW_DATA_ROOT + f'info/{factor}.csv')
        for factor in ['BLHSWSIND']:
            try:
                factor_df = pd.read_csv(RAW_DATA_ROOT + f'info/{factor}.csv',
                                        index_col=0, parse_dates=True)
            except FileNotFoundError:
                factor_df = pd.DataFrame()
            for date in tqdm(all_date_set):
                date_str = date.strftime("%Y-%m-%d")
                if date in factor_df.index:
                    continue
                date_df = pd.DataFrame()
                for i in range(len(self.all_stock_set) // 100 + 1):
                    params = f'EndDate={date_str},ClassiFication=1'
                    data = self._get_css_data(
                        self.all_stock_set[i * 100: (i + 1) * 100], factor, params)
                    date_df = pd.concat([date_df, data], axis=1)
                date_df.columns = self.all_stock_set
                factor_df = pd.concat([factor_df, date_df], axis=0)
            factor_df.index = all_date_set
            factor_df.to_csv(RAW_DATA_ROOT + f'info/{factor}.csv')

    def get_fundamental_data_2(self):
        if not os.path.exists(RAW_DATA_ROOT + 'fundamental_data'):
            os.mkdir(RAW_DATA_ROOT + 'fundamental_data')
        all_date_set = pd.date_range(self.start_date, self.end_date, freq='1w')
        empty_df = pd.DataFrame(index=all_date_set)
        for ticker in tqdm(self.all_stock_set):
            res = dm_client.historical(symbols=ticker,
                                       start_date=self.start_date_4y,
                                       end_date=self.end_date,
                                       fields='acas_roe,acas_pe',
                                       fill_method='ffill')
            monthly_df = pd.DataFrame(res['values'][ticker], columns=[
                                      'date', 'ROE', 'EP']).set_index('date')
            monthly_df.index = pd.to_datetime(monthly_df.index)
            weekly_df = pd.concat([empty_df, monthly_df], axis=1).ffill()
            weekly_df = weekly_df.loc[empty_df.index]
            weekly_df.to_csv(RAW_DATA_ROOT + f'fundamental_data/{ticker}.csv')

    def parse_industry_data(self):
        if not os.path.exists(RAW_DATA_ROOT + 'industry'):
            os.mkdir(RAW_DATA_ROOT + 'industry')
        df = pd.read_csv(RAW_DATA_ROOT + 'info/BLHSWSIND.csv',
                         index_col=0, parse_dates=True)
        all_date_set = pd.date_range(self.start_date, self.end_date, freq='W')
        for date in tqdm(all_date_set):
            date_str = date.strftime("%Y-%m-%d")
            one_df = df.loc[date].T
            one_df.columns = ['Industry']
            one_df.to_csv(RAW_DATA_ROOT + f'industry/{date_str}.csv')

    def get_fundamental_factor(self):
        if not os.path.exists(RAW_DATA_ROOT + 'factors/fundamental_factor_ticker'):
            os.mkdir(RAW_DATA_ROOT + 'factors/fundamental_factor_ticker')
        shares_df = pd.read_csv(RAW_DATA_ROOT + 'info/LIQASHARE.csv',
                                index_col=0, parse_dates=True)
        bm_df = 1 / pd.read_csv(RAW_DATA_ROOT + 'info/PB.csv',
                                index_col=0, parse_dates=True)
        for ticker in tqdm(self.all_stock_set):
            roe_df = pd.read_csv(RAW_DATA_ROOT + f'fundamental_data/{ticker}.csv',
                                 index_col=0, parse_dates=True)[['ROE']]
            pe_df = pd.read_csv(RAW_DATA_ROOT + f'fundamental_data/{ticker}.csv',
                                index_col=0, parse_dates=True)[['EP']]
            daily_df = pd.read_csv(RAW_DATA_ROOT + f'daily_data/{ticker}.csv',
                                   index_col=0, parse_dates=True)
            daily_df = daily_df.fillna(method='ffill')
            weekly_df = daily_df.asfreq(freq='1w', method='ffill')[
                shares_df.index.tolist()[0]: shares_df.index.tolist()[-1]]

            weekly_df['Size'] = weekly_df['close'].values * \
                shares_df[ticker][weekly_df.index.tolist(
                )[0]: weekly_df.index.tolist()[-1]].values / 1e6

            shares_df[ticker][weekly_df.index.tolist()[0]: weekly_df.index.tolist()
                              [-1]] = weekly_df['Size'].values
            weekly_df['BM'] = bm_df[ticker][weekly_df.index.tolist(
            )[0]: weekly_df.index.tolist()[-1]].values
            weekly_df['ME'] = weekly_df['Size'].apply(lambda x: self._get_log(x))

            weekly_df = pd.concat([weekly_df, roe_df, pe_df],
                                  axis=1).loc[weekly_df.index]
            weekly_df['BMROE'] = weekly_df['BM'] * weekly_df['ROE']

            weekly_df.to_csv(
                RAW_DATA_ROOT + f'factors/fundamental_factor_ticker/{ticker}.csv')
        shares_df.to_csv(RAW_DATA_ROOT + 'info/SIZE.csv')

    def calc_industry_neutral_factor(self):
        if not os.path.exists(RAW_DATA_ROOT + 'factors/fundamental_factor_indneu_date'):
            os.mkdir(RAW_DATA_ROOT + 'factors/fundamental_factor_indneu_date')
        factor_dict_by_ticker = {}
        factors = ['BM', 'ME', 'ROE', 'EP', 'BMROE', 'FScore']
        all_date_set = pd.date_range(self.start_date, self.end_date, freq='1W')
        for ticker in tqdm(self.all_stock_set):
            fundamental_df = pd.read_csv(
                RAW_DATA_ROOT + f'factors/fundamental_factor_ticker/{ticker}.csv',
                index_col=0, parse_dates=True)[['BM', 'ME', 'ROE', 'EP', 'BMROE']]
            fscore_df = pd.read_csv(
                RAW_DATA_ROOT + f'factors/fscore_factor_ticker/{ticker}.csv',
                index_col=0, parse_dates=True)
            factor_dict_by_ticker[ticker] = pd.concat(
                [fundamental_df, fscore_df], axis=1)
        industry_dict = {date: pd.read_csv(
            RAW_DATA_ROOT + f'info/industry/{date.strftime("%Y-%m-%d")}.csv',
            index_col=0) for date in all_date_set}
        result_dict = defaultdict(dict)
        for factor in tqdm(factors):
            df = pd.concat([factor_dict_by_ticker[ticker][[factor]].rename(
                columns={factor: ticker}) for ticker in self.all_stock_set], axis=1)
            for date in tqdm(all_date_set):
                industry_df = industry_dict[date].copy()
                df_now = df.loc[date].dropna()
                ix = [i for i in df_now.index if i in industry_df.index]
                dummy = pd.get_dummies(industry_df.loc[ix]['Industry'])
                df_now = df_now.loc[ix]
                rslt = sm.OLS(df_now.values, dummy.values).fit()
                result_dict[date][factor] = pd.DataFrame(
                    rslt.resid, index=ix, columns=[f'{factor}_indneu'])
        for date in tqdm(all_date_set):
            date_str = date.strftime('%Y-%m-%d')
            df = pd.concat([result_dict[date][factor] for factor in factors], axis=1)
            df.to_csv(RAW_DATA_ROOT
                      + f'factors/fundamental_factor_indneu_date/{date_str}.csv')

    def get_technical_factor(self):
        if not os.path.exists(RAW_DATA_ROOT + 'factors/tech_factor_ticker'):
            os.mkdir(RAW_DATA_ROOT + 'factors/tech_factor_ticker')
        for ticker in tqdm(self.all_stock_set):
            daily_df = pd.read_csv(RAW_DATA_ROOT + f'daily_data_adjust/{ticker}.csv',
                                   index_col=0, parse_dates=True)
            daily_df = daily_df.fillna(method='ffill')
            daily_df['close_pre_21d'] = daily_df['close'].shift(21)
            daily_df['close_pre_252d'] = daily_df['close'].shift(252)
            daily_df['return'] = daily_df['close'].pct_change()
            daily_df['abs_ret'] = np.abs(daily_df['return'].values)
            daily_df['daily_illiquidity'] = daily_df['abs_ret'] / (daily_df['amount'])
            daily_df['ILLIQ'] = pd.Series.rolling(
                daily_df['daily_illiquidity'], window=5).mean()

            weekly_df = daily_df.asfreq(freq='1w', method='ffill')
            weekly_df['REV'] = weekly_df['close'].pct_change()
            weekly_df['MOM'] = weekly_df['close_pre_21d'] / \
                weekly_df['close_pre_252d'] - 1
            weekly_df['MAX'] = daily_df['return'].resample('1W').max().ffill()
            weekly_df['MIN'] = daily_df['return'].resample('1W').min().ffill()
            weekly_df = weekly_df[['REV', 'MOM', 'MAX', 'MIN', 'ILLIQ']]
            weekly_df['ILLIQ'] = weekly_df['ILLIQ'].apply(lambda x: self._get_log(x))
            weekly_df.to_csv(RAW_DATA_ROOT + f'factors/tech_factor_ticker/{ticker}.csv')

    def calculate_RDV(self, x):
        return np.sum(np.square(x[1:]))

    def calculate_RSkew(self, x):
        return np.sum(np.power(x[1:], 3)) * np.sqrt(len(x[1:]))

    def calculate_RKurt(self, x):
        return np.sum(np.power(x[1:], 4)) * len(x[1:])

    def get_moment_factor(self):
        if not os.path.exists(RAW_DATA_ROOT + 'factors/moment_factor_ticker'):
            os.mkdir(RAW_DATA_ROOT + 'factors/moment_factor_ticker')
        for ticker in tqdm(self.all_stock_set):
            minute_df = pd.read_csv(RAW_DATA_ROOT + f'minute_data_new/{ticker}.csv',
                                    index_col=0, parse_dates=True)
            minute_df['date'] = [self._get_datetime(
                date) for date in minute_df.index.tolist()]
            minute_df_freq = minute_df.groupby('date').resample(
                '5min').last().ffill().dropna(axis=0)
            minute_df_freq = minute_df_freq.drop_duplicates(subset=['timestamp'])
            minute_df_freq['log_price'] = minute_df_freq['close'].apply(
                lambda x: self._get_log(x))

            minute_df_freq['log_return'] = minute_df_freq['log_price'] - \
                minute_df_freq['log_price'].shift(1)
            minute_df_freq['log_positive_return'] = [
                x if x > 0 else 0 for x in minute_df_freq['log_return'].tolist()]
            minute_df_freq['log_negative_return'] = [
                x if x < 0 else 0 for x in minute_df_freq['log_return'].tolist()]

            daily_result = pd.DataFrame(minute_df_freq['log_return'].groupby(
                'date').apply(lambda x: self.calculate_RDV(x)))
            daily_result.columns = ['RVol']
            daily_result['RSkew'] = minute_df_freq['log_return'].groupby(
                'date').apply(lambda x: self.calculate_RSkew(x))
            daily_result['RSkew'] = daily_result['RSkew'].values / \
                np.power(daily_result['RVol'].values, 3 / 2)

            daily_result['RKurt'] = minute_df_freq['log_return'].groupby(
                'date').apply(lambda x: self.calculate_RKurt(x))
            daily_result['RKurt'] = daily_result['RKurt'].values / \
                np.square(daily_result['RVol'].values)

            daily_result['semi_posi_variance'] = \
                minute_df_freq['log_positive_return'].groupby(
                    'date').apply(lambda x: self.calculate_RDV(x))
            daily_result['semi_nega_variance'] = \
                minute_df_freq['log_negative_return'].groupby(
                    'date').apply(lambda x: self.calculate_RDV(x))
            daily_result['RSJ'] = \
                (daily_result['semi_posi_variance']
                 - daily_result['semi_nega_variance']) / daily_result['RVol']

            weekly_result = daily_result.asfreq('1W', method='ffill')
            weekly_result['RVol'] = np.square(
                daily_result['RVol'].resample('1W').mean().ffill() * 252)
            weekly_result['RSKew'] = daily_result['RSkew'].resample('1W').mean().ffill()
            weekly_result['RKurt'] = daily_result['RKurt'].resample('1W').mean().ffill()
            weekly_result['RSJ'] = daily_result['RSJ'].resample('1W').mean().ffill()

            weekly_result = weekly_result[['RVol', 'RSkew', 'RKurt', 'RSJ']]
            weekly_result.to_csv(
                RAW_DATA_ROOT + f'factors/moment_factor_ticker/{ticker}.csv')
        return

    def calculate_port_return(self, port_list, month_begin, month_end,
                              daily_price_overall, daily_volume_overall):
        daily_price_df = daily_price_overall[port_list][month_begin: month_end]
        daily_volume_df = daily_volume_overall[port_list][month_begin: month_end]

        useful_days = np.count_nonzero(daily_volume_df.T.fillna(0).mean())
        average_volume = daily_volume_df.fillna(
            0).mean() * len(daily_volume_df) / useful_days
        threshold = np.percentile(average_volume, 10)
        condition_df = average_volume <= threshold
        drop_list = condition_df[condition_df].index.tolist()

        daily_price_df[daily_price_df <= 2] = 1.
        daily_price_df[drop_list] = 1.
        daily_return_df = daily_price_df.pct_change(axis=0)
        daily_return_df = daily_return_df.fillna(0)

        useful_tickers = np.count_nonzero(daily_return_df.mean())
        port_reward = daily_return_df.T.mean() * len(daily_return_df.T) / useful_tickers
        return port_reward

    def get_overall_info(self):
        daily_price_overall_list = []
        daily_volume_overall_list = []
        daily_price_adj_overall_list = []

        for ticker in tqdm(self.all_stock_set):
            daily_df = pd.read_csv(RAW_DATA_ROOT + f'daily_data/{ticker}.csv',
                                   index_col=0, parse_dates=True)
            tmp = daily_df.loc[:, ['close']].rename(columns={'close': ticker})
            daily_price_overall_list.append(tmp)
            tmp = daily_df.loc[:, ['volume']].rename(columns={'volume': ticker})
            daily_volume_overall_list.append(tmp)
            daily_adj_df = pd.read_csv(
                RAW_DATA_ROOT + f'daily_data_adjust/{ticker}.csv',
                index_col=0, parse_dates=True)
            tmp = daily_adj_df.loc[:, ['close']].rename(columns={'close': ticker})
            daily_price_adj_overall_list.append(tmp)
        daily_price_overall = pd.concat(daily_price_overall_list, axis=1)
        daily_volume_overall = pd.concat(daily_volume_overall_list, axis=1)
        daily_price_adj_overall = pd.concat(daily_price_adj_overall_list, axis=1)
        daily_price_overall.columns = self.all_stock_set
        daily_volume_overall.columns = self.all_stock_set
        daily_price_adj_overall.columns = self.all_stock_set
        daily_price_overall.to_csv(RAW_DATA_ROOT + 'info/daily_price_overall.csv')
        daily_volume_overall.to_csv(RAW_DATA_ROOT + 'info/daily_volume_overall.csv')
        daily_price_adj_overall.to_csv(
            RAW_DATA_ROOT + 'info/daily_price_adj_overall.csv')

    def get_speculate_factors(self):
        if not os.path.exists(RAW_DATA_ROOT + 'factors/speculate_factor_date'):
            os.mkdir(RAW_DATA_ROOT + 'factors/speculate_factor_date')
        all_date_set = pd.date_range(self.start_date, self.end_date, freq='1W')
        bm_df = pd.read_csv(RAW_DATA_ROOT + 'info/PB.csv',
                            index_col=0, parse_dates=True).T
        size_df = pd.read_csv(RAW_DATA_ROOT + 'info/SIZE.csv',
                              index_col=0, parse_dates=True).T
        market_df = self.benchmark_df
        shibor_df = self.shibor_df

        market_df = market_df.asfreq('1d')
        shibor_df = shibor_df.asfreq('1d')
        market_df['return'] = market_df['close'].pct_change()
        shibor_df['close'] = (shibor_df['close'] / 100).values
        market_df['excess_return'] = (market_df['return'] - shibor_df['close']).values

        all_trading_days = self.benchmark_df.index.tolist()

        daily_price_overall = pd.read_csv(
            RAW_DATA_ROOT + 'info/daily_price_overall.csv',
            index_col=0, parse_dates=True)
        daily_volume_overall = pd.read_csv(
            RAW_DATA_ROOT + 'info/daily_volume_overall.csv',
            index_col=0, parse_dates=True)
        path_price = RAW_DATA_ROOT + 'daily_data_adjust/'

        for date in tqdm(all_date_set):
            date_str = date.strftime("%Y-%m-%d")
            date_df = pd.DataFrame(
                columns=['IVOL', 'Price_Delay'], index=self.all_stock_set)
            cap_bm = pd.DataFrame(np.zeros(shape=(len(self.all_stock_set), 2)),
                                  columns=['market_cap', 'bm'],
                                  index=self.all_stock_set)
            cap_bm['market_cap'] = size_df[date].values
            cap_bm['bm'] = bm_df[date].values
            cap_bm = cap_bm.dropna(axis=0)
            cap_sort = cap_bm.sort_values(axis=0, ascending=True, by='market_cap')
            cap_small = cap_sort.iloc[:int(len(cap_sort) / 2), :]
            cap_big = cap_sort.iloc[int(len(cap_sort) / 2):2 * int(len(cap_sort) / 2),
                                    :]

            cap_small_sort = cap_small.sort_values(axis=0, ascending=True, by='bm')
            cap_big_sort = cap_big.sort_values(axis=0, ascending=True, by='bm')

            ports = {}
            port_list = ['port_sh', 'port_sm', 'port_sl',
                         'port_bh', 'port_bm', 'port_bl']

            ports[0] = list(cap_small_sort.index)[:int(len(cap_small_sort) * 0.3)]
            ports[1] = list(cap_small_sort.index)[
                int(len(cap_small_sort) * 0.3):int(len(cap_small_sort) * 0.7)]
            ports[2] = list(cap_small_sort.index)[-int(len(cap_small_sort) * 0.3):]
            ports[3] = list(cap_big_sort.index)[:int(len(cap_big_sort) * 0.3)]
            ports[4] = list(cap_big_sort.index)[
                int(len(cap_big_sort) * 0.3):int(len(cap_big_sort) * 0.7)]
            ports[5] = list(cap_big_sort.index)[-int(len(cap_big_sort) * 0.3):]

            day_count = 30
            lookback_end_idx = np.where(np.array(all_trading_days) <= date)[0][-1]
            lookback_end_date = all_trading_days[lookback_end_idx]
            lookback_start_idx = lookback_end_idx - day_count + 1
            lookback_start_date = all_trading_days[lookback_start_idx]
            date_index = all_trading_days[lookback_start_idx: lookback_end_idx + 1]

            ports_rewards = pd.DataFrame(
                np.ones(shape=(day_count, 6)), columns=port_list, dtype=np.float64)

            for i in range(6):
                ports_rewards[port_list[i]] = self.calculate_port_return(
                    ports[i], lookback_start_date, lookback_end_date,
                    daily_price_overall, daily_volume_overall).values

            reg_fac = pd.DataFrame(np.ones(shape=(day_count, 2)), columns=[
                                   'smb', 'hml'], index=date_index, dtype=np.float64)
            reg_fac['smb'] = ((ports_rewards['port_sh'] + ports_rewards['port_sm'] +
                               ports_rewards['port_sl']) / 3.0 -
                              (ports_rewards['port_bh'] + ports_rewards['port_bm'] +
                               ports_rewards['port_bl']) / 3.0).values
            reg_fac['hml'] = ((ports_rewards['port_sh']+ports_rewards['port_bh'])/2.0 -
                              (ports_rewards['port_sl'] + ports_rewards['port_bl'])/2.0
                              ).values

            mkt_cut = market_df[:date].index.tolist()[-4 * day_count:]
            mkt_3t = mkt_cut[: day_count]
            mkt_2t = mkt_cut[day_count: 2 * day_count]
            mkt_1t = mkt_cut[2 * day_count: 3 * day_count]

            reg_fac['mkt'] = market_df[lookback_start_date: date]['excess_return']
            reg_fac['mkt_1t'] = market_df[mkt_1t[0]: mkt_1t[-1]]['excess_return'].values
            reg_fac['mkt_2t'] = market_df[mkt_2t[0]: mkt_2t[-1]]['excess_return'].values
            reg_fac['mkt_3t'] = market_df[mkt_3t[0]: mkt_3t[-1]]['excess_return'].values
            reg_fac = reg_fac.fillna(0)

            x_1 = np.array(reg_fac.loc[:, ['mkt', 'smb', 'hml']])
            x_2 = np.array(reg_fac.loc[:, ['mkt', 'mkt_1t', 'mkt_2t', 'mkt_3t']])
            x_3 = np.array(reg_fac.loc[:, ['mkt', 'mkt_1t', 'mkt_2t', 'mkt_3t']])
            x_3[:, 1:] = 0.0

            spec_degree_list = []
            price_delay_list = []

            for stock in tqdm(self.all_stock_set):

                daily_price_df = pd.read_csv(
                    path_price + stock + '.csv',
                    index_col=0, parse_dates=True).asfreq('1d')
                daily_price_df['return'] = daily_price_df['close'].pct_change()
                index_ = daily_price_df.index
                daily_price_df['excess_return'] = \
                    (daily_price_df['return'] -
                     shibor_df['close'][index_[0]:index_[-1]]).values
                y = daily_price_df.fillna(
                    0)[lookback_start_date: date]['excess_return'].values

                if np.count_nonzero(np.isnan(y)) > 0 or len(y) != len(x_1):
                    spec_degree_list.append(np.float('nan'))
                    price_delay_list.append(np.float('nan'))
                    continue
                else:
                    spec_degree_list.append(1 - sm.OLS(y, x_1).fit().rsquared)
                    r1 = sm.OLS(y, x_2).fit().rsquared
                    r2 = sm.OLS(y, x_3).fit().rsquared
                    price_delay_list.append(1 - r2 / r1)

            date_df['IVOL'] = spec_degree_list
            date_df['Price_Delay'] = price_delay_list

            print('finished calculate speculate factors for date: '
                  + date.strftime('%Y-%m-%d'))
            date_df.to_csv(
                RAW_DATA_ROOT + f'factors/speculate_factor_date/{date_str}.csv')

    def get_comoment_factors(self):
        if not os.path.exists(RAW_DATA_ROOT + 'factors/comoment_factor_date'):
            os.mkdir(RAW_DATA_ROOT + 'factors/comoment_factor_date')
        daily_price_overall = pd.read_csv(
            RAW_DATA_ROOT + 'info/daily_price_overall.csv',
            index_col=0, parse_dates=True).asfreq('1d')
        daily_return_overall = daily_price_overall.pct_change(axis=0)
        all_date_set = pd.date_range(self.start_date, self.end_date, freq='1W')
        benchmark_df = self.benchmark_df.asfreq('1d')
        benchmark_df['return'] = benchmark_df['close'].pct_change()
        for date in tqdm(all_date_set):
            date_str = date.strftime("%Y-%m-%d")
            date_df = pd.DataFrame(index=self.all_stock_set, columns=['CSK', 'CKT'])
            pre_date = date + relativedelta(months=-1)

            daily_return_sub = daily_return_overall[pre_date: date]
            daily_return_sub = daily_return_sub - daily_return_sub.mean()
            daily_return_sub_square = daily_return_sub * daily_return_sub

            benchmark_sub = benchmark_df[pre_date: date]
            benchmark_sub['return'] = benchmark_sub['return'] - \
                benchmark_sub['return'].mean()
            benchmark_sub['return_square'] = np.square(benchmark_sub['return'].values)
            benchmark_sub['return_cubic'] = np.power(benchmark_sub['return'].values, 3)

            daily_return_sub_temp1 = (
                daily_return_sub.T * benchmark_sub['return_square'].values).T.mean()
            daily_return_sub_temp2 = (
                daily_return_sub.T * benchmark_sub['return_cubic'].values).T.mean()

            date_df['CSK'] = daily_return_sub_temp1.values / \
                (np.power(daily_return_sub_square.mean(), 1 / 2) *
                 benchmark_sub['return_square'].mean())

            date_df['CKT'] = daily_return_sub_temp2.values / \
                (np.power(daily_return_sub_square.mean(), 1 / 2) *
                 np.power(benchmark_sub['return_square'].mean(), 3 / 2))

            date_df.to_csv(
                RAW_DATA_ROOT + f'factors/comoment_factor_date/{date_str}.csv')

    def get_CGO_factor(self):
        if not os.path.exists(RAW_DATA_ROOT + 'factors/CGO_factor_ticker'):
            os.mkdir(RAW_DATA_ROOT + 'factors/CGO_factor_ticker')
        all_date_set = pd.date_range(self.start_date, self.end_date, freq='1W')
        turnover_list = []
        price_list = []
        for ticker in tqdm(self.all_stock_set):
            daily_df = pd.read_csv(
                RAW_DATA_ROOT + 'daily_data_adjust/{}.csv'.format(ticker),
                index_col=0, parse_dates=True)
            daily_df = daily_df.fillna(method='ffill')
            # transform %
            turnover = daily_df[['turn']].resample(
                'W').sum().rename(columns={'turn': ticker}) / 100
            price = daily_df[['close']].resample(
                'W').last().rename(columns={'close': ticker})
            turnover_list.append(turnover)
            price_list.append(price)
        turnover_df = pd.concat(turnover_list, axis=1).ffill()
        price_df = pd.concat(price_list, axis=1).ffill()
        start_i = int(np.where(turnover_df.index == all_date_set[0])[0])
        end_i = int(np.where(turnover_df.index == all_date_set[-1])[0])
        assert start_i > 156
        RP_df = pd.DataFrame(columns=turnover_df.columns, index=all_date_set)
        for i in tqdm(range(start_i, end_i + 1)):
            V = turnover_df.iloc[i - 156:i].copy().ffill().values
            P = price_df.iloc[i - 156:i].copy().ffill().values
            weight = np.zeros_like(P)
            for j in range(156):
                prod = 1
                for k in range(j):
                    prod = np.prod(1 - V[-k - 1:, :], axis=0)
                weight[155 - j] = V[155 - j] * prod
            k = np.nansum(weight, axis=0)
            RP = np.nansum(weight * P, axis=0) / k
            RP[np.nansum(V > 0, axis=0) < 156 * 0.5] = np.nan
            RP_df.iloc[i - start_i] = RP
        CGO_df = pd.DataFrame(
            1 - RP_df.values / price_df.iloc[start_i - 1:end_i].values,
            index=RP_df.index, columns=turnover_df.columns).astype(float)
        for ticker in tqdm(self.all_stock_set):
            tmp = CGO_df[[ticker]].rename(columns={ticker: 'CGO'})
            tmp.to_csv(RAW_DATA_ROOT
                       + f'factors/CGO_factor_ticker/{ticker}.csv')

    def get_external_factor(self):
        # VaR1 factor
        if not os.path.exists(RAW_DATA_ROOT + 'factors/VaR_factor_ticker'):
            os.mkdir(RAW_DATA_ROOT + 'factors/VaR_factor_ticker')
        all_date_set = pd.date_range(self.start_date, self.end_date, freq='1W')
        factor_df = pd.read_csv(
            RAW_DATA_ROOT + 'factors/factorVaR1.csv', index_col=0, parse_dates=True)
        factor_df = factor_df.loc[pd.to_datetime(
            self.start_date):pd.to_datetime(self.end_date)]
        factor_df_weekly = - factor_df.resample('W').mean().ffill() * np.sqrt(5)

        factor_df2 = pd.read_csv(
            RAW_DATA_ROOT + 'factors/factorVaR5.csv', index_col=0, parse_dates=True)
        factor_df2 = factor_df2.loc[pd.to_datetime(
            self.start_date):pd.to_datetime(self.end_date)]
        factor_df2_weekly = - factor_df2.resample('W').mean().ffill() * np.sqrt(5)

        for ticker in tqdm(self.all_stock_set):
            df1 = factor_df_weekly[[ticker]].rename(columns={ticker: 'VaR1'})
            df2 = factor_df2_weekly[[ticker]].rename(columns={ticker: 'VaR5'})
            pd.concat([df1, df2, pd.DataFrame(index=all_date_set)], axis=1).ffill().to_csv(
                RAW_DATA_ROOT + f'factors/VaR_factor_ticker/{ticker}.csv')
        # f-score factor
        if not os.path.exists(RAW_DATA_ROOT + 'factors/fscore_factor_ticker'):
            os.mkdir(RAW_DATA_ROOT + 'factors/fscore_factor_ticker')
        factor_df = pd.read_csv(
            RAW_DATA_ROOT + 'factors/fscore.csv', index_col=0, parse_dates=True)
        factor_df_weekly = factor_df.resample('W').last().ffill()
        factor_df_weekly = factor_df_weekly.loc[pd.to_datetime(
            self.start_date):pd.to_datetime(self.end_date)]
        for ticker in tqdm(self.all_stock_set):
            df = factor_df_weekly[[ticker]].rename(columns={ticker: 'FScore'})
            df.to_csv(RAW_DATA_ROOT + f'factors/fscore_factor_ticker/{ticker}.csv')

    def combine_factors(self):
        if not os.path.exists(RAW_DATA_ROOT + 'factors/combined_factors'):
            os.mkdir(RAW_DATA_ROOT + 'factors/combined_factors')
        all_date_set = pd.date_range(self.start_date, self.end_date, freq='1w')
        factor_dict_by_ticker = {}
        for ticker in tqdm(self.all_stock_set):
            # print ('get ticker df for ticker: ' + ticker)
            fundamental_df = pd.read_csv(RAW_DATA_ROOT + 'factors/fundamental_factor_ticker/{}.csv'.format(
                ticker), index_col=0, parse_dates=True)[['BM', 'ME', 'ROE', 'EP', 'BMROE']]
            technical_df = pd.read_csv(
                RAW_DATA_ROOT + 'factors/tech_factor_ticker/{}.csv'.format(ticker), index_col=0, parse_dates=True)
            moment_df = pd.read_csv(
                RAW_DATA_ROOT + 'factors/moment_factor_ticker/{}.csv'.format(ticker), index_col=0, parse_dates=True)
            cgo_df = pd.read_csv(
                RAW_DATA_ROOT + 'factors/CGO_factor_ticker/{}.csv'.format(ticker), index_col=0, parse_dates=True)
            var_df = pd.read_csv(
                RAW_DATA_ROOT + 'factors/VaR_factor_ticker/{}.csv'.format(ticker), index_col=0, parse_dates=True)
            fscore_df = pd.read_csv(
                RAW_DATA_ROOT + f'factors/fscore_factor_ticker/{ticker}.csv', index_col=0, parse_dates=True)
            incfscore_df = (fscore_df - fscore_df.shift(52)
                            ).rename(columns={'FScore': "IncFScore"})

            factor_df = pd.concat(
                [fundamental_df, technical_df, moment_df, cgo_df, var_df, fscore_df, incfscore_df], axis=1)
            factor_dict_by_ticker[ticker] = factor_df

        other_factors = factor_df.columns.tolist()

        for date in tqdm(all_date_set):
            # print ('combine factors for date: {}'.format(date.strftime('%Y-%m-%d')))
            spec_date_df = pd.read_csv(
                RAW_DATA_ROOT + 'factors/speculate_factor_date/{}.csv'.format(date.strftime('%Y-%m-%d')), index_col=0)
            comoment_date_df = pd.read_csv(
                RAW_DATA_ROOT + 'factors/comoment_factor_date/{}.csv'.format(date.strftime('%Y-%m-%d')), index_col=0)
            fundamental_indneu_df = pd.read_csv(
                RAW_DATA_ROOT + 'factors/fundamental_factor_indneu_date/{}.csv'.format(date.strftime('%Y-%m-%d')), index_col=0)

            date_df = pd.concat([spec_date_df, comoment_date_df,
                                 fundamental_indneu_df], axis=1)
            date_df_overall = pd.DataFrame(
                index=other_factors, columns=self.all_stock_set)

            for ticker in self.all_stock_set:
                try:
                    date_df_overall[ticker] = factor_dict_by_ticker[ticker].T[date].values
                except:
                    continue

            date_df_overall = pd.concat([date_df_overall.T, date_df], axis=1)
            date_df_overall.to_csv(
                RAW_DATA_ROOT + 'factors/combined_factors/{}.csv'.format(date.strftime('%Y-%m-%d')))


class DataCleanWeekly:
    def __init__(self):
        self.start_date = '2014-01-01'
        self.end_date = datetime.today().strftime('%Y-%m-%d')
        self.all_date_set = pd.date_range(self.start_date, self.end_date, freq='1W')

        self.all_stock_set = pd.read_csv('strategy/stocks/stock_universe/aqm_cn_stock.csv')['SECUCODE'].tolist()
        self.path_price = RAW_DATA_ROOT+'daily_data/'
        self.path_factor = RAW_DATA_ROOT+'factors/combined_factors/'

    def dynamic_can_regress_verify(self):
        # ------------------------------------------ #
        # Exclude stocks with long suspension period #
        # ------------------------------------------ #
        if not os.path.exists(RAW_DATA_ROOT + 'result/dynamic_exclude'):
            os.mkdir(RAW_DATA_ROOT + 'result/dynamic_exclude')
        num_stock = len(self.all_stock_set)
        num_date = len(self.all_date_set)
        array_all_true = np.full((num_stock, num_date), True, dtype=bool)
        can_regress_df = pd.DataFrame(array_all_true, index=self.all_stock_set, columns=self.all_date_set)

        for ticker in tqdm(self.all_stock_set):
            # print ('exclude cannot regress stock: {}'.format(ticker))
            price = pd.read_csv(self.path_price + ticker + '.csv', index_col=0, parse_dates=True, infer_datetime_format=True)
            price = price['close'].fillna(method='ffill')
            for date in self.all_date_set:
                sample_start = date - timedelta(days=30) # 30 for calendar days
                sample_price = price.ix[sample_start:date]
                sample_return = sample_price.pct_change().fillna(0)
                returnZeroCount = len(sample_return) - sample_return.astype(bool).sum()

                if returnZeroCount > 10 or np.isnan(price.ix[date:]).any():
                    can_regress_df[date].ix[ticker] = False

        self.can_regress_df = can_regress_df
        can_regress_df.to_csv(RAW_DATA_ROOT+'result/dynamic_exclude/can_regress.csv')
        return

    def dynamic_exclude_NA_stock(self):
        # ------------------------------------ #
        # Exclude stocks with many N/A factors #
        # ------------------------------------ #
        if not os.path.exists(RAW_DATA_ROOT + 'result/dynamic_exclude'):
            os.mkdir(RAW_DATA_ROOT + 'result/dynamic_exclude')
        check_factor_list = ['BM', 'ME', 'REV', 'MOM', 'MAX', 'MIN', 'ILLIQ', 'RSkew', 'RKurt', 'RSJ', 'IVOL']
        num_stock = len(self.all_stock_set)
        num_date = len(self.all_date_set)
        array_all_true = np.full((num_stock, num_date), True, dtype=bool)
        non_NA_df = pd.DataFrame(array_all_true, index=self.all_stock_set, columns=self.all_date_set)

        for date in tqdm(self.all_date_set):
            # print ('exclude NA stock: {}'.format(date))
            date_str = date.strftime('%Y-%m-%d')
            factor_df = pd.read_csv(self.path_factor + date_str + '.csv', index_col=0)

            me = factor_df['ME'].values
            me = me[~np.isnan(me)]
            cap_threshold = np.percentile(me, 10)

            for ticker in self.all_stock_set:
                check_factor = factor_df[check_factor_list].ix[ticker]
                NA_count = np.count_nonzero(np.isnan(check_factor))
                # if NA_count >= 4 or factor_df['P_CLOSE'].ix[ticker] < 2.0:
                if NA_count >= 4 or factor_df['ME'].ix[ticker] < cap_threshold:
                    non_NA_df[date].ix[ticker] = False

        self.non_NA_df = non_NA_df
        non_NA_df.to_csv(RAW_DATA_ROOT+'result/dynamic_exclude/non_NA.csv')
        return

    def dynamic_exclude_small_stock(self):
        # ----------------------------------------- #
        # Exclude stocks with many low price/volume #
        # ----------------------------------------- #
        if not os.path.exists(RAW_DATA_ROOT + 'result/dynamic_exclude'):
            os.mkdir(RAW_DATA_ROOT + 'result/dynamic_exclude')
        num_stock = len(self.all_stock_set)
        num_date = len(self.all_date_set)
        array_all_true = np.full((num_stock, num_date), True, dtype=bool)
        can_trade_df = pd.DataFrame(array_all_true, index=self.all_stock_set, columns=self.all_date_set)

        for ticker in tqdm(self.all_stock_set):
            # print('exclude cannot trade stock: {}'.format(ticker))
            price = pd.read_csv(self.path_price + ticker + '.csv', index_col=0, parse_dates=True, infer_datetime_format=True)
            volume = price['volume'].fillna(method='ffill')
            price = price['close'].fillna(method='ffill')

            for date in self.all_date_set:
                sample_start = date - timedelta(days=20)
                sample_price = price.ix[sample_start:date]
                sample_volume = volume.ix[sample_start:date]
                average_price = sample_price.mean()
                average_volume = sample_volume.mean()

                if average_price < 2. or average_volume < 1e6:
                    can_trade_df[date].ix[ticker] = False

        self.can_trade_df = can_trade_df
        can_trade_df.to_csv(RAW_DATA_ROOT+'result/dynamic_exclude/can_trade.csv')
        return

    def dynamic_exclude_extreme_factor(self):
        # ----------------------------------- #
        # Exclude stocks with extreme factors #
        # ----------------------------------- #
        if not os.path.exists(RAW_DATA_ROOT + 'result/dynamic_exclude'):
            os.mkdir(RAW_DATA_ROOT + 'result/dynamic_exclude')
        check_factor_list_1 = ['ILLIQ', 'RSkew', 'RKurt', 'RSJ', 'ROE']

        num_stock = len(self.all_stock_set)
        num_date = len(self.all_date_set)
        array_all_true = np.full((num_stock, num_date), True, dtype=bool)
        non_extreme_df = pd.DataFrame(array_all_true, index=self.all_stock_set, columns=self.all_date_set)

        for date in tqdm(self.all_date_set):
            # print ('exclude extreme stock: {}'.format(date))
            date_str = date.strftime('%Y-%m-%d')
            factor_df = pd.read_csv(self.path_factor + date_str + '.csv', index_col=0)
            for factor in check_factor_list_1:

                l_quantile = factor_df[factor].quantile(0.005)
                u_quantile = factor_df[factor].quantile(0.995)
                l_index = factor_df[factor][factor_df[factor] < l_quantile].index.values
                u_index = factor_df[factor][factor_df[factor] > u_quantile].index.values

                l_index = list(set(l_index).intersection(self.all_stock_set))
                u_index = list(set(u_index).intersection(self.all_stock_set))
                non_extreme_df[date].ix[l_index] = False
                non_extreme_df[date].ix[u_index] = False

        self.non_extreme_df = non_extreme_df
        non_extreme_df.to_csv(RAW_DATA_ROOT+'result/dynamic_exclude/non_extreme_df.csv')
        return

    def dynamic_exclude_combine(self):
        # --------------------------------------------------------- #
        # Exclude stocks satisfying one of the excluding conditions #
        # --------------------------------------------------------- #
        if not os.path.exists(RAW_DATA_ROOT + 'result/exclusive_factors'):
            os.mkdir(RAW_DATA_ROOT + 'result/exclusive_factors')
        data_exclusion_combine = {}

        self.can_regress_df = pd.read_csv(RAW_DATA_ROOT+'result/dynamic_exclude/can_regress.csv', index_col=0)
        self.non_NA_df = pd.read_csv(RAW_DATA_ROOT+'result/dynamic_exclude/non_NA.csv', index_col=0)
        self.can_trade_df = pd.read_csv(RAW_DATA_ROOT+'result/dynamic_exclude/can_trade.csv', index_col=0)
        self.non_extreme_df = pd.read_csv(RAW_DATA_ROOT+'result/dynamic_exclude/non_extreme_df.csv', index_col=0)

        effective_set = self.can_regress_df & self.can_trade_df & self.non_extreme_df & self.non_NA_df
        effective_set.to_csv(RAW_DATA_ROOT+'result/dynamic_exclude/combined.csv')
        for date in tqdm(self.all_date_set):
            # print ('combine exclusion: {}'.format(date))
            date_str = date.strftime('%Y-%m-%d')
            factor_df = pd.read_csv(self.path_factor + date_str + '.csv', index_col=0)
            effective_index = effective_set.index[effective_set[date_str] == True]
            data_exclusion_combine[date] = factor_df.ix[effective_index]

        self.data_exclusion_combine = data_exclusion_combine
        
        for date in self.all_date_set:
            date_str = date.strftime('%Y-%m-%d')
            data_exclusion = data_exclusion_combine[date]

            for factor in data_exclusion.columns.tolist():
                # fill inf, -inf
                factor_list = np.array(data_exclusion[factor])
                factor_list[np.isinf(factor_list)] = np.nan
                factor_list[np.isneginf(factor_list)] = np.nan
                data_exclusion[factor] = factor_list

            data_exclusion.to_csv(RAW_DATA_ROOT+'result/exclusive_factors/' + date_str + '.csv')
        return
    
    def dynamic_standardize_factor(self):
        # ------------------- #
        # Standardize factors #
        # ------------------- #
        if not os.path.exists(RAW_DATA_ROOT + 'result/standardized_factors'):
            os.mkdir(RAW_DATA_ROOT + 'result/standardized_factors')
        standardized_data_dict = {}

        for date in tqdm(self.all_date_set):
            # print ('standardize factor: {}'.format(date))
            date_str = date.strftime('%Y-%m-%d')
            factor_df = pd.read_csv(RAW_DATA_ROOT+'result/exclusive_factors/' + date_str + '.csv', index_col=0)
            factors_list = factor_df.columns.values

            for factor in factors_list:
                factor_df[factor] = (factor_df[factor] - factor_df[factor].mean()) / factor_df[factor].std()
            standardized_data_dict[date] = factor_df.fillna(0)
            standardized_data_dict[date].to_csv(RAW_DATA_ROOT+'result/standardized_factors/' + date_str + '.csv')
        return 


class FactorAnalysis():
    def __init__(self):
        self.start_date = '2014-01-01'
        self.end_date = datetime.today().strftime('%Y-%m-%d')
        self.all_stock_set = pd.read_csv('strategy/stocks/stock_universe/aqm_cn_stock.csv')['SECUCODE'].tolist()

        self.benchmark_df = pd.read_csv(
            '//192.168.9.170/share/alioss/1_StockStrategy/data/CSI300TR.csv',
            index_col=0, parse_dates=True)
        self.shibor_df = pd.read_csv(
            '//192.168.9.170/share/alioss/1_StockStrategy/data/SHIBOR.csv',
            index_col=0, parse_dates=True)
        self.n_portfolio = 10

    def single_sort_factor(self):
        if not os.path.exists(RAW_DATA_ROOT + 'result/single_sort_portfolio'):
            os.mkdir(RAW_DATA_ROOT + 'result/single_sort_portfolio')
        n_portfolio = self.n_portfolio
        
        all_date_set = pd.date_range(self.start_date, self.end_date, freq='W')
        
        industry_dict = {date: pd.read_csv(RAW_DATA_ROOT+f'info/industry/{date.strftime("%Y-%m-%d")}.csv', index_col=0) for date in all_date_set}
        result_dict = defaultdict(dict)
        
        weekly_price_overall = pd.read_csv(RAW_DATA_ROOT+'info/daily_price_adj_overall.csv', index_col=0, parse_dates=True).resample('W').last()
        weekly_return_overall = weekly_price_overall.pct_change(axis=0).shift(-1).loc[self.start_date:self.end_date] # align date
        
        factor_dict_by_date = {}
        for date in all_date_set:
            date_str = date.strftime('%Y-%m-%d')
            date_df = pd.read_csv(RAW_DATA_ROOT+'result/exclusive_factors/{}.csv'.format(date_str), index_col=0)
            factor_dict_by_date[date_str] = date_df
        
        factor_list = date_df.columns.tolist()
        for date in tqdm(all_date_set):
            date_str = date.strftime('%Y-%m-%d')
            factor_df = factor_dict_by_date[date_str]
            df = pd.DataFrame(index=factor_df.index)
            df['return'] = weekly_return_overall.loc[date_str].loc[factor_df.index]
            for factor in factor_list:
                if factor in ['FScore', 'FScore_indneu', 'IncFScore']:
                    n_portfolio = 3
                else:
                    n_portfolio = self.n_portfolio
                one_factor_df = factor_df[[factor]]
                if np.isnan(one_factor_df.values).all():
                    factor_array = np.zeros(len(one_factor_df))
                else:
                    factor_array = one_factor_df.values[~np.isnan(one_factor_df.values)]
                split_points = np.array([np.percentile(factor_array, (i+1) * 100/ n_portfolio) for i in range(n_portfolio-1)])
                one_factor_df = one_factor_df.fillna(one_factor_df.mean())
                get_group_ = partial(get_group, split_points=split_points)
                df[factor] = one_factor_df[factor]
                df[f'{factor}_group'] = one_factor_df[factor].apply(get_group_)
            # rank within industry
            industry_df = industry_dict[date].copy()
            ix = [i for i in factor_df.index if i in industry_df.index]
            industry_df = industry_df.loc[ix]
            # industry_df[['BM', 'ME', 'ROE']] = factor_df.loc[ix, ['BM', 'ME', 'ROE']]
            industry_group_df_list = []
            industry_list = list(set(industry_df['Industry']))
            for industry in industry_list:
                ind_df = pd.DataFrame(index=industry_df.loc[industry_df.loc[:,'Industry']==industry].index)
                
                for factor in ['BM', 'ME', 'ROE', 'BMROE', 'EP', 'FScore']:
                    if factor == 'FScore':
                        n_portfolio = 3
                    else:
                        n_portfolio = self.n_portfolio
                    one_factor_df = factor_df.loc[ind_df.index, [factor]]
                    one_factor_df = one_factor_df.fillna(one_factor_df.mean())
                    split_points = np.array([np.percentile(one_factor_df.values, (i+1) * 100/ n_portfolio) for i in range(n_portfolio-1)])
                    get_group_ = partial(get_group, split_points=split_points)
                    ind_df[f'{factor}IND'] = one_factor_df[factor]
                    ind_df[f'{factor}IND_group'] = one_factor_df[factor].apply(get_group_)
                industry_group_df_list.append(ind_df)
            industry_group_df = pd.concat(industry_group_df_list, axis=0)
            df = pd.concat([df, industry_group_df], axis=1)
            
            df.to_csv(RAW_DATA_ROOT+'result/single_sort_portfolio/{}.csv'.format(date_str))
        return

    def single_sort_factor_summary(self):
        if not os.path.exists(RAW_DATA_ROOT + 'result/single_sort_portfolio_summary'):
            os.mkdir(RAW_DATA_ROOT + 'result/single_sort_portfolio_summary')
        all_date_set = pd.date_range(self.start_date, self.end_date, freq='W')
        factor_list = pd.read_csv(RAW_DATA_ROOT+'result/exclusive_factors/{}.csv'.format(all_date_set[0].strftime('%Y-%m-%d')), index_col=0).columns.tolist()
        factor_list = factor_list + ['BMIND', 'MEIND', 'ROEIND', 'BMROEIND', 'EPIND', 'FScoreIND']
        benchmark_return = self.benchmark_df.resample('W').last().pct_change(axis=0).shift(-1).loc[self.start_date:self.end_date].values.flatten()
        risk_free_rate = self.shibor_df.resample('W').last().loc[self.start_date:self.end_date].values[-1, 0]/100
        
        summary = defaultdict(dict)
        for date in tqdm(all_date_set):
            date_str = date.strftime('%Y-%m-%d')
            df = pd.read_csv(RAW_DATA_ROOT+'result/single_sort_portfolio/{}.csv'.format(date_str), index_col=0)
            dfix_in_all_set = np.array([self.all_stock_set.index(i) for i in df.index])
            for factor in factor_list:
                if factor in ['FScore', 'FScoreIND', 'FScore_indneu', 'IncFScore']:
                    n_portfolio = 3
                else:
                    n_portfolio = self.n_portfolio
                one_df = df.groupby(f'{factor}_group').mean()
                summary[factor]['columns'] = one_df.columns.tolist()
                one_list = summary[factor].get('value', [])
                one_holding = summary[factor].get('holding', [])
                if len(one_df) == n_portfolio:
                    one_list.append(one_df.values.tolist())
                    holding = df[f'{factor}_group'].values
                else:
                    average = np.ones((n_portfolio, len(one_df.columns))) * df[[i for i in df.columns if i != factor]].mean().values
                    one_list.append(average.tolist())
                    holding = np.ones(len(df)) * (-1)
                all_holding = np.ones(len(self.all_stock_set)) * np.nan
                all_holding[dfix_in_all_set] = holding
                one_holding.append(all_holding.tolist())
                summary[factor]['holding'] = one_holding
                summary[factor]['value'] = one_list
            
        for factor in tqdm(factor_list):
            if factor in ['FScore', 'FScoreIND', 'FScore_indneu', 'IncFScore']:
                n_portfolio = 3
            else:
                n_portfolio = self.n_portfolio
            value = np.array(summary[factor]['value'])
            columns = summary[factor]['columns']
            holding = np.array(summary[factor]['holding'])
            
            rtns = value[:, :, 0]
            pd.DataFrame(rtns, index=all_date_set, columns=np.arange(n_portfolio)).cumsum().to_csv(RAW_DATA_ROOT+f'result/single_sort_portfolio_summary/{factor}_return.csv')
            metrics_list = []
            for i in range(n_portfolio):
                rtn = rtns[:, i]
                annualized_rtn = calc_annualized_return(rtn)
                annualized_mrtn = calc_annualized_return(benchmark_return)
                annualized_volatility = calc_annualized_volatility(rtn)
                sharpe_ratio = calc_sharpe_ratio(annualized_rtn, annualized_volatility, risk_free_rate)
                information_ratio = calc_information_ratio(rtn, benchmark_return)
                maximum_drawdown = calc_mdd(rtn)
                win_prob = calc_win_prob(rtn)
                beta = calc_beta(rtn, benchmark_return)
                alpha = calc_alpha(annualized_rtn, annualized_mrtn, beta, risk_free_rate)
                turnover = calc_turnover(holding, i, None)
                metrics_list.append([annualized_rtn, annualized_volatility, sharpe_ratio, information_ratio, maximum_drawdown, win_prob, alpha, beta, turnover])
            # 1-10
            rtn = rtns[:, 0] - rtns[:, n_portfolio-1]
            annualized_rtn = calc_annualized_return(rtn)
            annualized_mrtn = calc_annualized_return(benchmark_return)
            annualized_volatility = calc_annualized_volatility(rtn)
            sharpe_ratio = calc_sharpe_ratio(annualized_rtn, annualized_volatility, risk_free_rate)
            information_ratio = np.nan
            maximum_drawdown = calc_mdd(rtn)
            win_prob = calc_win_prob(rtn)
            beta = calc_beta(rtn, benchmark_return)
            alpha = calc_alpha(annualized_rtn, annualized_mrtn, beta, risk_free_rate)
            turnover = calc_turnover(holding, 0, n_portfolio-1)
            metrics_list.append([annualized_rtn, annualized_volatility, sharpe_ratio, information_ratio, maximum_drawdown, win_prob, alpha, beta, turnover])
            # 2-9
            rtn = rtns[:, 1] - rtns[:, n_portfolio-2]
            annualized_rtn = calc_annualized_return(rtn)
            annualized_mrtn = calc_annualized_return(benchmark_return)
            annualized_volatility = calc_annualized_volatility(rtn)
            sharpe_ratio = calc_sharpe_ratio(annualized_rtn, annualized_volatility, risk_free_rate)
            information_ratio = np.nan
            maximum_drawdown = calc_mdd(rtn)
            win_prob = calc_win_prob(rtn)
            beta = calc_beta(rtn, benchmark_return)
            alpha = calc_alpha(annualized_rtn, annualized_mrtn, beta, risk_free_rate)
            turnover = calc_turnover(holding, 1, n_portfolio-2)
            metrics_list.append([annualized_rtn, annualized_volatility, sharpe_ratio, information_ratio, maximum_drawdown, win_prob, alpha, beta, turnover])
            
            pd.DataFrame(metrics_list, columns=['return p.a.', 'vol p.a.', 'sharpe_ratio', 'information_ratio', 'maximum_drawdown', 'win_prob', 'alpha', 'beta', 'turnover'], index=[f'{factor}_{i}' for i in range(n_portfolio)]+[f'{factor}_0-{n_portfolio-1}', f'{factor}_1-{n_portfolio-2}']).to_csv(RAW_DATA_ROOT+f'result/single_sort_portfolio_summary/{factor}_metrics.csv')
            
            mean_rank = np.mean(value, axis=0)
            pd.DataFrame(mean_rank, index=np.arange(n_portfolio), columns=columns)[[i for i in columns if 'group' in i]].rename(columns={i: i[:-6] for i in columns if 'group' in i}).to_csv(RAW_DATA_ROOT+f'result/single_sort_portfolio_summary/{factor}_rank.csv')
        return


def get_group(v, split_points):
    for i in range(len(split_points)):
        if v <= split_points[i]:
            return i
    return len(split_points)

def calc_annualized_return(rtn):
    return np.mean(rtn) * 52

def calc_annualized_volatility(rtn):
    return np.std(rtn) * np.sqrt(52)

def calc_sharpe_ratio(annualized_rtn, annualized_volatility, rf):
    return (annualized_rtn-rf)/annualized_volatility

def calc_information_ratio(rtn, mrtn):
    return (np.mean(rtn)-np.mean(mrtn))/np.std(rtn-mrtn)*np.sqrt(52)

def calc_mdd(rtn):
    cumrtn = np.cumsum(rtn)
    hwm = np.maximum.accumulate(cumrtn)
    dd = hwm - cumrtn
    return np.max(dd)

def calc_win_prob(rtn):
    return len(rtn[rtn>0])/len(rtn)

def calc_beta(rtn, mrtn):
    return (np.cov(rtn, mrtn)/np.var(mrtn))[0, 1]

def calc_alpha(annualized_rtn, annualized_mrtn, beta, rf):
    return annualized_rtn - rf - beta * (annualized_mrtn - rf)

def calc_turnover(holding, long_group, short_group=None):
    # long
    long_holding = holding.copy()
    long_holding[long_holding==-1] = long_group
    long_weight = (long_holding == long_group).astype(int)
    # short
    if short_group is not None:
        short_holding = holding.copy()
        short_holding[short_holding==-1] = short_group
        short_weight = (short_holding == short_group).astype(int) * (-1)
    else:
        short_weight = np.zeros_like(long_weight)
    weight = long_weight-short_weight
    weight = weight / np.sum(np.abs(weight), axis=1).reshape((-1, 1))
    return np.mean(np.sum(np.abs(weight[1:]-weight[:-1]), axis=1))


class SignalGenerator():
    def __init__(self):
        self.start_date = '2014-01-01'
        self.end_date = datetime.today().strftime('%Y-%m-%d')
        self.all_date_set = pd.date_range(self.start_date, self.end_date, freq='W')
        self.all_stock_set = pd.read_csv(
            'strategy/stocks/stock_universe/aqm_cn_stock.csv')['SECUCODE'].tolist()
        
        self.benchmark_df = pd.read_csv(
            '//192.168.9.170/share/alioss/1_StockStrategy/data/CSI300TR.csv',
            index_col=0, parse_dates=True)
        self.benchmark_return = self.benchmark_df.resample('W').last().pct_change(axis=0).shift(-1).loc[self.start_date:self.end_date].values.flatten()
        
        self.portfolio_summary = {}
        for date in tqdm(self.all_date_set):
            date_str=date.strftime('%Y-%m-%d')
            df = pd.read_csv(RAW_DATA_ROOT+f'result/single_sort_portfolio/{date_str}.csv', index_col=0)
            self.portfolio_summary[date_str] = df
        
        self.industry_summary = {}
        for date in tqdm(self.all_date_set):
            date_str = date.strftime('%Y-%m-%d')
            df = pd.read_csv(RAW_DATA_ROOT+f'info/industry/{date_str}.csv', index_col=0)
            self.industry_summary[date_str] = dict(zip(df.index, df['Industry']))
        
    def EPIND_FSCORE(self):
        prohibit_dict = {}
        for date in self.all_date_set:
            date_str = date.strftime("%Y-%m-%d")
            df = portfolio_summary[date_str]
            aList = []
            aList = aList + df.loc[df.loc[:, f'EPIND_group']<=7].index.tolist()
            aList = aList + df.loc[df.loc[:, f'FScore_group']<=1].index.tolist()
            aList = list(set(aList))
            prohibit_dict[date_str] = aList
        return_df, return_pass_df, n_pass_df, turnover_pass_df, holding_pass, industry_count_df= run('ME', prohibit_dict)
        z = pd.DataFrame(np.sum(holding_pass, axis=1), index=self.all_date_set, columns=self.all_stock_set)
        z.to_csv('//192.168.9.170/share/alioss/1_StockStrategy/data/signal/EPIND_FSCORE.csv')
        
        
def agg_return(df, factor_name, n_portfolio=10):
    dfix_in_all_set = np.array([all_stock_set.index(i) for i in df.index])
    mean_return = df['return'].mean()
    one_df = df.groupby(f'{factor_name}_group').mean()
    if len(one_df) == 1:
        group_return = np.ones(n_portfolio) * 0
    elif len(one_df) < n_portfolio:
        group_return = np.ones(n_portfolio) * 0
        group_return[one_df.index.astype(int)] = one_df['return'].values
    else:
        group_return = one_df['return'].values
    one_df = df.groupby(f'{factor_name}_group').count()
    if len(one_df) == 1:
        group_n = np.ones(n_portfolio) * 0
    elif len(one_df) < n_portfolio:
        group_n = np.ones(n_portfolio) * 0
        group_n[one_df.index.astype(int)] = one_df['return'].values
    else:
        group_n = one_df['return'].values
        
    group = df[f'{factor_name}_group'].values
    holding = np.zeros((n_portfolio, len(all_stock_set)))
    if len(one_df) > 1: # 若只有1个分组, 则认为所有组都没有holding
        for i in range(n_portfolio):
            tmp = (group == i).astype(int)
            if np.sum(np.abs(tmp)) > 0:
                holding[i, dfix_in_all_set] =  tmp/np.sum(np.abs(tmp))  
    return mean_return, group_return, len(df), group_n, holding


def run(factor_name, prohibit_dict, top=None):
    mean_return_list = []
    group_return_list = []
    all_n_list = []
    group_n_list = []
    holding_list = []
    mean_return_pass_list = []
    group_return_pass_list = []
    all_n_pass_list = []
    group_n_pass_list = []
    holding_pass_list = []
    for date in tqdm(all_date_set):
        date_str = date.strftime('%Y-%m-%d')
        prohibit_list_now = prohibit_dict[date_str]
        df = portfolio_summary[date_str].copy()
        symbols = df.index.tolist()
        symbols_pass = [i for i in symbols if i not in prohibit_list_now]
        symbols_reject = [i for i in symbols if i in prohibit_list_now]
        df_pass = df.loc[symbols_pass]
        df_reject = df.loc[symbols_reject]
        mean_return, group_return, all_n, group_n, holding = agg_return(df, factor_name, 10)
        mean_return_list.append(mean_return)
        group_return_list.append(group_return.tolist())
        all_n_list.append(all_n)
        group_n_list.append(group_n.tolist())
        holding_list.append(holding.tolist())
        
        mean_return_pass, group_return_pass, all_n_pass, group_n_pass, holding_pass = agg_return(df_pass, factor_name, 10)
        mean_return_pass_list.append(mean_return_pass)
        group_return_pass_list.append(group_return_pass.tolist())
        all_n_pass_list.append(all_n_pass)
        group_n_pass_list.append(group_n_pass.tolist())
        holding_pass_list.append(holding_pass.tolist())
    return_df = pd.concat([pd.DataFrame(np.array(group_return_list), index=all_date_set),
                          pd.DataFrame({'all':mean_return_list}, index=all_date_set)],
                          axis=1)
    return_pass_df = pd.concat([pd.DataFrame(np.array(group_return_pass_list), index=all_date_set),
                                pd.DataFrame({'all_pass':mean_return_pass_list,
                                             'all':mean_return_list,
                                             'CSI300':benchmark_return}, index=all_date_set)],
                               axis=1)
    n_pass_df = pd.concat([pd.DataFrame(group_n_pass_list, index=all_date_set),
                          pd.DataFrame({'not_pass':[all_n_list[i]-all_n_pass_list[i] for i in range(len(all_n_list))]}, index=all_date_set)],
                         axis=1)
    holding = np.array(holding_list)
    holding_pass = np.array(holding_pass_list)
    turnover = np.zeros((holding_pass.shape[0], holding_pass.shape[1]))
    turnover[1:] = np.sum(np.abs(holding_pass[1:]-holding_pass[:-1]), axis=2)
    turnover[0] = np.sum(holding_pass[0, :, :], axis=1)
    
    tmp = (np.sum(holding_pass, axis=1)>0).astype(int)
    sum_ = np.sum(np.abs(tmp), axis=1)
    sum_[sum_==0] = 1
    all_holding = tmp / sum_.reshape((-1, 1))
    all_turnover = np.zeros(all_holding.shape[0])
    all_turnover[1:] = np.sum(np.abs(all_holding[1:,:]-all_holding[:-1,:]),axis=1)
    all_turnover[0] = np.sum(np.abs(all_holding[0]))
    
    turnover_pass_df = pd.concat([pd.DataFrame(turnover, index=all_date_set),
                                 pd.DataFrame({'all_pass':all_turnover}, index=all_date_set)], axis=1)
    # plots(return_pass_df, n_pass_df)
    
    agg_holding = np.sum(holding_pass.astype(bool), axis=1).astype(bool)
    industry_count_df_list = []
    for i in range(agg_holding.shape[0]):
        date_str = all_date_set[i].strftime('%Y-%m-%d')
        df = pd.DataFrame(Counter([industry_summary[date_str].get(i, '未知') for i in np.array(all_stock_set)[agg_holding[i]]]), index=[all_date_set[i]])
        industry_count_df_list.append(df)
    industry_count_df = pd.concat(industry_count_df_list, axis=0).fillna(0)
    # industry_count_df.plot.area()

    return return_df, return_pass_df, n_pass_df, turnover_pass_df, holding_pass, industry_count_df

if __name__ == '__main__':
    equity_weekly_factor = EquityWeeklyFactor()

    # DOWNLOAD DATA
    # =============================================
    # equity_weekly_factor.get_equity_minute_data()
    # equity_weekly_factor.get_equity_daily_data()
    equity_weekly_factor.get_equity_daily_data_adjust()
    equity_weekly_factor.get_fundamental_data()
    equity_weekly_factor.get_fundamental_data_2()
    equity_weekly_factor.parse_industry_data()

    # PROCESS DATA
    # =============================================
    # equity_weekly_factor.get_external_factor()
    # equity_weekly_factor.get_fundamental_factor()
    # equity_weekly_factor.calc_industry_neutral_factor()
    # equity_weekly_factor.get_technical_factor()
    # equity_weekly_factor.get_moment_factor()
    # equity_weekly_factor.get_overall_info()
    # equity_weekly_factor.get_speculate_factors()
    # equity_weekly_factor.get_comoment_factors()
    # equity_weekly_factor.get_CGO_factor()
    # equity_weekly_factor.combine_factors()

    # data_clean_process = DataCleanWeekly()
    # data_clean_process.dynamic_can_regress_verify()
    # data_clean_process.dynamic_exclude_NA_stock()
    # data_clean_process.dynamic_exclude_extreme_factor()
    # data_clean_process.dynamic_exclude_small_stock()
    # data_clean_process.dynamic_exclude_combine()
    # data_clean_process.dynamic_standardize_factor()
    
    # factor_analysis = FactorAnalysis()
    # factor_analysis.single_sort_factor()
    # factor_analysis.single_sort_factor_summary()
    
    signal_generator = SignalGenerator()
    signal_generator.EPIND_FSCORE()