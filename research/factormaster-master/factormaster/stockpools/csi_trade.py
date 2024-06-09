import pandas as pd
import os
from datetime import timedelta
import time
from tqdm import tqdm


SSE_CALENDAR = pd.read_csv('data/TRADEDATE.csv', index_col=0, parse_dates=True)
START_DATE = pd.to_datetime('2010-01-01')
END_DATE = pd.to_datetime('2020-10-31')


def _concat_by_col(df_list):
    df = pd.concat(df_list, axis=1, sort=True)
    return df


def generate_csi_trade():
    for n in [300, 500, 800]:
        csi_component = pd.read_csv(f'data/stockpools/CSI{n}_COMPONENT.csv', index_col=0, parse_dates=True)
        csi_component = pd.concat([csi_component, pd.DataFrame(index=SSE_CALENDAR.loc[START_DATE: END_DATE].index)], axis=1).ffill().fillna(0)
        csi_component = csi_component.loc[START_DATE: END_DATE]
        df_list = []
        for name in tqdm(csi_component.columns):
            try:
                df = pd.read_csv(f'data/features/raw/volume/{name}.csv', index_col=0, parse_dates=True)
                df.columns = [name]
            except:
                df = pd.DataFrame(columns=[name])
            df_list.append(df)
        volume = _concat_by_col(df_list)
        volume = pd.concat([volume, pd.DataFrame(index=csi_component.index)], axis=1)
        volume.sort_index(inplace=True)
        volume = volume.loc[~volume.index.duplicated(keep='first')]
        volume = volume.fillna(0)
        csi_component_trade = csi_component * volume.astype(bool).astype(int)
        
        df_list = []
        for name in tqdm(csi_component.columns):
            try:
                df = pd.read_csv(f'data/features/raw/adjust_ohlc/{name}.csv', index_col=0, parse_dates=True)
                # df = pd.read_csv(f'data/features/raw/ohlc/{name}.csv', index_col=0, parse_dates=True)
            except:
                df = pd.DataFrame(columns=['adjust_open', 'adjust_high', 'adjust_low', 'adjust_close'])
                # df = pd.DataFrame(columns=['open', 'high', 'low', 'close'])
            df['zero_amplitude'] = (df.max(axis=1) == df.min(axis=1))
            df['rtn'] = df['adjust_close'].pct_change()
            # df['rtn'] = df['close'].pct_change()
            df['bs_limit'] = (df['rtn'] > 0.095) | (df['rtn'] < -0.095)
            df_list.append((df['zero_amplitude'].astype(bool) & df['bs_limit'].astype(bool)).to_frame(name))
        bslimit = _concat_by_col(df_list)
        bslimit = pd.concat([bslimit, pd.DataFrame(index=csi_component.index)], axis=1)
        bslimit.sort_index(inplace=True)
        bslimit = bslimit.loc[~bslimit.index.duplicated(keep='first')]
        bslimit = bslimit.fillna(0)
        bslimit = (~bslimit.astype(bool)).astype(int)
        csi_component_trade = csi_component_trade * bslimit
            
        csi_component_trade.loc[SSE_CALENDAR.loc[START_DATE: END_DATE].index].to_csv(f'data/stockpools/CSI{n}_TRADE_COMPONENT.csv')


if __name__ == '__main__':
    generate_csi_trade()