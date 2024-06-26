# -*- coding: utf-8 -*-
"""
Module for building a complete dataset from local directory with csv files.
"""
import os
import sys

from logbook import Logger, StreamHandler
from numpy import empty
from pandas import DataFrame, read_csv, Index, Timedelta, NaT
from trading_calendars import register_calendar_alias
import pytz
import pandas as pd

from zipline.utils.cli import maybe_show_progress

from zipline.data.bundles import core as bundles


handler = StreamHandler(sys.stdout, format_string=" | {record.message}")
logger = Logger(__name__)
logger.handlers.append(handler)


def csvaqm_equities(tframes=None, csvdir=None):
    """
    Generate an ingest function for custom data bundle
    This function can be used in ~/.zipline/extension.py
    to register bundle with custom parameters, e.g. with
    a custom trading calendar.

    Parameters
    ----------
    tframes: tuple, optional
        The data time frames, supported timeframes: 'daily' and 'minute'
    csvdir : string, optional, default: CSVDIR environment variable
        The path to the directory of this structure:
        <directory>/<timeframe1>/<symbol1>.csv
        <directory>/<timeframe1>/<symbol2>.csv
        <directory>/<timeframe1>/<symbol3>.csv
        <directory>/<timeframe2>/<symbol1>.csv
        <directory>/<timeframe2>/<symbol2>.csv
        <directory>/<timeframe2>/<symbol3>.csv

    Returns
    -------
    ingest : callable
        The bundle ingest function

    Examples
    --------
    This code should be added to ~/.zipline/extension.py
    .. code-block:: python
       from zipline.data.bundles import csvdir_equities, register
       register('custom-csvdir-bundle',
                csvdir_equities(["daily", "minute"],
                '/full/path/to/the/csvdir/directory'))
    """

    return CSVDIRBundle(tframes, csvdir).ingest


class CSVDIRBundle:
    """
    Wrapper class to call csvdir_bundle with provided
    list of time frames and a path to the csvdir directory
    """

    def __init__(self, tframes=None, csvdir=None):
        self.tframes = tframes
        self.csvdir = csvdir

    def ingest(self,
               environ,
               asset_db_writer,
               minute_bar_writer,
               daily_bar_writer,
               adjustment_writer,
               calendar,
               start_session,
               end_session,
               cache,
               show_progress,
               output_dir):

        csvdir_bundle(environ,
                      asset_db_writer,
                      minute_bar_writer,
                      daily_bar_writer,
                      adjustment_writer,
                      calendar,
                      start_session,
                      end_session,
                      cache,
                      show_progress,
                      output_dir,
                      self.tframes,
                      self.csvdir)


@bundles.register("csvdir")
def csvdir_bundle(environ,
                  asset_db_writer,
                  minute_bar_writer,
                  daily_bar_writer,
                  adjustment_writer,
                  calendar,
                  start_session,
                  end_session,
                  cache,
                  show_progress,
                  output_dir,
                  tframes=None,
                  csvdir=None):
    """
    Build a zipline data bundle from the directory with csv files.
    """
    if not csvdir:
        csvdir = environ.get('CSVDIR')
        if not csvdir:
            raise ValueError("CSVDIR environment variable is not set")

    # if not os.path.isdir(csvdir):
    #     raise ValueError("%s is not a directory" % csvdir)

    if not tframes:
        tframes = set(["daily", "minute"]).intersection(os.listdir(csvdir))

        if not tframes:
            raise ValueError("'daily' and 'minute' directories "
                             "not found in '%s'" % csvdir)

    divs_splits = {'divs': DataFrame(columns=['sid', 'amount',
                                              'ex_date', 'record_date',
                                              'declared_date', 'pay_date']),
                   'stock_divs': DataFrame(columns=['sid', 'ratio',
                                              'ex_date', 'record_date',
                                              'declared_date', 'pay_date',
                                              'payment_sid']),
                   'splits': DataFrame(columns=['sid', 'ratio',
                                                'effective_date'])}

    for tframe in tframes:

        ddir = os.path.join(csvdir, 'Summary.csv')
        symbols = read_csv(ddir)['Name'].tolist()
        ddir = os.path.join(csvdir, tframe)

        # symbols = sorted(item.split('.csv')[0]
        #                  for item in os.listdir(ddir)
        #                  if '.csv' in item)
        if not symbols:
            raise ValueError("no <symbol>.csv* files found in %s" % ddir)

        dtype = [('start_date', 'datetime64[ns]'),
                 ('end_date', 'datetime64[ns]'),
                 ('auto_close_date', 'datetime64[ns]'),
                 ('symbol', 'object')]
        metadata = DataFrame(empty(len(symbols), dtype=dtype))

        if tframe == 'minute':
            writer = minute_bar_writer
        else:
            writer = daily_bar_writer

        writer.write(_pricing_iter(ddir, symbols, metadata,
                     divs_splits, show_progress, calendar=calendar, start = start_session),
                     show_progress=show_progress)

    # Hardcode the exchange to "CSVDIR" for all assets and (elsewhere)
    # register "CSVDIR" to resolve to the NYSE calendar, because these
    # are all equities and thus can use the NYSE calendar.
    metadata['exchange'] = "CSVDIR"

    if 'daily' not in tframes: # Minute
        asset_db_writer.write(equities=metadata)

        divs_splits['divs']['sid'] = divs_splits['divs']['sid'].astype(int)
        divs_splits['stock_divs']['sid'] = divs_splits['stock_divs']['sid'].astype(int)
        divs_splits['splits']['sid'] = divs_splits['splits']['sid'].astype(int)
        adjustment_writer.write(dividends=divs_splits['divs'],
                                stock_dividends=divs_splits['stock_divs'],
                                splits=divs_splits['splits'])
    # if tframe == 'daily':
    elif 'daily' in tframes:
        asset_db_writer.write(equities=metadata)

        divs_splits['divs']['sid'] = divs_splits['divs']['sid'].astype(int)
        divs_splits['stock_divs']['sid'] = divs_splits['stock_divs']['sid'].astype(int)
        divs_splits['stock_divs']['payment_sid'] = divs_splits['stock_divs']['payment_sid'].astype(int)
        divs_splits['splits']['sid'] = divs_splits['splits']['sid'].astype(int)
        adjustment_writer.write(dividends=divs_splits['divs'],
                                stock_dividends=divs_splits['stock_divs'],
                                splits=divs_splits['splits'])

def _ensure_calendar_aligned(data, calendar, start, freq = 'daily'):
    if not start:
        start = data.index[0]
    if freq == 'daily':
        data.index = data.index.tz_localize(tz='UTC')
        calendar_index = calendar.closes[start: data.index[-1]]
        result = data.join(calendar_index,how='right').set_index('market_close')
        result['dividend'] = 0
        result['split'] = 1
        # _, result = calendar_index.align(data, join='left')
    else:
        data.index = data.index.tz_localize(tz=calendar.tz)
        calendar_index = calendar.all_minutes[(calendar.all_minutes>=start)&
                                              (calendar.all_minutes<= data.index[-1])]
        calendar_index2 = pd.Series(data=[0]*len(calendar_index),index=calendar_index)
        _, result = calendar_index2.align(data, join='left')

    # calendar_index = calendar.closes[data.index[0]:data.index[-1]]
    return result


def _pricing_iter(csvdir, symbols, metadata, divs_splits, show_progress, calendar=None, start = None):
    with maybe_show_progress(symbols, show_progress,
                             label='Loading custom pricing data: ') as it:
        # files = os.listdir(csvdir)
        for sid, symbol in enumerate(it):
            logger.debug('%s: sid %s' % (symbol, sid))
            fname = symbol + '.csv'

            # Lance：路径获取方式修改
            dfr = read_csv(os.path.join(csvdir, fname).replace('\\', '/'),
                           parse_dates=[0],
                           infer_datetime_format=True,
                           index_col=0).sort_index()
            freq = os.path.basename(csvdir)

            dfr = _ensure_calendar_aligned(dfr, calendar, start, freq=freq).fillna(method='ffill')

            '''
            if 'dividend' in dfr.columns:
                dfr['dividend'] = dfr['dividend'].fillna(0.)
            else:
                dfr['dividend'] = .0
            if 'split' in dfr.columns:
                dfr['split'] = dfr['split'].fillna(1.)
            else:
                dfr['split'] = 1.
            '''
            start_date = dfr.index[0]
            end_date = dfr.index[-1]

            # The auto_close date is the day after the last trade.
            ac_date = end_date + Timedelta(days=1)
            metadata.iloc[sid] = start_date, end_date, ac_date, symbol

            if 'dividend' in dfr.columns:
                # ex_date   amount  sid record_date declared_date pay_date
                tmp = dfr[dfr['dividend'] != 0.0]['dividend']
                tmp.index = tmp.index + Timedelta(hours=-8)
                div = DataFrame(data=tmp.index.tolist(), columns=['ex_date'])
                div['record_date'] = NaT
                div['declared_date'] = NaT
                div['pay_date'] = div['ex_date'] + Timedelta(days=0)
                div['amount'] = tmp.tolist()
                div['sid'] = sid

                divs = divs_splits['divs']
                index = Index(range(divs.shape[0],
                                  divs.shape[0] + div.shape[0]))
                div.set_index(index, inplace=True)
                divs_splits['divs'] = divs.append(div)

            if 'stock_dividend' in dfr.columns:
                # ex_date ratio sid record_date declared_date pay_date payment_sid
                tmp = dfr[dfr['stock_dividend'] != 0.0]['stock_dividend']
                tmp.index = tmp.index + Timedelta(hours=-8)
                stock_div = DataFrame(data=tmp.index.tolist(), columns=['ex_date'])
                stock_div['record_date'] = NaT
                stock_div['declared_date'] = NaT
                stock_div['pay_date'] = stock_div['ex_date'] + Timedelta(days=0)
                stock_div['ratio'] = tmp.tolist()
                stock_div['sid'] = sid
                stock_div['payment_sid'] = sid

                stock_divs = divs_splits['stock_divs']
                index = Index(range(stock_divs.shape[0],
                                  stock_divs.shape[0] + stock_div.shape[0]))
                stock_div.set_index(index, inplace=True)
                divs_splits['stock_divs'] = stock_divs.append(stock_div)

            if 'split' in dfr.columns:
                tmp = 1. / dfr[dfr['split'] != 1.0]['split']
                tmp.index = tmp.index + Timedelta(hours=-8)
                split = DataFrame(data=tmp.index.tolist(),
                                  columns=['effective_date'])
                split['ratio'] = tmp.tolist()
                split['sid'] = sid

                splits = divs_splits['splits']
                index = Index(range(splits.shape[0],
                                    splits.shape[0] + split.shape[0]))
                split.set_index(index, inplace=True)
                divs_splits['splits'] = splits.append(split)

            yield sid, dfr

# register_calendar_alias("CSVDIR", "NYSE")
