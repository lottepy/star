import os
from os.path import join
from datamaster import dm_client
import pandas as pd
from algorithm import addpath

def download_Ashare_trading_data():
    data_path = join(addpath.root_path, 'data')
    result_path = join(addpath.root_path, 'data', 'Ashare', 'trading_data')
    ticker = pd.read_csv(os.path.join(data_path, 'ticker_industry.csv'))

    dm_client.start()

    # download A share from dataMaster
    for i in ticker['Stkcd']:
        print(i)
        try:
            tmp_data = dm_client.historical(symbols=i, start_date='2008-01-01',
                                            fields=['adjust_factor', 'choice_mktcap', 'volume', 'close',
                                                    'open', 'high', 'low'])
            tmp_data = pd.DataFrame(tmp_data['values'][i], columns=tmp_data['fields'])
            tmp_data['date'] = pd.to_datetime(tmp_data['date'])
            tmp_data.set_index('date', inplace=True)
            tmp_data = tmp_data.rename({'close': 'PX_LAST_RAW', 'open': 'PX_OPEN_RAW',
                                        'high': 'PX_HIGH_RAW', 'low': 'PX_LOW_RAW',
                                        'choice_mktcap': 'MARKET_CAP', 'volume': 'PX_VOLUME_RAW'}, axis=1)
            tmp_data['close'] = tmp_data['PX_LAST_RAW'] * tmp_data['adjust_factor']
            tmp_data['open'] = tmp_data['PX_OPEN_RAW'] * tmp_data['adjust_factor']
            tmp_data['high'] = tmp_data['PX_HIGH_RAW'] * tmp_data['adjust_factor']
            tmp_data['low'] = tmp_data['PX_LOW_RAW'] * tmp_data['adjust_factor']
            tmp_data = tmp_data[tmp_data['open'].notna()]
            tmp_data.to_csv(join(result_path, i + '.csv'))

        except:
            continue

def download_Hshare_trading_data():
    dm_client.start()
    hk_trading_data_path = join(addpath.data_path, 'Hshare', 'trading')

    # download H share from dataMaster
    start_date = '2009-12-31'
    hk_symbol = pd.read_csv(os.path.join(addpath.config_path, 'Hshare_symbol_list.csv'))
    for i in range(0, hk_symbol.shape[0]):
        symbol = hk_symbol.iloc[i, 0]
        # print(symbol)
        # tmp_data = pd.read_csv(join(hk_result_path, symbol + ".csv"))
        # last_close = tmp_data[tmp_data['date'] == start_date]['PX_LAST']
        #
        # 港股 开盘价 收盘价 最高价 最低价 成交量 流通市值 总市值
        tmp = pd.DataFrame(
            columns=['date', 'PX_OPEN_RAW', 'PX_HIGH_RAW', 'PX_LOW_RAW', 'PX_LAST_RAW', 'PX_VOLUME_RAW',
                     'DAY_TO_DAY_TOT_RETURN_GROSS_DVDS', 'EQY_SH_OUT_RAW', 'MARKET_CAP',
                     'DAY_TO_DAY_TOT_RETURN_GROSS_DVDS_PRODUCT', 'adjust_factor', 'PX_LAST'])

        data = dm_client.historical(symbols=hk_symbol.iloc[i, 1], start_date=start_date,
                                    fields=['adjust_factor', 'choice_mktcap', 'volume', 'close',
                                            'open', 'high', 'low', 'bbg_daily_total_return','bbg_total_shares_outstanding'])
        data = pd.DataFrame(data['values'][hk_symbol.iloc[i, 1]], columns=data['fields'])
        data['date'] = pd.to_datetime(data['date'])

        tmp['date'] = data['date']
        tmp['PX_OPEN_RAW'] = data['open']
        tmp['PX_HIGH_RAW'] = data['high']
        tmp['PX_LOW_RAW'] = data['low']
        tmp['PX_LAST_RAW'] = data['close']
        tmp['PX_VOLUME_RAW'] = data['volume']
        tmp['MARKET_CAP'] = data['choice_mktcap']
        tmp['adjust_factor'] = data['adjust_factor']
        tmp['DAY_TO_DAY_TOT_RETURN_GROSS_DVDS'] = data['bbg_daily_total_return']
        tmp['PX_LAST'] = tmp['PX_LAST_RAW'] * tmp['adjust_factor']
        tmp['PX_VOLUME'] = tmp['PX_VOLUME_RAW']/tmp['adjust_factor']
        tmp['EQY_SH_OUT_RAW']=data['bbg_total_shares_outstanding']
        tmp['PX_LAST']=tmp['PX_LAST'].ffill()
        tmp['PX_VOLUME']=tmp['PX_VOLUME'].fillna(0)

        tmp.to_csv(join(hk_trading_data_path, symbol + '.csv'),index=False)

def download_Hshare_financial_data():
    dm_client.start()
    hk_financial_data_path = join(addpath.data_path, 'Hshare', 'financial')
    if os.path.exists(hk_financial_data_path):
        pass
    else:
        os.makedirs(hk_financial_data_path)

    # download H share from dataMaster
    start_date = '2009-12-31'
    hk_symbol = pd.read_csv(os.path.join(addpath.config_path, 'Hshare_symbol_list.csv'))
    for i in range(0, hk_symbol.shape[0]):
        symbol = hk_symbol.iloc[i, 0]
        # print(symbol)
        try:
            # 港股 开盘价 收盘价 最高价 最低价 成交量 流通市值 总市值

            fields = ['bbg_announcement_dt', 'bbg_cash_and_marketable_securities','bbg_bs_acct_note_rcv',\
                      'bbg_bs_inventories','bbg_bs_cur_asset_report','bbg_bs_gross_fix_asset',\
                      'bbg_bs_disclosed_intangibles','bbg_bs_tot_asset','bbg_ardr_accounts_payable_trade',\
                      'bbg_bs_st_borrow','bbg_bs_st_portion_of_lt_debt','bbg_bs_cur_liab',\
                      'bbg_bs_long_term_borrowings','bbg_bs_pensions_lt_liabs','bbg_bs_tot_liab2','bbg_tot_common_eqy',\
                      'bbg_bs_retain_earn','bbg_sales_rev_turn','bbg_is_cogs_to_fe_and_pp_and_g',\
                      'bbg_is_sg_a_expense','bbg_cf_depr_amort','bbg_is_oper_inc','bbg_is_int_expense',\
                      'bbg_net_income','bbg_is_div_per_shr'
                      ]

            data = dm_client.historical(symbols=hk_symbol.iloc[i, 1], start_date=start_date,
                                        fields=fields)
            data = pd.DataFrame(data['values'][hk_symbol.iloc[i, 1]], columns=data['fields'])
            data['date'] = pd.to_datetime(data['date'])
            data.rename(columns={'bbg_announcement_dt': 'ANNOUNCEMENT_DT',\
                                 'bbg_cash_and_marketable_securities': 'BS_CASH_CASH_EQUIVALENTS_AND_STI',\
                                 'bbg_bs_acct_note_rcv': 'BS_ACCT_NOTE_RCV','bbg_bs_inventories': 'BS_INVENTORIES',\
                                 'bbg_bs_cur_asset_report':'BS_CUR_ASSET_REPORT', \
                                 'bbg_bs_gross_fix_asset':'BS_GROSS_FIX_ASSET', \
                                 'bbg_bs_disclosed_intangibles':'BS_DISCLOSED_INTANGIBLES',\
                                 'bbg_bs_tot_asset':'BS_TOT_ASSET',
                                 'bbg_ardr_accounts_payable_trade':'ARDR_ACCOUNTS_PAYABLE_TRADE',\
                                 'bbg_bs_st_borrow':'BS_ST_BORROW', \
                                 'bbg_bs_st_portion_of_lt_debt':'BS_ST_PORTION_OF_LT_DEBT',\
                                 'bbg_bs_cur_liab':'BS_CUR_LIAB',\
                                 'bbg_bs_long_term_borrowings':'BS_LONG_TERM_BORROWINGS',\
                                 'bbg_bs_pensions_lt_liabs':'BS_PENSIONS_LT_LIABS',
                                 'bbg_bs_tot_liab2':'BS_TOT_LIAB2','bbg_tot_common_eqy':'TOT_COMMON_EQY',\
                                 'bbg_bs_retain_earn':'BS_RETAIN_EARN','bbg_sales_rev_turn':'SALES_REV_TURN',\
                                 'bbg_is_cogs_to_fe_and_pp_and_g':'IS_COGS_TO_FE_AND_PP_AND_G',\
                                 'bbg_is_sg_a_expense':'IS_SG&A_EXPENSE','bbg_cf_depr_amort':'CF_DEPR_AMORT',\
                                 'bbg_is_oper_inc':'IS_OPER_INC', 'bbg_is_int_expense':'IS_INT_EXPENSE', \
                                 'bbg_net_income':'NET_INCOME', 'bbg_is_div_per_shr':'IS_DIV_PER_SHR'
                                 }, inplace=True)


            output = data[['date', 'ANNOUNCEMENT_DT', 'BS_CASH_CASH_EQUIVALENTS_AND_STI',
       'BS_ACCT_NOTE_RCV', 'BS_INVENTORIES', 'BS_CUR_ASSET_REPORT',
       'BS_GROSS_FIX_ASSET', 'BS_DISCLOSED_INTANGIBLES', 'BS_TOT_ASSET',
       'ARDR_ACCOUNTS_PAYABLE_TRADE', 'BS_ST_BORROW',
       'BS_ST_PORTION_OF_LT_DEBT', 'BS_CUR_LIAB', 'BS_LONG_TERM_BORROWINGS',
       'BS_PENSIONS_LT_LIABS', 'BS_TOT_LIAB2', 'TOT_COMMON_EQY',
       'BS_RETAIN_EARN', 'SALES_REV_TURN', 'IS_COGS_TO_FE_AND_PP_AND_G',
       'IS_SG&A_EXPENSE', 'CF_DEPR_AMORT', 'IS_OPER_INC', 'IS_INT_EXPENSE',
       'NET_INCOME', 'IS_DIV_PER_SHR']]
            output.to_csv(join(hk_financial_data_path, symbol + '.csv'),index=False)
            if len(data) == 0:
                print('dm has no data for ' + symbol)

        except:
            continue

if __name__ == '__main__':
    download_Hshare_financial_data()
    # download_Hshare_trading_data()


