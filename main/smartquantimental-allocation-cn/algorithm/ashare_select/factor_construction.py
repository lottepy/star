import numpy as np
import pandas as pd
import os
from datetime import datetime, timedelta
import multiprocessing
from algorithm import addpath


# Factor construction

def cal_factors(symbol):
    iu_path = os.path.join(addpath.data_path, "Ashare", "investment_universe")
    formation_dates = os.listdir(iu_path)
    formation_dates = [datetime.strptime(formation_date[:-4], "%Y-%m-%d") for formation_date in formation_dates]

    trading_data_path = os.path.join(addpath.data_path, "Ashare", "trading_data", symbol + ".csv")
    trading_data = pd.read_csv(trading_data_path, index_col=0, parse_dates=[0])
    trading_data['fwd_return_qtr'] = trading_data['PX_LAST'].shift(-63) / trading_data['PX_LAST'] - 1
    trading_data['fwd_return_semiyear'] = trading_data['PX_LAST'].shift(-123) / trading_data['PX_LAST'] - 1
    trading_data['fwd_return_annyear'] = trading_data['PX_LAST'].shift(-245) / trading_data['PX_LAST'] - 1
    trading_data['EQY_SH_OUT'] = trading_data['MARKET_CAP'] / trading_data['PX_LAST_RAW']
    trading_data = trading_data[['fwd_return_qtr', 'fwd_return_semiyear', 'fwd_return_annyear', 'MARKET_CAP',
                                 'EQY_SH_OUT', 'PX_LAST_RAW']]
    trading_data = trading_data.resample('1D').last().ffill()
    trading_data = trading_data.loc[formation_dates, :]
    trading_data_tmp = trading_data[trading_data.index.month == 4]
    trading_data_tmp.index = trading_data_tmp.index + timedelta(days=1)
    trading_data = pd.concat([trading_data, trading_data_tmp]).sort_index()

    financial_data_path = os.path.join(addpath.data_path, "Ashare", "financial_data", symbol + ".csv")
    financial_data = pd.read_csv(financial_data_path, index_col='end_date', parse_dates=['end_date', 'report_date'])
    financial_data = pd.concat([trading_data, financial_data], axis=1)
    financial_data.index.name = 'end_date'

    financial_data = financial_data.reset_index()
    financial_data = financial_data.set_index('report_date')
    financial_data.index.name = 'report_date'
    financial_data.sort_index(inplace=True)

    financial_data = financial_data.dropna(subset=['TINCOMESTATEMENTQ_9'])

    flow_measures = ['TINCOMESTATEMENTQ_60', 'TINCOMESTATEMENTQ_9', 'TINCOMESTATEMENTQ_10', 'TINCOMESTATEMENTQ_48',
                    'TINCOMESTATEMENTQ_14', 'TCASHFLOWSTATEMENTQ_39']

    bs_measures = ['TBALANCESTATEMENT_141', 'TBALANCESTATEMENT_74', 'TBALANCESTATEMENT_216']

    financial_data['TINCOMESTATEMENT_89'] = financial_data['TINCOMESTATEMENT_89'].fillna(0)
    financial_data['TINCOMESTATEMENTQ_14'] = financial_data['TINCOMESTATEMENTQ_14'].fillna(0)
    if 'TINCOMESTATEMENTQ_10' in financial_data.columns:
        financial_data['TINCOMESTATEMENTQ_10'] = financial_data['TINCOMESTATEMENTQ_10'].fillna(0)
    else:
        financial_data['TINCOMESTATEMENTQ_10'] = 0
    financial_data['TCASHFLOWSTATEMENTQ_39'] = financial_data['TCASHFLOWSTATEMENTQ_39'].fillna(0)

    financial_data['TBALANCESTATEMENT_141'] = financial_data['TBALANCESTATEMENT_141'].ffill()
    financial_data['TBALANCESTATEMENT_31'] = financial_data['TBALANCESTATEMENT_31'].ffill()
    financial_data['TBALANCESTATEMENT_25'] = financial_data['TBALANCESTATEMENT_25'].ffill()
    financial_data['TBALANCESTATEMENT_9'] = financial_data['TBALANCESTATEMENT_9'].ffill()
    financial_data['TBALANCESTATEMENT_93'] = financial_data['TBALANCESTATEMENT_93'].ffill()
    financial_data['TBALANCESTATEMENT_74'] = financial_data['TBALANCESTATEMENT_74'].ffill()
    financial_data['TBALANCESTATEMENT_216'] = financial_data['TBALANCESTATEMENT_216'].ffill()
    financial_data['TBALANCESTATEMENT_147'] = financial_data['TBALANCESTATEMENT_147'].ffill()
    financial_data['TBALANCESTATEMENT_88'] = financial_data['TBALANCESTATEMENT_88'].ffill()
    financial_data['TBALANCESTATEMENT_94'] = financial_data['TBALANCESTATEMENT_94'].ffill()
    financial_data['TBALANCESTATEMENT_228'] = financial_data['TBALANCESTATEMENT_228'].ffill()
    financial_data['TBALANCESTATEMENT_147'] = financial_data['TBALANCESTATEMENT_147'].fillna(0)
    financial_data['TBALANCESTATEMENT_88'] = financial_data['TBALANCESTATEMENT_88'].fillna(0)
    financial_data['TBALANCESTATEMENT_94'] = financial_data['TBALANCESTATEMENT_94'].fillna(0)
    financial_data['TBALANCESTATEMENT_228'] = financial_data['TBALANCESTATEMENT_228'].fillna(0)
    financial_data['TBALANCESTATEMENT_216'] = financial_data['TBALANCESTATEMENT_216'].fillna(0)
    financial_data['REPORTDATEDIVIDEND'] = financial_data['REPORTDATEDIVIDEND'].fillna(0)
    financial_data['PPERIODUNLOCKAMT'] = financial_data['PPERIODUNLOCKAMT'].fillna(0)

    # Profitability and Growth
    for flow_measure in flow_measures:
        financial_data[flow_measure + '_TTM'] = financial_data[flow_measure] + financial_data[flow_measure].shift(1) + financial_data[flow_measure].shift(2) + financial_data[flow_measure].shift(3)
        financial_data[flow_measure + '_Semi'] = financial_data[flow_measure] + financial_data[flow_measure].shift(1)

    for bs_measure in bs_measures:
        financial_data[bs_measure + '_TTM'] = (financial_data[bs_measure] + financial_data[bs_measure].shift(1) + financial_data[bs_measure].shift(2) + financial_data[bs_measure].shift(3)) / 4
        financial_data[bs_measure + '_Semi'] = (financial_data[bs_measure] + financial_data[bs_measure].shift(1)) / 2

    for period in ['', '_Semi', '_TTM']:
        financial_data['ROE' + period] = financial_data['TINCOMESTATEMENTQ_60' + period] / financial_data['TBALANCESTATEMENT_141' + period]
        financial_data['ROA' + period] = financial_data['TINCOMESTATEMENTQ_60' + period] / financial_data['TBALANCESTATEMENT_74' + period]
        financial_data['GPOE' + period] = (financial_data['TINCOMESTATEMENTQ_9' + period] - financial_data['TINCOMESTATEMENTQ_10' + period]) / financial_data['TBALANCESTATEMENT_141' + period]
        financial_data['GPOA' + period] = (financial_data['TINCOMESTATEMENTQ_9' + period] - financial_data['TINCOMESTATEMENTQ_10' + period]) / financial_data['TBALANCESTATEMENT_74' + period]
        financial_data['SOE' + period] = financial_data['TINCOMESTATEMENTQ_9' + period] / financial_data['TBALANCESTATEMENT_141' + period]
        financial_data['SOA' + period] = financial_data['TINCOMESTATEMENTQ_9' + period] / financial_data['TBALANCESTATEMENT_74' + period]
        financial_data['OPOE' + period] = (financial_data['TINCOMESTATEMENTQ_48' + period] + financial_data['TINCOMESTATEMENTQ_14' + period]) / financial_data['TBALANCESTATEMENT_141' + period]
        financial_data['OPOA' + period] = (financial_data['TINCOMESTATEMENTQ_48' + period] + financial_data['TINCOMESTATEMENTQ_14' + period]) / financial_data['TBALANCESTATEMENT_74' + period]
        financial_data['CFOOE' + period] = financial_data['TCASHFLOWSTATEMENTQ_39' + period] / financial_data['TBALANCESTATEMENT_141' + period]
        financial_data['CFOOA' + period] = financial_data['TCASHFLOWSTATEMENTQ_39' + period] / financial_data['TBALANCESTATEMENT_74' + period]
        financial_data['ARTOS' + period] = financial_data['TBALANCESTATEMENT_216' + period] / financial_data['TINCOMESTATEMENTQ_9' + period]

        financial_data['sales' + period] = (financial_data['SOE' + period] + financial_data['SOA' + period]) / 2
        financial_data['gross_profitability' + period] = (financial_data['GPOE' + period] + financial_data['GPOA' + period]) / 2
        financial_data['operating_profitability' + period] = (financial_data['OPOE' + period] + financial_data['OPOA' + period]) / 2
        financial_data['net_profitability' + period] = (financial_data['ROE' + period] + financial_data['ROA' + period]) / 2
        financial_data['cashflow_generation' + period] = (financial_data['CFOOE' + period] + financial_data['CFOOA' + period]) / 2

        financial_data['gross_profitability' + period] = financial_data['gross_profitability' + period].fillna(0)
        financial_data['profitability' + period] = (financial_data['gross_profitability' + period] + financial_data['operating_profitability' + period] + financial_data['net_profitability' + period]) / 3

        financial_data['ROE' + period + '_gr_1yr'] = (financial_data['TINCOMESTATEMENTQ_60' + period] - financial_data['TINCOMESTATEMENTQ_60' + period].shift(4)) / financial_data['TBALANCESTATEMENT_141' + period]
        financial_data['ROA' + period + '_gr_1yr'] = (financial_data['TINCOMESTATEMENTQ_60' + period] - financial_data['TINCOMESTATEMENTQ_60' + period].shift(4)) / financial_data['TBALANCESTATEMENT_74' + period]
        financial_data['GPOE' + period + '_gr_1yr'] = ((financial_data['TINCOMESTATEMENTQ_9' + period] - financial_data['TINCOMESTATEMENTQ_10' + period]) - (financial_data['TINCOMESTATEMENTQ_9' + period].shift(4) - financial_data['TINCOMESTATEMENTQ_10' + period].shift(4))) / financial_data['TBALANCESTATEMENT_141' + period]
        financial_data['GPOA' + period + '_gr_1yr'] = ((financial_data['TINCOMESTATEMENTQ_9' + period] - financial_data['TINCOMESTATEMENTQ_10' + period]) - (financial_data['TINCOMESTATEMENTQ_9' + period].shift(4) - financial_data['TINCOMESTATEMENTQ_10' + period].shift(4))) / financial_data['TBALANCESTATEMENT_74' + period]
        financial_data['SOE' + period + '_gr_1yr'] = (financial_data['TINCOMESTATEMENTQ_9' + period] - financial_data['TINCOMESTATEMENTQ_9' + period].shift(4)) / financial_data['TBALANCESTATEMENT_141' + period]
        financial_data['SOA' + period + '_gr_1yr'] = (financial_data['TINCOMESTATEMENTQ_9' + period] - financial_data['TINCOMESTATEMENTQ_9' + period].shift(4)) / financial_data['TBALANCESTATEMENT_74' + period]
        financial_data['OPOE' + period + '_gr_1yr'] = ((financial_data['TINCOMESTATEMENTQ_48' + period] + financial_data['TINCOMESTATEMENTQ_14' + period]) - (financial_data['TINCOMESTATEMENTQ_48' + period].shift(4) + financial_data['TINCOMESTATEMENTQ_14' + period].shift(4))) / financial_data['TBALANCESTATEMENT_141' + period]
        financial_data['OPOA' + period + '_gr_1yr'] = ((financial_data['TINCOMESTATEMENTQ_48' + period] + financial_data['TINCOMESTATEMENTQ_14' + period]) - (financial_data['TINCOMESTATEMENTQ_48' + period].shift(4) + financial_data['TINCOMESTATEMENTQ_14' + period].shift(4))) / financial_data['TBALANCESTATEMENT_74' + period]
        financial_data['CFOOE' + period + '_gr_1yr'] = (financial_data['TCASHFLOWSTATEMENTQ_39' + period] - financial_data['TCASHFLOWSTATEMENTQ_39' + period].shift(4)) / financial_data['TBALANCESTATEMENT_141' + period]
        financial_data['CFOOA' + period + '_gr_1yr'] = (financial_data['TCASHFLOWSTATEMENTQ_39' + period] - financial_data['TCASHFLOWSTATEMENTQ_39' + period].shift(4)) / financial_data['TBALANCESTATEMENT_74' + period]
        financial_data['ARTOS' + period + '_gr_1yr'] = financial_data['ARTOS' + period] / financial_data['ARTOS' + period].shift(4) - 1
        financial_data['S' + period + '_gr_1yr'] = financial_data['TINCOMESTATEMENTQ_9' + period] / financial_data['TINCOMESTATEMENTQ_9' + period].shift(4) - 1

        financial_data['ROE' + period + '_gr_2yr'] = (financial_data['TINCOMESTATEMENTQ_60' + period] - financial_data['TINCOMESTATEMENTQ_60' + period].shift(8)) / financial_data['TBALANCESTATEMENT_141' + period]
        financial_data['ROA' + period + '_gr_2yr'] = (financial_data['TINCOMESTATEMENTQ_60' + period] - financial_data['TINCOMESTATEMENTQ_60' + period].shift(8)) / financial_data['TBALANCESTATEMENT_74' + period]
        financial_data['GPOE' + period + '_gr_2yr'] = ((financial_data['TINCOMESTATEMENTQ_9' + period] - financial_data['TINCOMESTATEMENTQ_10' + period]) - (financial_data['TINCOMESTATEMENTQ_9' + period].shift(8) - financial_data['TINCOMESTATEMENTQ_10' + period].shift(8))) / financial_data['TBALANCESTATEMENT_141' + period]
        financial_data['GPOA' + period + '_gr_2yr'] = ((financial_data['TINCOMESTATEMENTQ_9' + period] - financial_data['TINCOMESTATEMENTQ_10' + period]) - (financial_data['TINCOMESTATEMENTQ_9' + period].shift(8) - financial_data['TINCOMESTATEMENTQ_10' + period].shift(8))) / financial_data['TBALANCESTATEMENT_74' + period]
        financial_data['SOE' + period + '_gr_2yr'] = (financial_data['TINCOMESTATEMENTQ_9' + period] - financial_data['TINCOMESTATEMENTQ_9' + period].shift(8)) / financial_data['TBALANCESTATEMENT_141' + period]
        financial_data['SOA' + period + '_gr_2yr'] = (financial_data['TINCOMESTATEMENTQ_9' + period] - financial_data['TINCOMESTATEMENTQ_9' + period].shift(8)) / financial_data['TBALANCESTATEMENT_74' + period]
        financial_data['OPOE' + period + '_gr_2yr'] = ((financial_data['TINCOMESTATEMENTQ_48' + period] + financial_data['TINCOMESTATEMENTQ_14' + period]) - (financial_data['TINCOMESTATEMENTQ_48' + period].shift(8) + financial_data['TINCOMESTATEMENTQ_14' + period].shift(8))) / financial_data['TBALANCESTATEMENT_141' + period]
        financial_data['OPOA' + period + '_gr_2yr'] = ((financial_data['TINCOMESTATEMENTQ_48' + period] + financial_data['TINCOMESTATEMENTQ_14' + period]) - (financial_data['TINCOMESTATEMENTQ_48' + period].shift(8) + financial_data['TINCOMESTATEMENTQ_14' + period].shift(8))) / financial_data['TBALANCESTATEMENT_74' + period]
        financial_data['CFOOE' + period + '_gr_2yr'] = (financial_data['TCASHFLOWSTATEMENTQ_39' + period] - financial_data['TCASHFLOWSTATEMENTQ_39' + period].shift(8)) / financial_data['TBALANCESTATEMENT_141' + period]
        financial_data['CFOOA' + period + '_gr_2yr'] = (financial_data['TCASHFLOWSTATEMENTQ_39' + period] - financial_data['TCASHFLOWSTATEMENTQ_39' + period].shift(8)) / financial_data['TBALANCESTATEMENT_74' + period]
        financial_data['ARTOS' + period + '_gr_2yr'] = financial_data['ARTOS' + period] / financial_data['ARTOS' + period].shift(8) - 1
        financial_data['S' + period + '_gr_2yr'] = financial_data['TINCOMESTATEMENTQ_9' + period] / financial_data['TINCOMESTATEMENTQ_9' + period].shift(8) - 1

        financial_data['ROE' + period + '_Abnormal'] = (financial_data['TINCOMESTATEMENTQ_60' + period] - (financial_data['TINCOMESTATEMENTQ_60' + period].shift(4) + financial_data['TINCOMESTATEMENTQ_60' + period].shift(8)) / 2) / financial_data['TBALANCESTATEMENT_141' + period]
        financial_data['ROA' + period + '_Abnormal'] = (financial_data['TINCOMESTATEMENTQ_60' + period] - (financial_data['TINCOMESTATEMENTQ_60' + period].shift(4) + financial_data['TINCOMESTATEMENTQ_60' + period].shift(8)) / 2) / financial_data['TBALANCESTATEMENT_74' + period]
        financial_data['GPOE' + period + '_Abnormal'] = ((financial_data['TINCOMESTATEMENTQ_9' + period] - financial_data['TINCOMESTATEMENTQ_10' + period]) - (financial_data['TINCOMESTATEMENTQ_9' + period].shift(4) - financial_data['TINCOMESTATEMENTQ_10' + period].shift(4) + financial_data['TINCOMESTATEMENTQ_9' + period].shift(8) - financial_data['TINCOMESTATEMENTQ_10' + period].shift(8)) / 2) / financial_data['TBALANCESTATEMENT_141' + period]
        financial_data['GPOA' + period + '_Abnormal'] = ((financial_data['TINCOMESTATEMENTQ_9' + period] - financial_data['TINCOMESTATEMENTQ_10' + period]) - (financial_data['TINCOMESTATEMENTQ_9' + period].shift(4) - financial_data['TINCOMESTATEMENTQ_10' + period].shift(4) + financial_data['TINCOMESTATEMENTQ_9' + period].shift(8) - financial_data['TINCOMESTATEMENTQ_10' + period].shift(8)) / 2) / financial_data['TBALANCESTATEMENT_74' + period]
        financial_data['SOE' + period + '_Abnormal'] = (financial_data['TINCOMESTATEMENTQ_9' + period] - (financial_data['TINCOMESTATEMENTQ_9' + period].shift(4) + financial_data['TINCOMESTATEMENTQ_9' + period].shift(8)) / 2) / financial_data['TBALANCESTATEMENT_141' + period]
        financial_data['SOA' + period + '_Abnormal'] = (financial_data['TINCOMESTATEMENTQ_9' + period] - (financial_data['TINCOMESTATEMENTQ_9' + period].shift(4) + financial_data['TINCOMESTATEMENTQ_9' + period].shift(8)) / 2) / financial_data['TBALANCESTATEMENT_74' + period]
        financial_data['OPOE' + period + '_Abnormal'] = ((financial_data['TINCOMESTATEMENTQ_48' + period] + financial_data['TINCOMESTATEMENTQ_14' + period]) - (financial_data['TINCOMESTATEMENTQ_48' + period].shift(4) + financial_data['TINCOMESTATEMENTQ_14' + period].shift(4) + financial_data['TINCOMESTATEMENTQ_48' + period].shift(8) + financial_data['TINCOMESTATEMENTQ_14' + period].shift(8)) / 2) / financial_data['TBALANCESTATEMENT_141' + period]
        financial_data['OPOA' + period + '_Abnormal'] = ((financial_data['TINCOMESTATEMENTQ_48' + period] + financial_data['TINCOMESTATEMENTQ_14' + period]) - (financial_data['TINCOMESTATEMENTQ_48' + period].shift(4) + financial_data['TINCOMESTATEMENTQ_14' + period].shift(4) + financial_data['TINCOMESTATEMENTQ_48' + period].shift(8) + financial_data['TINCOMESTATEMENTQ_14' + period].shift(8)) / 2) / financial_data['TBALANCESTATEMENT_74' + period]
        financial_data['CFOOE' + period + '_Abnormal'] = (financial_data['TCASHFLOWSTATEMENTQ_39' + period] - (financial_data['TCASHFLOWSTATEMENTQ_39' + period].shift(4) + financial_data['TCASHFLOWSTATEMENTQ_39' + period].shift(8)) / 2) / financial_data['TBALANCESTATEMENT_141' + period]
        financial_data['CFOOA' + period + '_Abnormal'] = (financial_data['TCASHFLOWSTATEMENTQ_39' + period] - (financial_data['TCASHFLOWSTATEMENTQ_39' + period].shift(4) + financial_data['TCASHFLOWSTATEMENTQ_39' + period].shift(8)) / 2) / financial_data['TBALANCESTATEMENT_74' + period]
        financial_data['ARTOS' + period + '_Abnormal'] = financial_data['ARTOS' + period] - ((financial_data['ARTOS' + period].shift(4) + financial_data['ARTOS' + period].shift(8)) / 2) - 1
        financial_data['S' + period + '_Abnormal'] = financial_data['TINCOMESTATEMENTQ_9' + period] - ((financial_data['TINCOMESTATEMENTQ_9' + period].shift(4) + financial_data['TINCOMESTATEMENTQ_9' + period].shift(8)) / 2) - 1

        financial_data['profitability_growth' + period] = (financial_data['ROE' + period + '_gr_1yr'] + financial_data['ROA' + period + '_gr_1yr'] + financial_data['GPOE' + period + '_gr_1yr'].fillna(0) + financial_data['GPOA' + period + '_gr_1yr'].fillna(0) + financial_data['OPOE' + period + '_gr_1yr'] + financial_data['OPOA' + period + '_gr_1yr'] + financial_data['ROE' + period + '_gr_2yr'] + financial_data['ROA' + period + '_gr_2yr'] + financial_data['GPOE' + period + '_gr_2yr'].fillna(0) + financial_data['GPOA' + period + '_gr_2yr'].fillna(0) + financial_data['OPOE' + period + '_gr_2yr'] + financial_data['OPOA' + period + '_gr_2yr'] + financial_data['ROE' + period + '_Abnormal'] + financial_data['ROA' + period + '_Abnormal'] + financial_data['GPOE' + period + '_Abnormal'].fillna(0) + financial_data['GPOA' + period + '_Abnormal'].fillna(0) + financial_data['OPOE' + period + '_Abnormal'] + financial_data['OPOA' + period + '_Abnormal']) / 18
        financial_data['sales_growth' + period] = (financial_data['SOE' + period + '_gr_1yr'] + financial_data['SOA' + period + '_gr_1yr'] + financial_data['S' + period + '_gr_1yr'] + financial_data['SOE' + period + '_gr_2yr'] + financial_data['SOA' + period + '_gr_2yr'] + financial_data['S' + period + '_gr_2yr'] + financial_data['SOE' + period + '_Abnormal'] + financial_data['SOA' + period + '_Abnormal'] + financial_data['S' + period + '_Abnormal']) / 9
        financial_data['cashflow_growth' + period] = (financial_data['CFOOE' + period + '_gr_1yr'] + financial_data['CFOOA' + period + '_gr_1yr'] + financial_data['CFOOE' + period + '_gr_2yr'] + financial_data['CFOOA' + period + '_gr_2yr'] + financial_data['CFOOE' + period + '_Abnormal'] + financial_data['CFOOA' + period + '_Abnormal']) / 6
        financial_data['noncashsales_growth' + period] = (financial_data['ARTOS' + period + '_gr_1yr'] + financial_data['ARTOS' + period + '_gr_2yr'] + financial_data['ARTOS' + period + '_Abnormal']) / 3

    # Empire_Building
    financial_data['ASSET_gr_1yr'] = financial_data['TBALANCESTATEMENT_74'] / financial_data['TBALANCESTATEMENT_74'].shift(4) - 1
    financial_data['BV_gr_1yr'] = financial_data['TBALANCESTATEMENT_141'] / financial_data['TBALANCESTATEMENT_141'].shift(4) - 1
    financial_data['SH_gr_1yr'] = financial_data['EQY_SH_OUT'] / financial_data['EQY_SH_OUT'].shift(4) - 1
    financial_data['ASSET_gr_2yr'] = financial_data['TBALANCESTATEMENT_74'] / financial_data['TBALANCESTATEMENT_74'].shift(8) - 1
    financial_data['BV_gr_2yr'] = financial_data['TBALANCESTATEMENT_141'] / financial_data['TBALANCESTATEMENT_141'].shift(8) - 1
    financial_data['SH_gr_2yr'] = financial_data['EQY_SH_OUT'] / financial_data['EQY_SH_OUT'].shift(8) - 1
    financial_data['ASSET_Abnormal'] = financial_data['TBALANCESTATEMENT_74'] / ((financial_data['TBALANCESTATEMENT_74'].shift(4) + financial_data['TBALANCESTATEMENT_74'].shift(8)) / 2)  - 1
    financial_data['BV_Abnormal'] = financial_data['TBALANCESTATEMENT_141'] / ((financial_data['TBALANCESTATEMENT_141'].shift(4) + financial_data['TBALANCESTATEMENT_141'].shift(8)) / 2)  - 1
    financial_data['SH_Abnormal'] = financial_data['EQY_SH_OUT'] / ((financial_data['EQY_SH_OUT'].shift(4) + financial_data['EQY_SH_OUT'].shift(8)) / 2)  - 1

    financial_data['Empire_Building'] = (financial_data['ASSET_gr_1yr'] + financial_data['BV_gr_1yr'] + financial_data['SH_gr_1yr'] + financial_data['ASSET_gr_2yr'] + financial_data['BV_gr_2yr'] + financial_data['SH_gr_2yr'] + financial_data['ASSET_Abnormal'] + financial_data['BV_Abnormal'] + financial_data['SH_Abnormal']) / 9

    # Total Accruals
    financial_data['Noncash_WC'] = (financial_data['TBALANCESTATEMENT_25'] - financial_data['TBALANCESTATEMENT_9']) - (financial_data['TBALANCESTATEMENT_93'] - financial_data['TBALANCESTATEMENT_147'] - financial_data['TBALANCESTATEMENT_88'])
    financial_data['TOTALACCRUAL'] = (financial_data['Noncash_WC'] - financial_data['Noncash_WC'].shift(4)) / financial_data['TBALANCESTATEMENT_74'].shift(4)
    financial_data['CashSales_Chg'] = ((financial_data['TINCOMESTATEMENTQ_9_TTM'] - financial_data['TINCOMESTATEMENTQ_9_TTM'].shift(4)) - (financial_data['TBALANCESTATEMENT_216'] - financial_data['TBALANCESTATEMENT_216'].shift(4))) / financial_data['TBALANCESTATEMENT_74'].shift(4)
    financial_data['PPETA'] = financial_data['TBALANCESTATEMENT_31'] / financial_data['TBALANCESTATEMENT_74'].shift(4)

    # R&D
    financial_data['RNDOE'] = financial_data['TINCOMESTATEMENT_89'] / financial_data['TBALANCESTATEMENT_141']
    financial_data['RNDOA'] = financial_data['TINCOMESTATEMENT_89'] / financial_data['TBALANCESTATEMENT_74']
    financial_data['RNDOE_gr_1yr'] = (financial_data['TINCOMESTATEMENT_89'] - financial_data['TINCOMESTATEMENT_89'].shift(4)) / financial_data['TBALANCESTATEMENT_141']
    financial_data['RNDOA_gr_1yr'] = (financial_data['TINCOMESTATEMENT_89'] - financial_data['TINCOMESTATEMENT_89'].shift(4)) / financial_data['TBALANCESTATEMENT_74']
    financial_data['RNDOE_gr_2yr'] = (financial_data['TINCOMESTATEMENT_89'] - financial_data['TINCOMESTATEMENT_89'].shift(8)) / financial_data['TBALANCESTATEMENT_141']
    financial_data['RNDOA_gr_2yr'] = (financial_data['TINCOMESTATEMENT_89'] - financial_data['TINCOMESTATEMENT_89'].shift(8)) / financial_data['TBALANCESTATEMENT_74']

    financial_data['RND'] = (financial_data['RNDOE'] + financial_data['RNDOA']) / 2
    financial_data['RND_growth'] = (financial_data['RNDOE_gr_1yr'] + financial_data['RNDOA_gr_1yr'] + financial_data['RNDOE_gr_2yr'] + financial_data['RNDOA_gr_2yr']) / 4

    # Earnings Variability
    financial_data['ROEVar'] = financial_data['ROE_TTM'].rolling(window=20, min_periods=12).std()
    financial_data['ROAVar'] = financial_data['ROA_TTM'].rolling(window=20, min_periods=12).std()
    financial_data['ROEGrVar'] = financial_data['ROE_TTM_gr_1yr'].rolling(window=20, min_periods=12).std()
    financial_data['ROAGrVar'] = financial_data['ROA_TTM_gr_1yr'].rolling(window=20, min_periods=12).std()

    financial_data['profit_variability'] = (financial_data['ROEVar'] + financial_data['ROAVar']) / 2
    financial_data['profit_growth_variability'] = (financial_data['ROEGrVar'] + financial_data['ROAGrVar']) / 2

    # Leverage
    financial_data['Leverage'] = financial_data['TBALANCESTATEMENT_128'] / financial_data['TBALANCESTATEMENT_74']
    financial_data['DebtTA'] = (financial_data['TBALANCESTATEMENT_94'] + financial_data['TBALANCESTATEMENT_88'] + financial_data['TBALANCESTATEMENT_147'] + financial_data['TBALANCESTATEMENT_228']) / financial_data['TBALANCESTATEMENT_74']
    financial_data['CurrentRatio'] = financial_data['TBALANCESTATEMENT_25'] / financial_data['TBALANCESTATEMENT_93']

    # Dividend Yield
    financial_data['DivYield'] = financial_data['REPORTDATEDIVIDEND'] / financial_data['MARKET_CAP'].shift(4)
    financial_data['DivYield_growth'] = financial_data['DivYield'] / financial_data['DivYield'].shift(4) - 1

    # Unlock
    financial_data['UNLOCKAMTTTTUNLOCKAMT'] = financial_data['PPERIODUNLOCKAMT'] / financial_data['PTOTALUNLOCKAMT']
    financial_data['UNLOCKAMTTMKTCAP'] = financial_data['PPERIODUNLOCKAMT'] / financial_data['MARKET_CAP']

    financial_data['unlock_proportion'] = (financial_data['UNLOCKAMTTTTUNLOCKAMT'] + financial_data['UNLOCKAMTTMKTCAP']) / 2

    # Analyst View
    financial_data['WRATINGINSTNUM'] = financial_data['WRATINGINSTNUM'].fillna(0)
    financial_data['WRATINGNUMOFBUY'] = financial_data['WRATINGNUMOFBUY'].fillna(0)
    financial_data['WRATINGNUMOFOUTPERFORM'] = financial_data['WRATINGNUMOFOUTPERFORM'].fillna(0)
    financial_data['WRATINGNUMOFHOLD'] = financial_data['WRATINGNUMOFHOLD'].fillna(0)
    financial_data['WRATINGNUMOFUNDERPERFORM'] = financial_data['WRATINGNUMOFUNDERPERFORM'].fillna(0)
    financial_data['WRATINGNUMOFSELL'] = financial_data['WRATINGNUMOFSELL'].fillna(0)
    financial_data['Positive_NUM'] = financial_data['WRATINGNUMOFBUY'] + financial_data['WRATINGNUMOFOUTPERFORM'] + financial_data['WRATINGNUMOFHOLD']
    financial_data['Positive_PROP'] = financial_data['Positive_NUM'] / financial_data['WRATINGINSTNUM']
    financial_data['TPTP'] = financial_data['RATINGTARGETPRICE'] / financial_data['PX_LAST_RAW']

    # Valuation
    financial_data['EPTTM'] = financial_data['TINCOMESTATEMENTQ_60_TTM'] / financial_data['MARKET_CAP']
    financial_data['SPTTM'] = financial_data['TINCOMESTATEMENTQ_9_TTM'] / financial_data['MARKET_CAP']
    financial_data['ESTEP'] = 1 / financial_data['ESTPE']
    financial_data['ESTPEG'] = financial_data['ESTPEG']
    financial_data['EPDYNAMIC'] = 1 / financial_data['PEDYNAMIC']
    financial_data['CFCFOPTTM'] = 1 / financial_data['PCFCFOTTM']

    # Forecast
    financial_data['ESTINSTNUM'] = financial_data['ESTINSTNUM'].map(lambda x: np.log(1 + x))
    financial_data['ESTMEDIANROA'] = financial_data['ESTMEDIANROA']
    financial_data['ESTMEDIANROE'] = financial_data['ESTMEDIANROE']
    financial_data['EVA'] = financial_data['EVA']

    # Size
    financial_data['MARKET_CAP'] = financial_data['MARKET_CAP']

    financial_data = financial_data.reset_index()
    result = financial_data.set_index('end_date')
    result = result[['ARTOS', 'sales', 'cashflow_generation', 'profitability', 'profitability_growth', 'sales_growth',
                     'cashflow_growth', 'noncashsales_growth', 'ARTOS_Semi', 'sales_Semi', 'cashflow_generation_Semi',
                     'profitability_Semi', 'profitability_growth_Semi', 'sales_growth_Semi', 'cashflow_growth_Semi',
                     'noncashsales_growth_Semi', 'ARTOS_TTM', 'sales_TTM', 'cashflow_generation_TTM', 'profitability_TTM',
                     'profitability_growth_TTM', 'sales_growth_TTM', 'cashflow_growth_TTM', 'noncashsales_growth_TTM',
                     'Empire_Building', 'Noncash_WC', 'TOTALACCRUAL', 'CashSales_Chg', 'PPETA', 'RND', 'RND_growth',
                     'profit_variability', 'profit_growth_variability', 'Leverage', 'DebtTA', 'CurrentRatio', 'DivYield',
                     'DivYield_growth', 'unlock_proportion', 'Positive_PROP', 'TPTP', 'WRATINGINSTNUM', 'WRATINGNUMOFBUY',
                     'EPTTM', 'SPTTM', 'ESTEP', 'ESTPEG', 'EPDYNAMIC', 'CFCFOPTTM', 'ESTINSTNUM', 'ESTMEDIANROA',
                     'ESTMEDIANROE', 'EVA', 'fwd_return_qtr', 'fwd_return_semiyear', 'fwd_return_annyear', 'MARKET_CAP',
                                 'EQY_SH_OUT', 'PX_LAST_RAW']]

    result_path = os.path.join(addpath.data_path, "Ashare", "factors", symbol + '.csv')
    result.to_csv(result_path)



if __name__ == "__main__":
    symbol_list_path = os.path.join(addpath.config_path, "ashare_symbol_list.csv")
    symbol_list = pd.read_csv(symbol_list_path)['Stkcd'].tolist()

    pool = multiprocessing.Pool()
    # for symbol in [symbol_list[0]]:
    for symbol in symbol_list:
        # cal_factors(symbol)
        pool.apply_async(cal_factors, args=(symbol,))
        print('Sub-processes begins!')
    pool.close()  # 关闭进程池，表示不能再往进程池中添加进程，需要在join之前调用
    pool.join()  # 等待进程池中的所有进程执行完毕
    print("Sub-process(es) done.")