from enum import Enum

US_TRADINGS = {'open': 'PX_OPEN',
               'high': 'PX_HIGH',
               'low': 'PX_LOW',
               'close': 'PX_LAST',
               'volume': 'PX_VOLUME',
               'bbg_total_shares_outstanding': 'EQY_SH_OUT',
               'bbg_daily_total_return': 'DAY_TO_DAY_TOT_RETURN_GROSS_DVDS'}

US_FINANCIALS = {'bbg_announcement_dt': 'ANNOUNCEMENT_DT',
                 'bbg_bs_tot_asset': 'BS_TOT_ASSET',
                 'bbg_bs_tot_liab2': 'BS_TOT_LIAB2',
                 'bbg_total_equity': 'TOT_EQUITY',
                 'bbg_sales_rev_turn': 'SALES_REV_TURN',
                 'bbg_ebit': 'EBIT',
                 'bbg_net_oper_income': 'NET_OPER_INCOME',
                 'bbg_net_income': 'NET_INCOME',
                 'bbg_cf_cash_from_oper': 'CF_CASH_FROM_OPER',
                 'bbg_cf_cap_expend_prpty_add': 'CF_CAP_EXPEND_PRPTY_ADD',
                 'bbg_is_int_expense': 'IS_INT_EXPENSE',
                 'bbg_ebitda': 'EBITDA'}

CHOICE_FINANCIALS = {'bbg_announcement_dt': 'ANNOUNCEMENT_DT',
                     'choice_totalasset': 'BS_TOT_ASSET',
                     'choice_totalliab': 'BS_TOT_LIAB2',
                     'choice_stkhldereq': 'TOT_EQUITY',
                     'choice_operarevenue': 'SALES_REV_TURN',
                     'choice_optoebt': 'EBIT',
                     'bbg_net_oper_income': 'NET_OPER_INCOME',
                     'choice_teofholder': 'NET_INCOME',
                     'choice_opercashflow': 'CF_CASH_FROM_OPER',
                     'bbg_cf_cap_expend_prpty_add': 'CF_CAP_EXPEND_PRPTY_ADD',
                     'bbg_is_int_expense': 'IS_INT_EXPENSE',
                     'bbg_ebitda': 'EBITDA',
                     'choice_gpmargin':'GP_SALEREV_Ratio',
                     'choice_profitbetax':'EBT',
                     'choice_qfa_opprofit':'OPPROFIT',
                     'choice_ltborrow':'LONGTERM_BORROW',
                     'choice_cash':'CASH',

                     'choice_trans_fin_assets1':'SPECIFIC_TRANS_FIN_ASSETS',
                     'choice_central_bank_borrow':'CENTRAL_BANK_BORROW',
                     'choice_borrow_fund':'FUND_BORROWED',
                     'choice_deriv_liab':'DERIV_FIN_LIAB',
                     'choice_sale_repurchase_asset':'SOLD_REPUR_FIN_ASSETS',
                     'choice_bill_receivable':'BILL_RECEIVABLE',
                     'choice_accounts_receivable':'ACCOUNTS_RECEIVABLE',
                     'choice_shortterm_bond':'SHORTTERM_BOND',
                     'choice_deposits_interbank':'DEPOSIT_INTERBANK',
                     'choice_fair_value_profits_equity':'FAIR_VALUE_PROFITS_EQUITY',
                     'choice_fair_value_profits_equity1':'SPECIFIC_FAIR_VALUE_PROFITS_EQUITY',
                     'choice_inventory':'INVENTORY',
                     'choice_fair_value_profits_liab':'FAIR_VALUE_PROFITS_LIAB',
                     'choice_fair_value_profits_liab1': 'SPECIFIC_FAIR_VALUE_PROFITS_LIAB',
                     'choice_fund_provide': 'FUND_PROVIDED',
                     'choice_preferred_stock': 'PREFERRED_STOCK',
                     'choice_sustainable_debt': 'SUSTAINABLE_DEBT',
                     'choice_noncurrent_assets_1y': 'NONCURRENT_ASSETS_1Y',
                     'choice_onsale_equity': 'ONSALE_EQUITY',
                     'choice_amortized_cost_equity': 'AMORTIZED_COST_FIN_EQUITY',
                     'choice_fair_value_otherprofits_equity': 'FAIR_VALUE_OTHERPROFITS_FIN_EQUITY',
                     'choice_amortized_cost_liab': 'AMORTIZED_COST_FIN_LIAB',
                     'choice_contract_equity': 'CONTRACT_EQUITY',
                     'choice_amortized_cost_noncurrent_equity': 'AMORTIZED_COST_NONCURRENT_FIN_EQUITY',
                     'choice_fair_value_otherprofits_noncurrenct_equity': 'FAIR_VALUE_OTHERPROFITS_NONCURRENCT_FIN_EQUITY',
                     'choice_amortized_cost_noncurrent_liab': 'AMORTIZED_COST_NONCURRENT_FIN_LIAB',
                     'choice_notes&accounts_receivable': 'NOTESNACCOUNTS_RECEIVABLE',
                     'choice_creditor_invest': 'CREDITOR_INVEST',
                     'choice_creditor_invest_other': 'CREDITOR_INVEST_OTHER',
                     'choice_equity_invest_other': 'EQUITY_INVEST_OTHER',
                     'choice_noncurrent_equity_other': 'NONCURRENT_EQUITY_OTHER',
                     'choice_notes&accounts_payable': 'NOTESNACCOUNTS_PAYABLE',
                     'choice_trans_fin_assets': 'TRANS_FIN_ASSETS',
                     'choice_trans_fin_liab': 'TRANS_FIN_LIAB',

                     'choice_total_other_payable': 'TOTAL_OTHER_PAYABLE',
                     'choice_lease_liab': 'LEASE_LIAB',
                     'choice_onsale_fin_assets': 'ONSALE_FIN_ASSETS',
                     'choice_hold_to_maturity_invest': 'HOLD_TO_MATURITY_INVEST',
                     'choice_invest_real_estate': 'REAL_ESTATE_INVEST',
                     'choice_longterm_equity_invest': 'LONGTERM_EQUITY_INVEST',
                     'choice_lending_money': 'FUND_LENDED',
                     'choice_deriv_fin_assets': 'DERIV_FIN_ASSETS',
                     'choice_buy_sell_fin_assets': 'BUY_SOLD_FIN_ASSETS',
                     'choice_issue_loans_advances': 'ISSUE_LOANS_ADVANCES',
                     'choice_settlement_provisions': 'DEPOSIT_RESERVATION',
                     'choice_shortterm_borrow': 'SHORTTERM_BORROW',
                     'choice_trans_fin_liab1': 'SPECIFIC_TRANS_FIN_LIAB',
                     'choice_notes_payable': 'NOTES_PAYABLE',
                     'choice_accounts_payable': 'ACCOUNTS_PAYABLE',
                     'choice_interest_payable': 'INTEREST_PAYABLE',
                     'choice_dividends_payable': 'DIVIDENDS_PAYABLE',
                     'choice_other_payables': 'OTHER_PAYABLES',
                     'choice_noncurrent_liab_1y': 'NONCURRENT_LIAB_1Y',
                     'choice_bonds_payable': 'BONDS_PAYABLE',
                     'choice_fixed_assets_cash_payable': 'FIXED_ASSETS_CASH_PAYABLE',
                     'choice_qfa_net_cash_flows_fin_act': 'QFA_NET_CASH_FLOWS_FIN_ACT',
                     'choice_divannuaccum': 'DIVANNUACCUM',
                     'choice_divcashpsbftaxp': 'DIVCASHPSBFTAXP',
                     'choice_oper_cost_quarter': 'OPER_COST_QUARTER',
                     'choice_interest_income_quarter': 'INTEREST_INCOME_QUARTER',
                     'choice_interest_payments_quarter': 'INTEREST_PAYMENTS_QUARTER',
                     'choice_profit_tot_qurater': 'PROFIT_TOT_QURATER',
                     'choice_research_cost_qurater': 'RESEARCH_COST_QURATER',
                     'choice_oper_income_qurater': 'OPER_INCOME_QURATER'
                     }

LOOKBACK_DAYS = 1825
REBALANCE_FREQ = 'QUARTERLY'
CASH = 50000
COMMISSION = 0.003
NO_ASSETS = 10
MARKET_CAP_THRESHOLD = 0.99
UPPER_BOUND = 0.4
LOWERBOUND = 0.03

reb_freq = {
    "ANNUALLY": "A",
    "SEMIANNUALLY": "6M",
    "QUARTERLY": "Q",
    "BIMONTHLY": "2M",
    "MONTHLY": "M",
    "BIWEEKLY": "2W",
    "WEEKLY": "W",
    "DAILY": "D"
}
import os
from algorithm import addpath
STRATEGY_CODE = {
    "CN_Value":os.path.join(addpath.data_path, "strategy_temps", "CN_Value", "portfolios"),
    "CN_Quality":os.path.join(addpath.data_path, "strategy_temps", "CN_Quality", "portfolios"),
    "HK_Hstech_B":os.path.join(addpath.data_path, "strategy_temps", "HK_Hstech_B", "portfolios"),
    "HK_Hstech_S": os.path.join(addpath.data_path, "strategy_temps", "HK_Hstech_S", "portfolios"),
    "US_Profit_B":os.path.join(addpath.data_path, "strategy_temps", "US_Profit_B", "portfolios"),
    "US_Safety_B": os.path.join(addpath.data_path, "strategy_temps", "US_Safety_B", "portfolios"),
    "US_Tech_B": os.path.join(addpath.data_path, "strategy_temps", "US_Tech_B", "portfolios"),
    "US_Profit_S": os.path.join(addpath.data_path, "strategy_temps", "US_Profit_S", "portfolios"),
    "US_Safety_S": os.path.join(addpath.data_path, "strategy_temps", "US_Safety_S", "portfolios"),
    "US_Tech_S": os.path.join(addpath.data_path, "strategy_temps", "US_Tech_S", "portfolios"),

    "US_5G": os.path.join(addpath.data_path, "strategy_temps", "US_5G", "portfolios")
}


class TimeFreq(Enum):
    MINLY = 60
    HOURLY = 3600
    DAILY = 3600 * 24


class InstrumentType(Enum):
    FX = 1
    HK_STOCK = 2
    CN_STOCK = 3
    US_STOCK = 4
    FUTURE = 5


# {
#     "rank_factor": "MARKET_CAP",
#     "criteria": {"ROA": ['Positive', 0.3],
#                  "Momentum_2_12": ['Positive', 0.2],
#                  "ivol": ['Negative', 0.3]}
# }

RF = 0


PARAMETER_ALGO={
    'CN_Quality':{
        'criteria':{
        'ROA_SEMI': ['Positive', 0.3],
        'MARKET_CAP': ['Positive', 0.1],
        'REVOE_SEMI': ['Positive', 0.1],
        'OPCFOE_SEMI': ['Positive', 0.1],
        'OPCF_MARGIN': ['Positive', 0.1],
        'LEV':['Negative', 0.1],
        'REVOEGROWTH': ['Positive', 0.1]
        },

        "start_year": 2015,
        "start_month": 12,
        "start_day": 31,
        "end_year": 2020,
        "end_month": 12,
        "end_day": 31,

        "port_form_freq": "MONTHLY",
        "lookback_days": 1825,
        "base_ccy": "CNY",
        'ini_cash': 100000,
        "underlying_type": "CN_STOCK",
        "ind_class": "SW1",
        "number_assets": 10,
        "num_top_ind": 2,
        "weighting_approach": "MARKET_CAP",
        "upper_bound": 0.2,
        "lower_bound": 0.05
    },
    'CN_Value': {
        'criteria': {
            'MARKET_CAP': ['Positive', 0.1],
            'ETP_TTM': ['Positive', 0.1],
            'REVOAGROWTH': ['Positive', 0.1],
            'ROA_SEMI': ['Positive', 0.1],
            'OPCFOE_SEMI': ['Positive', 0.1],
            'EBITOAVAR': ['Negative', 0.1],
            'LEV': ['Negative', 0.1]
        },

        "start_year": 2015,
        "start_month": 12,
        "start_day": 31,
        "end_year": 2020,
        "end_month": 12,
        "end_day": 31,

        "port_form_freq": "MONTHLY",
        "lookback_days": 1825,
        "base_ccy": "CNY",
        'ini_cash': 100000,
        "underlying_type": "CN_STOCK",
        "ind_class": "SW1",
        "number_assets": 10,
        "num_top_ind": 2,
        "weighting_approach": "MARKET_CAP",
        "upper_bound": 0.2,
        "lower_bound": 0.05
    },
    'HK_Hstech_B':{
        'criteria': {
            'MARKET_CAP': ['Positive', 0.2],
            'REVOAGROWTH': ['Positive', 0.2],
            'GPOE': ['Positive', 0.2],
            'ROA': ['Positive', 0.2],
            'ACCT_RCV_TO': ['Positive', 0.2],
            'EBITDEVAR': ['Negative', 0.2],
            'LEV': ['Negative', 0.2],
        },

        "start_year": 2014,
        "start_month": 12,
        "start_day": 31,
        "end_year": 2020,
        "end_month": 12,
        "end_day": 31,

        "port_form_freq": "MONTHLY",
        "lookback_days": 1825,
        "base_ccy": "HKD",
        'ini_cash': 100000,
        "underlying_type": "HK_STOCK",
        "ind_class": "SEHKENG",
        "number_assets": 6,
        "num_top_ind": 6,
        "weighting_approach": "MARKET_CAP",
        "upper_bound": 0.2,
        "lower_bound": 0.05
    },
    'HK_Hstech_S': {
        'criteria': {
            'MARKET_CAP': ['Positive', 0.2],
            'REVOAGROWTH': ['Positive', 0.2],
            'GPOE': ['Positive', 0.2],
            'ROA': ['Positive', 0.2],
            'ACCT_RCV_TO': ['Positive', 0.2],
            'EBITDEVAR': ['Negative', 0.2],
            'LEV': ['Negative', 0.2],
        },

        "start_year": 2014,
        "start_month": 12,
        "start_day": 31,
        "end_year": 2020,
        "end_month": 12,
        "end_day": 31,

        "port_form_freq": "MONTHLY",
        "lookback_days": 1825,
        "base_ccy": "HKD",
        'ini_cash': 250000,
        "underlying_type": "HK_STOCK",
        "ind_class": "SEHKENG",
        "number_assets": 10,
        "num_top_ind": 10,
        "weighting_approach": "MARKET_CAP",
        "upper_bound": 0.2,
        "lower_bound": 0.05
    },
    'US_Profit_B':{
        'criteria' : {
            'ROE': ['Positive', 0.5],
            'NIexACConASSET_TTM': ['Positive', 0.1],
            'GPOE': ['Positive', 0.1],
            'OPCFOE_SEMI': ['Positive', 0.1],
            'GROSS_PROFIT_MARGIN_TTM': ['Positive', 0.1],
            'EBITDAOA': ['Positive', 0.1]
        },
    "start_year": 2009,
    "start_month": 12,
    "start_day": 31,
    "end_year": 2020,
    "end_month": 12,
    "end_day": 31,

    "port_form_freq": "MONTHLY",
    "lookback_days": 1825,
    "base_ccy": "USD",
    'ini_cash': 2500,
    "underlying_type": "US_STOCK",
    "ind_class": "GICSECTOR",
    "number_assets": 10,
    "num_top_ind": 2,
    "weighting_approach": "Z_SCORE",
    "upper_bound": 0.2,
    "lower_bound": 0.05
    },
    'US_Profit_S': {
        'criteria': {
            'ROE': ['Positive', 0.5],
            'NIexACConASSET_TTM': ['Positive', 0.1],
            'GPOE': ['Positive', 0.1],
            'OPCFOE_SEMI': ['Positive', 0.1],
            'GROSS_PROFIT_MARGIN_TTM': ['Positive', 0.1],
            'EBITDAOA': ['Positive', 0.1]
        },
        "start_year": 2009,
        "start_month": 12,
        "start_day": 31,
        "end_year": 2020,
        "end_month": 12,
        "end_day": 31,

        "port_form_freq": "MONTHLY",
        "lookback_days": 1825,
        "base_ccy": "USD",
        'ini_cash': 5000,
        "underlying_type": "US_STOCK",
        "ind_class": "GICSECTOR",
        "number_assets": 20,
        "num_top_ind": 5,
        "weighting_approach": "Z_SCORE",
        "upper_bound": 0.1,
        "lower_bound": 0.03
    },
    'US_Safety_B': {
        'criteria': {
            'LEV': ['Negative', 0.5],
            'GPOAVAR_SEMI': ['Negative', 0.1],
            'LONGTERM_LIABRATIO': ['Negative', 0.1],
            'CURRENT_RATIO': ['Positive', 0.1],
        },
        "start_year": 2009,
        "start_month": 12,
        "start_day": 31,
        "end_year": 2020,
        "end_month": 12,
        "end_day": 31,

        "port_form_freq": "MONTHLY",
        "lookback_days": 1825,
        "base_ccy": "USD",
        'ini_cash': 2500,
        "underlying_type": "US_STOCK",
        "ind_class": "GICSECTOR",
        "number_assets": 10,
        "num_top_ind": 2,
        "weighting_approach": "Z_SCORE",
        "upper_bound": 0.2,
        "lower_bound": 0.05
    },
    'US_Safety_S': {
        'criteria': {
            'LEV': ['Negative', 0.5],
            'GPOAVAR_SEMI': ['Negative', 0.1],
            'LONGTERM_LIABRATIO': ['Negative', 0.1],
            'CURRENT_RATIO': ['Positive', 0.1],
        },
        "start_year": 2009,
        "start_month": 12,
        "start_day": 31,
        "end_year": 2020,
        "end_month": 12,
        "end_day": 31,

        "port_form_freq": "MONTHLY",
        "lookback_days": 1825,
        "base_ccy": "USD",
        'ini_cash': 5000,
        "underlying_type": "US_STOCK",
        "ind_class": "GICSECTOR",
        "number_assets": 20,
        "num_top_ind": 5,
        "weighting_approach": "Z_SCORE",
        "upper_bound": 0.1,
        "lower_bound": 0.03
    },
    'US_Tech_B': {
        'criteria': {
            'ROE': ['Positive', 0.2],
            'ROEGROWTH_SEMI': ['Positive', 0.2],
            'REVOAGROWTH': ['Positive', 0.2],
            'RnD_onEQUITY': ['Positive', 0.2],
            'OPCF_MARGIN': ['Positive', 0.2],
            'GPOA': ['Positive', 0.2],
        },
        "start_year": 2009,
        "start_month": 12,
        "start_day": 31,
        "end_year": 2020,
        "end_month": 12,
        "end_day": 31,

        "port_form_freq": "MONTHLY",
        "lookback_days": 1825,
        "base_ccy": "USD",
        'ini_cash': 2500,
        "underlying_type": "US_STOCK",
        "ind_class": "GICSUBIND",
        "number_assets": 10,
        "num_top_ind": 2,
        "weighting_approach": "Z_SCORE",
        "upper_bound": 0.2,
        "lower_bound": 0.05
    },
    'US_Tech_S': {
        'criteria': {
            'ROE': ['Positive', 0.2],
            'ROEGROWTH_SEMI': ['Positive', 0.2],
            'REVOAGROWTH': ['Positive', 0.2],
            'RnD_onEQUITY': ['Positive', 0.2],
            'OPCF_MARGIN': ['Positive', 0.2],
            'GPOA': ['Positive', 0.2],
        },
        "start_year": 2009,
        "start_month": 12,
        "start_day": 31,
        "end_year": 2020,
        "end_month": 12,
        "end_day": 31,

        "port_form_freq": "MONTHLY",
        "lookback_days": 1825,
        "base_ccy": "USD",
        'ini_cash': 5000,
        "underlying_type": "US_STOCK",
        "ind_class": "GICSUBIND",
        "number_assets": 20,
        "num_top_ind": 5,
        "weighting_approach": "Z_SCORE",
        "upper_bound": 0.15,
        "lower_bound": 0.03
    },
    'US_5G': {
        'criteria': {
            'ROE': ['Positive', 0.2],
            'ROEGROWTH_SEMI': ['Positive', 0.2],
            'ROEVAR_SEMI': ['Negative', 0.2],
            'RnD_onEQUITY': ['Positive', 0.2],
            'GPOA': ['Positive', 0.2],
        },
        "start_year": 2009,
        "start_month": 12,
        "start_day": 31,
        "end_year": 2020,
        "end_month": 12,
        "end_day": 31,

        "port_form_freq": "MONTHLY",
        "lookback_days": 1825,
        "base_ccy": "USD",
        'ini_cash': 10000000,
        "underlying_type": "US_STOCK",
        "ind_class": "GICSUBIND",
        "number_assets": 60,
        "num_top_ind": 60,
        "weighting_approach": "Z_SCORE",
        "upper_bound": 0.03,
        "lower_bound": 0.01
    },

}

BACKTEST_SE={
    'CN_STOCK':{
        'start':'2015-12-31',
        'end':'2021-01-15'
    },
    'HK_STOCK': {
        'start': '2014-12-31',
        'end': '2021-01-15'
    },
    'US_STOCK': {
        'start': '2009-12-31',
        'end': '2021-01-15'
    },


}