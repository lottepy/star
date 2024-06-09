'''
input file:
Factor Categorization and Single Factor Significance.csv
Summary.csv

'''

cs_factor_category_list = ['aum_factor', 'average_tenure', 'beta_factor', 'expense_ratio', 'fund_company_factor',
                           'manager_factor', 'maximum_drawdown_factor', 'return_factor', 'sharperatio_factor',
                           'style_drift_factor', 'tail_risk_factor', 'turnover_ratio', 'volatility_factor', 'rsj_factor',
                           'skew_factor']

new_category_list = ['Equity_US', 'Index_Global', 'Equity_EM', 'Equity_Global', 'Equity_APAC', 'Equity_DMexUS',
                     'Bond_Global', 'Balance_Global', 'Bond_US', 'Alternative'
                     ]

new_category_mapping = {
    'Equity_US': 'Equity_US',
    'Index_Global': 'Index_Global',
    'Equity_EM': 'Equity_EM',
    'Equity_Global': 'Equity_Global',
    'Equity_APAC': 'Equity_APAC',
    'Equity_DMexUS': 'Equity_DMexUS',
    'Bond_Global': 'Bond_Global',
    'Balance_Global': 'Balance_Global',
    'Bond_US': 'Bond_US',
    'Alternative_Gold_Global': 'Alternative',
    'Alternative_Futures_Global': 'Alternative'
}

ms_sector_dict = {
    '101': 'Basic Materials',
    '102': 'Consumer Cyclical',
    '103': 'Financial Services',
    '104': 'Real Estate',
    '205': 'Consumer Defensive',
    '206': 'Healthcare',
    '207': 'Utilities',
    '308': 'Communication Services',
    '309': 'Energy',
    '310': 'Industrials',
    '311': 'Technology',
}

ts_factor_list = []
