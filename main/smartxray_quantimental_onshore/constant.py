
CATEGORIZATION_CAPITAL_MAPPING = {
    '小盘': 'Mid/Small-Cap Equity',
    '中盘': 'Mid/Small-Cap Equity',
    '大盘': 'Large-Cap Equity',
    '中小盘': 'Mid/Small-Cap Equity',
    '沪深300': 'Large-Cap Equity',
    '中证100': 'Large-Cap Equity',
    '中证500': 'Mid/Small-Cap Equity',
    '中证800': 'Mid/Small-Cap Equity',
    '中证1000': 'Mid/Small-Cap Equity',
    '上证50': 'Large-Cap Equity',
    '深证100': 'Large-Cap Equity',
    '深圳300': 'Large-Cap Equity',
}

CATEGORIZATION_SECTOR_MAPPING = {
    '钢铁': 'Basic Materials',
    '资源': 'Basic Materials',
    '有色金属': 'Basic Materials',

    '食品': 'Consumption',
    '饮料': 'Consumption',
    '酒': 'Consumption',
    '农业': 'Consumption',

    '银行': 'Financial Services',
    '保险': 'Financial Services',
    '金融': 'Financial Services',

    '房地产': 'Real Estate',
    '地产': 'Real Estate',

    '医疗': 'Healthcare',
    '医药': 'Healthcare',
    '中药': 'Healthcare',
    '健康': 'Healthcare',
    '保健': 'Healthcare',
    '生物': 'Healthcare',

    '煤炭': 'Energy',

    '汽车': 'Industrials',
    '高铁': 'Industrials',
    '国防': 'Industrials',
    '军工': 'Industrials',
    '环境治理': 'Industrials',
    '基建': 'Industrials',
    '工程': 'Industrials',
    '工业': 'Industrials',

    '信息': 'Technology',
    '电子': 'Technology',
    '科技': 'Technology',
    '人工智能': 'Technology',
    '互联网': 'Technology',
    '计算机': 'Technology',
    '半导体': 'Technology',
    '通信': 'Technology',

    '传媒': 'Communication Services',
    '娱乐': 'Communication Services',

    '公用事业': 'Utilities',
    '消费': 'Consumption',
}

CATEGORIZATION_STYLE_MAPPING = {
    '价值': 'Value',
    '成长': 'Growth',
}


daily_factor_level = ['USYC2Y10 Index', 'MXWD Index_logvolume', 'MXWD Index_amihud', 'GCNY1YR Index', 'GCNY10YR Index']
daily_factor_change = ['USYC2Y10 Index', 'MXWD Index_logvolume', 'MXWD Index_amihud', 'GCNY1YR Index', 'GCNY10YR Index']
daily_factor_growth = ['USDCNH Curncy', 'MXWD Index', 'SHSZ300 Index', 'CL1 COMB Comdty', 'XAU Curncy', 'CSIH1001 Index',
                       'CSIMMF Index', 'MXWD Index_amihud']

monthly_factor_level = ['CNCPIYOY Index', 'MXWD Index_rv']
monthly_factor_change = ['CNCPIYOY Index', 'MXWD Index_rv']
monthly_factor_growth = ['MXWD Index_rv']

quarterly_factor_level = ['CNGDPYOY Index']
quarterly_factor_change = ['CNGDPYOY Index']
quarterly_factor_growth = []

market_index_onshore = ['SHSZ300 Index', 'CSIH1001 Index', 'CSIMMF Index', 'MXWD Index']

funda_factor_list = ['aum_factor', 'average_tenure', 'expense_ratio', 'style_drift_factor', 'tail_risk_factors', 'turnover_ratio']
nav_factor_list = ['maximum_drawdown_factor', 'return_factor', 'sharperatio_factor', 'volatility_factor', 'skew_factor',
                   'rsj_factor', 'maximum_daily_drop_factor', 'momentum_return_factor', 'momentum_sharperatio_factor',
                   'longterm_reversal_return_factor', 'longterm_reversal_sharperatio_factor']
other_factor_list = ['beta_factor', 'fund_manager_factor', 'fund_company_factor']