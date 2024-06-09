ROOTPATH = r"C:\Users\yeah_\Desktop\Aqumon\Quantimacro\Quantimacro"

# Raw data path
RAWDATAPATH = ROOTPATH + r"\Data\Raw_Data"
# US raw data
QTR_PATH = ROOTPATH + r"\Data\Raw_Data\US\Quarterly_Data"
MTR_PATH = ROOTPATH + r"\Data\Raw_Data\US\Monthly_Data"
WKL_PATH = ROOTPATH + r"\Data\Raw_Data\US\Weekly_Data"

# Results path
RESULTSPATH = ROOTPATH + r"\Result"

# Raw data prefix

RAWDATAPREFIX = {'US': 'United States_', 'China': 'China_', 'EZ': 'Eurozone_'}

# US data parameters
RAW_LIST = ['ADP CHNG Index', 'AHE MOM% Index', 'BAKEOIL Index', 'CHPMINDX Index', 'CONCCONF Index', 'CONSCURR Index',
			'CONSPXMD Index', 'CONSSENT Index', 'CPI CHNG Index', 'CPI XYOY Index', 'CPI YOY Index', 'CPUPXCHG Index',
			'DGNOCHNG Index', 'DGNOXTCH Index', 'DOEASCRD Index', 'EMPRGBCI Index', 'FDIDSGMO Index', 'FRNTTOTL Index',
			'INJCJC Index', 'IP  CHNG Index', 'JOLTTOTL Index', 'LMCILMCC Index', 'MPMIUSCA Index', 'MPMIUSMA Index',
			'MPMIUSSA Index', 'NAPMNMI  Index', 'NAPMPMI Index', 'NAPMPRIC Index', 'NFP PCH Index', 'NFP TCH Index',
			'NHCHSTCH Index', 'NHSLCHNG Index', 'NHSLTOT Index', 'NHSPATOT Index', 'NHSPSTOT Index', 'NYPMCURR Index',
			'OUTFGAF Index', 'PCE CMOM Index', 'PCE CRCH Index', 'PCE CYOY Index', 'PITLCHNG Index', 'PRUSTOT Index',
			'RCHSINDX Index', 'RSTAMOM Index', 'RSTAXAGM Index', 'RSTAXMOM Index', 'SAARTOTL Index', 'SBOITOTL Index',
			'USHBMIDX Index', 'USPHTYOY Index', 'USTBTOT Index', 'USTGTTCB Index', 'USURTOT Index']

# Weekly data
WEEKLY_LIST = ['BAKEOIL Index', 'DOEASCRD Index', 'INJCJC Index']
WEEKLY_FO_DIFF_LIST = []
WEEKLY_GROWTH_LIST = ['BAKEOIL Index', 'INJCJC Index']
WEEKLY_UNCHG_LIST = ['DOEASCRD Index']


# Monthly data
# MONTHLY_LIST = ['USTBTOT Index', 'USURTOT Index', 'DGNOCHNG Index', 'OUTFGAF Index',
#        'NHSPSTOT Index', 'CONCCONF Index', 'CHPMINDX Index', 'NAPMPMI Index',
#        'CONSSENT Index', 'CPUPXCHG Index', 'PITLCHNG Index', 'CPI CHNG Index',
#        'PCE CRCH Index']
MONTHLY_LIST = ['NFP TCH Index', 'USTGTTCB Index', 'RSTAXMOM Index', 'AHE MOM% Index', 'NHCHSTCH Index', 'CONSPXMD Index',
        'FRNTTOTL Index', 'CPUPXCHG Index', 'MPMIUSMA Index', 'CPI XYOY Index', 'RSTAMOM Index', 'NYPMCURR Index',
        'NHSPSTOT Index', 'NAPMPRIC Index', 'CONSCURR Index', 'PITLCHNG Index', 'CPI YOY Index', 'SBOITOTL Index',
        'FDIDSGMO Index', 'NHSLCHNG Index', 'CONCCONF Index', 'PRUSTOT Index', 'CHPMINDX Index', 'PCE CMOM Index',
        'USTBTOT Index', 'USURTOT Index', 'NAPMPMI Index', 'DGNOCHNG Index', 'NHSPATOT Index', 'RSTAXAGM Index',
        'DGNOXTCH Index', 'CPI CHNG Index', 'USPHTYOY Index', 'PCE CRCH Index', 'OUTFGAF Index', 'NFP PCH Index',
        'RCHSINDX Index', 'JOLTTOTL Index', 'SAARTOTL Index', 'ADP CHNG Index', 'PCE CYOY Index', 'MPMIUSCA Index',
        'EMPRGBCI Index', 'USHBMIDX Index', 'MPMIUSSA Index', 'CONSSENT Index', 'LMCILMCC Index']
# MONTHLY_FO_DIFF_LIST = ['CPI XYOY Index', 'CPI YOY Index', 'PCE CYOY Index', 'USTBTOT Index', 'USTGTTCB Index',
#             'USURTOT Index', 'AHE MOM% Index', 'DGNOCHNG Index', 'FDIDSGMO Index',
#             'OUTFGAF Index', 'PRUSTOT Index', 'USPHTYOY Index']
# MONTHLY_GROWTH_LIST = ['FRNTTOTL Index', 'MPMIUSMA Index', 'NYPMCURR Index', 'NHSPSTOT Index', 'NAPMPRIC Index',
#              'CONSCURR Index', 'SBOITOTL Index', 'CONCCONF Index', 'CHPMINDX Index', 'NAPMPMI Index',
#              'NHSPATOT Index', 'RCHSINDX Index', 'JOLTTOTL Index', 'SAARTOTL Index', 'MPMIUSCA Index',
#              'EMPRGBCI Index', 'USHBMIDX Index', 'MPMIUSSA Index', 'CONSSENT Index']
# MONTHLY_UNCHG_LIST = ['RSTAXMOM Index', 'NHCHSTCH Index', 'CPUPXCHG Index', 'PITLCHNG Index', 'NHSLCHNG Index',
#             'PCE CMOM Index', 'RSTAXAGM Index', 'CPI CHNG Index', 'PCE CRCH Index', 'NFP PCH Index',
#             'LMCILMCC Index']
# MONTHLY_FO_DIFF_LIST = ['USURTOT Index']
# MONTHLY_GROWTH_LIST = ['ADP CHNG Index']
# MONTHLY_UNCHG_LIST = ['CHPMINDX Index','EMPRGBCI Index','IP  CHNG Index','NAPMNMI  Index','NAPMPMI Index',
#                       'NAPMPRIC Index','NFP TCH Index','OUTFGAF Index','PCE CRCH Index','RCHSINDX Index',
#                       'RSTAMOM Index','RSTAXMOM Index']
# Quarterly data
QTRLY_LIST = ['ECI SA% Index', 'FDTR Index', 'GDP CQOQ Index', 'GDPCPCEC Index', 'USCABAL Index']
QTRLY_FO_DIFF_LIST = ['ECI SA% Index', 'FDTR Index']
QTRLY_GROWTH_LIST = ['USCABAL Index']
QTRLY_UNCHG_LIST = ['GDP CQOQ Index', 'GDPCPCEC Index']

