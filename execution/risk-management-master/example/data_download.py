from utils.bbg_downloader import download_his
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
sns.set_style('whitegrid')

start = '2018-12-31'
end = '2019-11-22'
ticker_list = ["ASHR US Equity", "EWH US Equity", "EWU US Equity", "EWZ US Equity", "IVV US Equity",
               "SCHF US Equity", "VEA US Equity", "VOE US Equity", "VWO US Equity", "XLB US Equity",
               "XLI US Equity", "XLP US Equity", "EWG US Equity", "VB US Equity", "VTI US Equity",
               "EWA US Equity", "INDA US Equity", "VNQ US Equity", "XLF US Equity", "IEFA US Equity"]
index_list = ['SPX Index', 'CSIN0301 Index', 'HSI Index', 'MXWD Index']
list_1 = ['700 HK Equity', 'HSI Index', 'SPX Index', 'MXWD Index']
list_2 = ['M1WD Index', 'LBUSTRUU Index', 'MXWD Index']
list_3 = ['MXWD Index', 'LBUSTRUU Index', 'LBEATRUH Index', 'JPEIJACC Index', 'LEGATRUU Index', 'SPX Index']
list_4 = ['ASHR US Equity', 'EWH US Equity', 'CSIR0300 Index', 'HSI 1 Index']
list_5 = ['USDCNY Curncy']
list_6 = ['ASHR US Equity', 'EWZ US Equity', 'INDA US Equity', 'VWO US Equity']
list_7 = ['NEAR US Equity', 'ERND LN Equity']
list_8 = ['FLOT US Equity', 'FLOA LN Equity']
list_9 = ['2823 HK Equity', '2823IV Index', 'TXIN9IC Index', 'HKDCNY Curncy']
list_10 = ['HSI Index', 'HSI 1 Index']
list_11 = ['MERGAAI LX Equity', 'GAOAX US Equity', 'PGMAX US Equity']    # BlackRock/JPM/PIMCO Global Allocation Fund A2
list_12 = ['CSI BBB Index', 'FLOT US Equity', 'SHYG US Equity', 'SHY US Equity']
list_13 = ['XU1 Index', 'USDHKD Curncy']
list_14 = ['NDEUCHF Index', 'HSI Index', 'HSI 1 Index']
list_15 = ['CSIR0300 Index', 'SHSZ300 Index']
list_16 = ['SPX Index']
list_17 = ['CSI BBB Index', 'FDTR Index', 'DXY Index']
list_18 = ['H15T10Y Index', 'H15T5Y Index', 'H15T3M Index', 'H15T1M Index']
list_19 = ['601888 CH Equity', '600009 CH Equity', '600900 CH Equity',
           '300015 CH Equity', '600519 CH Equity', 'CSIR0300 Index']
list_20 = ['1299 HK Equity', '2388 HK Equity', '2588 HK Equity', '3613 HK Equity',
		   '11 HK Equity', '778 HK Equity', 'HSI 1 Index']
list_21 = ['VIX Index', 'DXY Index', 'US0003M Index', 'SKEW Index']
list_22 = ['SPX Index', 'LBUSTRUU Index']
list_23 = ['MXWD Index']
vix_list = ['UX1 Index', 'UX2 Index', 'VXX US Equity', 'VXXB US Equity']
list_25 = ['AAPL US Equity', 'TSM US Equity', 'BRK/B US Equity', 'DIS US Equity',
		   'COST US Equity', 'DPZ US Equity', 'MCO US Equity', 'SPXT Index']
list_26 = ['AIA AU Equity', 'AIA NZ Equity', 'SATS SP Equity', 'NDUEACWZ Index']
list_27 = ['H15T1M Index', 'H15T1Y Index', 'H15T2Y Index', 'H15T5Y Index', 'H15T10Y Index', 'H15T30Y Index',
		   'SPX Index', 'DJI Index', 'NDX Index', 'VIX Index', 'DXY Index', 'XAU Index', 'CO1 Comdty']
list_28 = ['ENZL US Equity', 'EWJ US Equity', 'EWT US Equity', 'NDUEACWZ Index']
list_29 = ['MXUS Index', 'SPX Index']
list_30 = ['CPI INDX Index']
list_31 = ['MXCN1A Index', 'SHSZ300 Index']
list_32 = ['GBPUSD Curncy', 'UKX Index', 'SPX Index', 'SHSZ300 Index']
list_33 = ['IVV US Equity', 'IWM US Equity', 'QQQ US Equity', 'VXX US Equity',
		   'SHY US Equity', 'IEF US Equity', 'HYG US Equity', 'SHYG US Equity']
macro_list = ['SPX Index', 'H15T3M Index', 'H15T5Y Index', 'H15T10Y Index', 'CSI BBB Index',
		   'VIX Index', 'SKEW Index', 'US0003M Index', 'FDFD Index', 'DXY Index',
		   '.BAASP G Index']
list_35 = ['CPI YOY Index', 'IP Index', 'M1 Index', 'M2 Index', 'EHUPUS Index']
list_36 = ['VIX Index', 'VXX US Equity']
list_37 = ['AGG US Equity', 'ASHR US Equity', 'EMB US Equity', 'FLOT US Equity',
		   'GLD US Equity', 'HYG US Equity', 'INDA US Equity', 'QQQ US Equity',
		   'SCHF US Equity', 'SHV US Equity', 'VB US Equity', 'VTI US Equity',
		   'VTIP US Equity', 'VWO US Equity', 'VXX US Equity', 'XLE US Equity']
list_38 = ['FLOT US Equity', 'CSI BBB Index']
list_39 = ['FLOT US Equity', 'FLRN US Equity', 'FLTR US Equity']
list_40 = ['MXWD Index', 'SPUSBMIT Index']
china_list = ['601888 CH Equity', '600009 CH Equity', '600900 CH Equity',
			  '300015 CH Equity', '600519 CH Equity', 'CSIR0300 Index']
hk_list = ['1299 HK Equity', '2388 HK Equity', '2588 HK Equity', '3613 HK Equity',
		   '11 HK Equity', '778 HK Equity', '345 HK Equity', 'HSI 1 Index']
us_list = ['AAPL US Equity', 'TSM US Equity', 'BRK/B US Equity', 'DIS US Equity',
		   'COST US Equity', 'DPZ US Equity', 'MCO US Equity', 'SPXT Index']
other_list = ['AIA AU Equity', 'AIA NZ Equity', 'SATS SP Equity', 'NDUEACWZ Index']
list_41 = ['TWD Curncy']
list_42 = ['TWSE Index', 'CSIR0300 Index']
list_43 = ['IVV US Equity', 'AGG US Equity', 'SPXT Index', 'LBUSTRUU Index']
list_44 = ['GMWAX US Equity', 'EAAFX US Equity', 'GXSAX US Equity',
		   'MSGOX US Equity', 'MERGAAI LX Equity']
list_45 = ['SPXT Index', 'NDUEACWZ Index']
list_46 = ['00679B TT Equity', '00697B TT Equity']
list_47 = ['VWO US Equity', '0051 TT Equity', '0061 TT Equity']
list_48 = ['INDA US Equity', 'EWZ US Equity', 'EWA US Equity', 'M1WD Index']
list_49 = ['VXX US Equity', 'VIX Index', 'VTI US Equity']
list_50 = ['AV7177034@CBBT Corp']
list_51 = ['CSIR0300 Index', 'SHSZ300 Index', 'HSI Index', 'HSI 1 Index']
list_52 = ['LEGATRUU Index', 'LAPCTRJU Index', 'LG30TRUU Index',
		   'EMUSTRUU Index', 'I32561US Index',
		   'I08273US Index', 'I08271US Index', 'I08272US Index',
		   'I08275US Index', 'I29136US Index']
region_list = ['ASHR US Equity', 'EWA US Equity', 'EWC US Equity',
			   'EWJ US Equity', 'EWG US Equity', 'EWH US Equity',
			   'EWU US Equity', 'EWZ US Equity', 'EWL US Equity',
			   'EWY US Equity', 'INDA US Equity']
list_53 = ['VXX US Equity', 'VXXB US Equity']
list_54 = ['MXCN Index', 'HSI Index', 'SHSZ300 Index']
list_55 = ['3140 HK Equity', 'SPY US Equity', 'IVV US Equity']
list_56 = ['603559 CH Equity', '002723 CH Equity', '000576 CH Equity',
           '600462 CH Equity', '002098 CH Equity', '002054 CH Equity',
		   '002072 CH Equity']
list_57 = ['601111 CH Equity', '753 HK Equity', '601336 CH Equity', '1336 HK Equity']
list_58 = ['JPST LN Equity', 'JPST US Equity']
list_59 = ['BA US Equity', 'XLI US Equity']
list_60 = ['XLB US Equity', 'XLE US Equity', 'XLF US Equity', 'XLI US Equity',
		   'XLK US Equity', 'XLP US Equity', 'XLU US Equity', 'XLV US Equity',
		   'XLY US Equity', 'VNQ US Equity']
list_61 = ['MXWD Index', 'LEGATRUU Index', 'TEMGBLA LX Equity']
list_62 = ['JPST LN Equity', 'MINT US Equity']
list_63 = ['IVV US Equity', 'ACWX US Equity', 'SPXT US Equity', 'NDUEACWZ Index']
list_64 = ['EWJ US Equity', 'NKY Index']
list_65 = ['HTAHYIU HK Equity', 'IPRP LN Equity', 'FSEFIAU LX Equity',
		   'GLD US Equity', 'CUISCHI HK Equity', 'F750 GR Equity', 'E903 GR Equity',
		   'JPVNOPC HK Equity']
list_66 = ['USAG LN Equity', 'IDFF LN Equity', 'IMEU LN Equity', 'CSPX LN Equity',
		   '4GLD GR Equity']
list_67 = ['HKDCNY Curncy', 'USDHKD Curncy', 'USDCNY Curncy']
list_68 = ['LBUSTRUU Index', 'NDEUCFEX Index', 'MSDEE15N Index', 'SPTR500N Index']
list_69 = ['IHYA LN Equity', 'JPEA LN Equity', 'JPST LN Equity', 'TIP5 LN Equity']
list_70 = ['AAPL US Equity', 'MSFT US Equity', '700 HK Equity', 'INTC US Equity',
		   'CSCO US Equity', '005930 KS Equity', '2330 TT Equity', 'ORCL Equity',
		   'SAP GR Equity', 'ADBE US Equity', 'IBM US Equity', 'CRM US Equity',
		   'AVGO US Equity', 'NVDA US Equity', 'TCS IN Equity', 'ACN US Equity',
		   'TXN US Equity', 'ASML NA Equity', 'VMW US Equity', 'ADP US Equity',
		   'QCOM US Equity', 'INTU US Equity', '6758 JP Equity', 'SPGI US Equity',
		   '000660 KS Equity', '002415 CH Equity']
list_71 = ['601228 CH Equity', '601878 CH Equity', '603833 CH Equity']
list_72 = ['DXY Index', 'EURUSD Curncy', 'FDTR Index', 'USDJPY Curncy']
list_73 = ['EURTN Curncy']
list_74 = ['VNINDEX Index', 'SET Index', 'PCOMP Index', 'FBMKLCI Index', 'JCI Index']
list_75 = ['SPX Index']
list_76 = ['HSI Index']
list_77 = ['USDR1T Curncy', 'JYDR1T Curncy', 'EUDR1T Curncy',
		   'EURTN Curncy', 'JPYTN Curncy', 'EURUSD Curncy', 'USDJPY Curncy']
list_78 = ['VXX US Equity', 'VXXB US Equity']
list_79 = ['PLMAX US Equity', 'MERKX US Equity', 'ICPHX US Equity', 'SCAFX US Equity']
list_80 = ['HFRIMI Index']
list_81 = ['510300 CH Equity', '510330 CH Equity']
list_82 = ['1299 HK Equity', '700 HK Equity', '5 HK Equity']
list_83 = ['GLD US Equity', 'IVV US Equity', 'AGG US Equity']
list_84 = ['050002 CH Equity', '160124 CH Equity']
list_85 = ['753 HK Equity', 'HSI 1 Index']
list_86 = ['3053 HK Equity']
list_87 = ['CSIR0300 Index', 'SHSZ300 Index']
list_88 = ['GLD US Equity', 'QQQ US Equity', 'SPY US Equity']
list_89 = ['MXWD Index', 'SPBDUB3T Index']
sgm_4 = ['EMB US Equity', 'GOVT US Equity',
		 'HYG US Equity', 'IWS US Equity', 'LQD US Equity', 'MUB US Equity',
		 'SCHF US Equity', 'VNQ US Equity', 'VTI US Equity', 'VWO US Equity',
		 'XLE US Equity']
sgm_432 = ['EMB US Equity',
		 'HYG US Equity', 'IWS US Equity', 'LQD US Equity', 'MUB US Equity',
		 'SCHF US Equity', 'TIP US Equity', 'VNQ US Equity', 'VTI US Equity',
		 'VWO US Equity', 'XLE US Equity']
sgm_522 = ['AGG US Equity', 'ASHR US Equity', 'EMB US Equity', 'FLOT US Equity',
		   'GLD US Equity', 'HYG US Equity', 'INDA US Equity', 'SCHF US Equity',
		   'VBR US Equity', 'VNQ US Equity', 'VTI US Equity', 'VWO US Equity',
		   'XLE US Equity']
list_90 = ['SPX Index', 'MXWD Index', 'HSI Index']
list_91 = ['SPXT Index', 'M1WD Index', 'HSI 1 Index']
list_92 = ['2833 HK Equity', '7200 HK Equity', '7205 HK Equity', '7221 HK Equity']
list_93 = ['753 HK Equity', '1055 HK Equity', '670 HK Equity',
		   '601111 CH Equity', '600029 CH Equity', '600115 CH Equity',
		   'HSAHP Index']
shk_2 = ['3169 HK Equity', '3010 HK Equity', '3101 HK Equity',
		 '2833 HK Equity', '3140 HK Equity', '3147 HK Equity',
		 '3141 HK Equity', '2813 HK Equity', '3081 HK Equity']
shk_3 = ['3169 HK Equity', '3010 HK Equity', '3101 HK Equity',
		 '2833 HK Equity', '3140 HK Equity', '3147 HK Equity',
		 '3141 HK Equity', '2813 HK Equity', '3081 HK Equity',
		 '3011 HK Equity', '3077 HK Equity']
sgm_2 = ['VTI US Equity', 'SCHF US Equity', 'VWO US Equity',
		 'VB US Equity', 'AGG US Equity', 'EMB US Equity',
		 'HYG US Equity', 'FLOT US Equity', 'VTIP US Equity',
		 'SHV US Equity', 'GLD US Equity', 'XLE US Equity',
		 'QQQ US Equity', 'ASHR US Equity', 'INDA US Equity']
sd = ['SPBDUB3T Index', 'JPST US Equity', 'MINT US Equity', 'ERND LN Equity']
prop = ['3188 HK Equity', '1299 HK Equity', '2007 HK Equity',
		'27 HK Equity', '1336 HK Equity', '753 HK Equity',
		'670 HK Equity', '1928 HK Equity', '2382 HK Equity',
		'HSI Index']
us_benchmark = ['SPX Index']
tn = ['EURTN BGNL Curncy', 'JPYTN BGNL Curncy']
fx = ['EURUSD BGNL Curncy', 'USDJPY BGNL Curncy']
hk_oil_etf = ['3135 HK Equity', '3097 HK Equity', '3175 HK Equity']
shk_v3 = ['SBWMUD3U Index', 'BTFLTRUU Index', '3011 HK Equity', '3077 HK Equity']
list_94 = ['XAUUSDV1M Index']
list_95 = ['700 HK Equity', '5 HK Equity', '388 HK Equity']
list_96 = ['XAU Curncy', 'XAG Curncy']
list_97 = ['VTI US Equity', 'ITOT US Equity', 'AGG US Equity', 'SCHZ US Equity']
list_98 = ['SPX Index', 'USDCNH Curncy']
list_99 = ['MXWD Index', 'HSI Index', 'MXCN Index', 'SHSZ300 Index',
		   'LEGATRUU Index', 'LBUSTRUU Index']
list_100 = ['USDCNY Curncy', 'USDHKD Curncy', 'USDCNH Curncy']
list_101 = ['HI1 Index']
list_102 = ['AGG US Equity', 'MINT US Equity', 'JPST US Equity', 'SHV US Equity']
fx_pair = pd.read_csv('../data/fx_pair.csv', index_col=0)['FX_Pair'].values.tolist()
fx_pair = [f + ' BGNL Curncy' for f in fx_pair]
list_103 = ['2819', '3141', '2813', '3079', '3122', '3199', '3080', '3077', '3011']
list_103 = [s + ' HK Equity' for s in list_103]
list_104 = ['600000 CH Equity']
list_105 = ['600519', '600276', '000596', '603288', '000568', '000858',
			'000661', '600104']
list_105 = [s + ' CH Equity' for s in list_105] + ['SHSZ300 Index']
list_106 = ['BOCAWGA HK Equity']
list_107 = ['CSIN0301 Index', 'MXCN1A Index']
list_108 = ['EURUSD T150 Curncy', 'EURUSD BGNL Curncy']
list_109 = ['PLMAX US Equity', 'MERKX US Equity', 'ICPHX US Equity']

current_list = list_3
data = download_his(
	current_list,
	['PX_LAST'],
	# ['PX_LAST', 'VOLUME'],
	# ['FUND_NET_ASSET_VAL'],
	# ['PX_BID', 'PX_ASK', 'PX_LAST'],
	# ['FREE_FLOAT_MARKET_CAP'],
	# ['PX_LAST', 'CEF_PCT_PREM', 'FUND_NET_ASSET_VAL'],
	# ['PR291', 'PR292'],
	# ['YAS_BOND_YLD', 'YAS_MOD_DUR'],
	start,
	end,
	period='DAILY',
	currency='HKD',
	dpdf=True
).ffill()
data.columns = data.columns.get_level_values(0)
data = data[current_list]
print data
data.to_csv('../result/index.csv')

dr = data / data.iloc[0, :]
dr.plot()
plt.show()
