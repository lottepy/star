from .enum_type import EnumBase


class AlgoType(EnumBase):
	SMART_GLOBAL = 1
	SMART_HK = 2
	SMART_XRAY_CN = 3
	SMART_GLOBAL_TEST = 4
	MFM_CN = 5
	SMART_XRAY_HK = 6

	ALGO_SMART_GLOBAL_V3 = 100
	ALGO_SMART_GLOBAL_V4 = 101
	ALGO_SMART_GLOBAL_V4_3_2 = 102
	ALGO_SMART_GLOBAL_V5_0 = 104
	ALGO_SG_V5_0_DEV = 103
	ALGO_SG_V5_0_VIX = 105
	ALGO_SMART_GLOBAL_V4_3_2_TEST = 1000
	ALGO_SMART_HK_V1 = 110
	ALGO_SMART_HK_V2 = 111
	ALGO_SMART_XRAY_CN_SP = 120
	ALGO_SMART_XRAY_CN_HR = 121
	ALGO_SMART_XRAY_CN_BOCHK = 122
	ALGO_SMART_XRAY_CN_SG = 123
	ALGO_MFM_CN_V1 = 130
	ALGO_MFM_CN_V2 = 131
	ALGO_MFM_CN_V3 = 132

ALGOTYPE_REGION_MAP = {
	1: 'US',
	2: 'HK',
	3: 'CN',
	4: 'US',
	5: 'CN',
	100: 'US',
	101: 'US',
	102: 'US',
	103: 'US',
	104:'US',
	110: 'HK',
	111: 'HK',
	120: 'CN',
	121: 'CN',
	122: 'CN',
	123: 'CN',
	130: 'CN',
	131: 'CN',
	132: 'CN',
	1000: 'US',
	1001: 'US',
	1002: 'US',
}

ALGOTYPE_RISK_RATIO_MAP = {
	1: list(range(0,110,10)),
	4: list(range(0,110,10)),
	100: list(range(0,110,10)),
	101: list(range(0,110,10)),
	102: list(range(0,110,10)),
	103: list(range(0,110,10)),
	104: list(range(0,110,10)),
	110: list(range(10,110,10)),
	111: list(range(10,110,10)),
	120: list(range(0,110,10)),
	121: list(range(0,110,10)),
	122: list(range(0,110,10)),
	123: list(range(0,110,10)),
	130: [100],
	131: [100],
	132: [100],
	1000: [20, 40, 60, 80],
	1001: [20, 40, 60, 80],
	1002: [20, 40, 60, 80]
}

ALGOTYPE_BTREGION_MAP = {
	1: ["NON", "AUS", "DEU", "HKG", "JPN", "GBR", "CHN"],
	4: ["NON", "AUS", "DEU", "HKG", "JPN", "GBR", "CHN"],
	100: ["NON", "AUS", "DEU", "HKG", "JPN", "GBR", "CHN"],
	101: ["NON", "AUS", "DEU", "HKG", "JPN", "GBR", "CHN"],
	102: ["NON", "AUS", "DEU", "HKG", "JPN", "GBR", "CHN"],
	104: ["NON"],
	103: ["NON"],
	110: ["NON"],
	111: ["NON"],
	120: ["NON"],
	121: ["NON"],
	122: ["NON"],
	123: ["NON"],
	130: ["NON"],
	131: ["NON"],
	132: ["NON"],
	1000: ["NON", "AUS", "DEU", "HKG", "JPN", "GBR", "CHN"],
	1001: ["NON", "CHN"],
	1002: ["NON"]
}

BACKTESTING_REGION_MAP = {
	'NON': "",
	'AUS': "Australia",
	'CHN': "China",
	'DEU': "Germany",
	'GBR': "UK",
	'HKG': 'HK',
	'JPN': "Japan"
}

ALGOTYPE_BTSECTOR_MAP = {
	1: ["NON", "TEC", "CNS", "UTL", "HEA"],
	4: ["NON", "TEC", "CNS", "UTL", "HEA"],
	100: ["NON", "TEC", "CNS", "UTL", "HEA"],
	101: ["NON", "TEC", "CNS", "UTL", "HEA"],
	102: ["NON", "TEC", "CNS", "UTL", "HEA"],
	104:["NON"],
	103:["NON"],
	110: ["NON"],
	111: ["NON"],
	120: ["NON"],
	121: ["NON"],
	122: ["NON"],
	123: ["NON"],
	130: ["NON"],
	131: ["NON"],
	132: ["NON"],
	1000: ["NON", "TEC", "CNS", "UTL", "HEA"],
	1001: ["NON"],
	1002: ["NON"]
}

BACKTESTING_SECTOR_MAP = {
	# "NON", "TEC", "CNS", "UTL", "HEA"
	# "Consumer non-cycl", "Health care", "", "Technology",  "Utilities"
	"NON": "",
	"TEC": "Technology",
	"CNS": "Consumer non-cycl",
	"UTL": "Utilities",
	"HEA": "Health care"
}

# Benchmark Map
REGION_BM_MAP = {
	'US': 'SP500',
	'HK': 'HSI',
	'CN': 'SHSZ300',
	'GB': 'SP500',
}
# timezone map
REGION_TZ_MAP = {
	'US': 'US/Eastern',
	'HK': 'Asia/Hong_Kong',
	'CN': 'Asia/Shanghai',
	'GB': 'Europe/London'
}
# calendar
REGION_CALENDAR_MAP = {
	'US': 'AQMUS',
	'HK': 'AQMHK',
	'CN': 'AQMCN',
	'GB': 'AQMUK',
}

BUNDLE_MAP = {
	103: 'aqm102_usetf',
	110: 'aqm102_hketf',
	121: 'aqm102_cnfund',
}
# for make bundles
REGION_CURRENCY_MAP = {
	'CN': 'CNY',
	'US': 'USD',
	'HK': 'HKD',
	'SG': 'SGD',
	'WW': '',
	'GB': '',
}

ASSET_INSTRUMENT_MAP = {
	'CN_XRAY_MF':'MUTUAL FUND',
	'CN_XRAY_INDEX':'INDEX',
	'US_ETF':'EQUITY',
	'HK_ETF':'EQUITY',
	'HK_STOCK':'EQUITY',
	'HK_FUTURE':'FUTURE'
}

ASSET_PATTERN_MAP ={
	'CN_XRAY_MF': r'^CH([\d]{6}$)|^([\d]{6}).OF',
	'US_ETF':'',
	'HK_ETF':r'HK([\d]{4,5})',
	'CN_STOCK':r'^([\d]{6}).SZ|^([\d]{6}).SH',
	'HK_STOCK':r'^([\d]{4}).HK',
	# 'HK_FUTURE':r'^([A-Z]{2,3})[A-Z][\d]',
	'HK_FUTURE':r'(^[A-Z]{2,4}[\d])',
	'INDEX':r'^IDX_([\w]+)',
	'GB_STOCK':r'^([\w]+?).LN',
	'US_STOCK':r'^([\w]+?).US',
}

ASSET_TYPE_MAP = {'EQUITY': '10',
			  'MUTUAL FUND': '20',
			  'INDEX': '30',
			  'FX PAIR': '40',
			  'CRYPTO PAIR': '50',
			  'FUTURE': '60'
			  }

INDEX_IUID_MAP = {
	'IDX_SPX': "US_30_SP500",
	'IDX_IXIC': "US_30_SP500", # NASDAQ Composite Index
	'IDX_CRSPTMT': "US_30_CRSPTMT", # CRSP US Total Market TR Index
	'IDX_MXBRIC': "WW_30_MXBRIC", # MSCI BRIC Index
	'IDX_HSI': "HK_30_HSI"
}

ALGOCLASS_BUNDLE_MAP = {
	'SMART_GLOBAL': 'aqm130_usetf',
	'SMART_HK': 'aqm130_hketf',
	'SMART_XRAY_CN': 'aqm130_cnfund',
	'MFM_CN': 'aqm130_cnstock',
	'MFM_HK': 'aqm130_hkstock',
	'SMART_DEPOSIT': 'aqm130_uketf'
}

ALGOCLASS_REGEION_MAP = {
	'SMART_GLOBAL': 'US',
	'SMART_HK': 'HK',
	'SMART_XRAY_CN': 'CN',
	'MFM_CN': 'CN',
	'MFM_HK': 'HK',
	'SMART_DEPOSIT': 'GB'
}

ALGOCLASS_BTSTART_MAP = {
	'SMART_GLOBAL': '2003-01-01',
	'SMART_HK': '2013-01-01',
	'SMART_XRAY_CN': '2014-09-01',
	'MFM_CN': '2015-12-31',
	'MFM_HK': '2014-01-01',
	'SMART_DEPOSIT': '2019-01-01'
}

ALGOCLASS_BTEND = {
	'SMART_GLOBAL': '2003-01-01',
	'SMART_HK': '2013-01-01',
	'SMART_XRAY_CN': '2014-09-01',
	'MFM_CN': '2014-01-01',
	'MFM_HK': '2014-01-01'
}

class AlgoClassBase:
	def __init__(self, name):
		self.name = name
		self.bundle_name = ALGOCLASS_BUNDLE_MAP[name]
		self.region = ALGOCLASS_REGEION_MAP[name]
		self.calendar = REGION_CALENDAR_MAP[self.region]
		self.timezone = REGION_TZ_MAP[self.region]
		self.currency = REGION_CURRENCY_MAP[self.region]
		self.benchmark = REGION_BM_MAP[self.region]
		self.btstart = ALGOCLASS_BTSTART_MAP[name]

class AlgoTypeBase:
	def __init__(self, algotype, algoclass_name, algoversion = 1):
		self.algotype = algotype
		self.algoclass = AlgoClassBase(algoclass_name)
		self.algoversion = algoversion

# OBSELTE DB TYPE
ALGO_SMART_GLOBAL_V4_3_2 = AlgoTypeBase(102,'SMART_GLOBAL')
ALGO_SMART_GLOBAL_V5_0 = AlgoTypeBase(104,'SMART_GLOBAL')
ALGO_SG_V5_0_FAST = AlgoTypeBase(103,'SMART_GLOBAL')
# ALGO_SMART_HK_V1 = AlgoTypeBase(110,'SMART_HK')
# ALGO_SMART_HK_V2 = AlgoTypeBase(111,'SMART_HK')
# ALGO_SMART_HK_V2_BT = AlgoTypeBase(113,'SMART_HK')
# ALGO_SMART_XRAY_CN_HR = AlgoTypeBase(121,'SMART_XRAY_CN')
# ALGO_SMART_XRAY_CN_ZJU = AlgoTypeBase(124,'SMART_XRAY_CN')
ALGO_MFM_CN_OFFSHORT_LONG_v1 = AlgoTypeBase(130,'MFM_CN')

# db version v2
ALGO_SMART_GLOBAL_V4_32 = AlgoTypeBase(1,'SMART_GLOBAL')
ALGO_SMART_GLOBAL_V5_01 = AlgoTypeBase(2,'SMART_GLOBAL')
ALGO_SMART_GLOBAL_V5_VXX = AlgoTypeBase(3,'SMART_GLOBAL')
ALGO_SMART_GLOBAL_V5_2 = AlgoTypeBase(22,'SMART_GLOBAL')
ALGO_SMART_GLOBAL_V5_2_Dummy = AlgoTypeBase(10,'SMART_GLOBAL')
ALGO_SMART_HK_V1_BT = AlgoTypeBase(5,'SMART_HK')
ALGO_SMART_HK_V2_BT = AlgoTypeBase(6,'SMART_HK')
ALGO_SMART_XRAY_CN_HR = AlgoTypeBase(8,'SMART_XRAY_CN')
ALGO_SMART_XRAY_CN_HR_PROD = AlgoTypeBase(25,'SMART_XRAY_CN')
ALGO_SMART_XRAY_CN_MOJIE = AlgoTypeBase(9,'SMART_XRAY_CN')
ALGO_MFM_CN_OFFSHORT_LONG = AlgoTypeBase(16,'MFM_CN')
ALGO_MFM_CN_OFFSHORT_ALLOCATION = AlgoTypeBase(14, 'MFM_CN')
ALGO_MFM_CN_ONSHORT_LONG = AlgoTypeBase(13,'MFM_CN')
ALGO_MFM_CN_ONSHORT_ALLOCATION = AlgoTypeBase(15, 'MFM_CN')
ALGO_MFM_HK_LONG  = AlgoTypeBase(23, 'MFM_HK')
ALGO_MFM_HK_PORTFOLIO = AlgoTypeBase(24, 'MFM_HK')
ALGO_SMART_GLOBAL_SEC_ROTATION = AlgoTypeBase(25,'SMART_GLOBAL')
ALGO_SMART_GLOBAL_REG_ROTATION = AlgoTypeBase(27,'SMART_GLOBAL')
ALGO_SMART_DEPOSIT_V1 = AlgoTypeBase(221,'SMART_DEPOSIT')
ALGO_QZHITOU_V2_test = AlgoTypeBase(224,'SMART_XRAY_CN')

# custom default start date
ALGO_SMART_HK_V2_BT.algoclass.btstart = '2017-9-1'
ALGO_SMART_GLOBAL_V5_VXX.algoclass.btstart = '2015-1-1'
ALGO_SMART_XRAY_CN_MOJIE.algoclass.btstart = '2016-12-31'
ALGO_SMART_GLOBAL_V5_2.algoclass.btstart = '2011-12-31'
ALGO_SMART_GLOBAL_SEC_ROTATION.algoclass.btstart = '2002-12-31'
ALGO_SMART_GLOBAL_REG_ROTATION.algoclass.btstart = '2002-12-31'


# for algo control
class AlgoControlBase:
	def __init__(self, riskratios, btregions, btsectors):
		self.riskratios = riskratios
		self.btregions = btregions
		self.btsectors = btsectors

ALGO_CONTROL_S1 = AlgoControlBase(range(0,110,10),
                           ["NON", "AUS", "DEU", "HKG", "JPN", "GBR", "CHN"],
                           ["NON", "TEC", "CNS", "UTL", "HEA"])

ALGO_CONTROL_S2 = AlgoControlBase(range(20,90,10),
                           ["NON"],
                           ["NON"])

ALGO_CONTROL_S3 = AlgoControlBase(range(40,90,10),
                           ["NON"],
                           ["NON"])
ALGO_CONTROL_S4 = AlgoControlBase([20, 40, 60, 80], ["NON"], ['NON'])
ALGO_CONTROL_S5 = AlgoControlBase([55], ["NON"], ['NON'])
ALGO_CONTROL_S6 = AlgoControlBase(range(0,110,10),
                           ["NON"],
                           ["NON"])
ALGO_CONTROL_S7 = AlgoControlBase([80], ['NON'], ['NON'])
ALGO_CONTROL_S8 = AlgoControlBase([0], ['NON'], ['NON'])

ALGO_CONTROL_S9 = AlgoControlBase([20, 40, 60, 70, 80],
                           ["NON", "AUS", "DEU", "HKG", "JPN", "GBR", "CHN"],
                           ["NON", "TEC", "CNS", "UTL", "HEA"])

# for DB Sync
class DBSyncBase:
	def __init__(self,bt_updatemode = 0, weight_updatemode = 0,
	             bt_updatefreq = 'D', bt_updatecal = 'Active Days'):
		self.bt = bt_updatemode
		self.btfreq = bt_updatefreq
		self.btcalendar = bt_updatecal
		self.weight = weight_updatemode

DB_SYNC_S0 = DBSyncBase(0,0) # NO UPDATE
DB_SYNC_S1 = DBSyncBase(2,2) # FULL UPDATE
DB_SYNC_S2 = DBSyncBase(1,0) # INCREMENTAL UPDATE
