from ..bundles.base import BaseBundle
from ..setting.constants import *
import pandas as pd
import argparse

ap = argparse.ArgumentParser()
ap.add_argument("-a", "--algoclass", required=True,
	help="algo class")
ap.add_argument("-s", "--summaryfolder", required=True,
	help="summary folder for symbols")
args = vars(ap.parse_args())

print (ALGOCLASS_BUNDLE_MAP[args["algoclass"]])
print (REGION_CALENDAR_MAP[ALGOCLASS_REGEION_MAP[args["algoclass"]]])

bundle = BaseBundle(ALGOCLASS_BUNDLE_MAP[args["algoclass"]],
                    REGION_CALENDAR_MAP[ALGOCLASS_REGEION_MAP[args["algoclass"]]],
                    pd.Timestamp('2000-01-04', tz='UTC'))
bundle.update_bundle(summaryfolder=args["summaryfolder"])

#python -m commonalgo.bundles.bundle_ingest_v2_cmd.py -a SMART_HK -s http://aqm-algo.oss-cn-hongkong.aliyuncs.com/DataBundles/HK_ETF_20170907/



# us = BaseBundle('aqm102_usetf', 'AQMUS',pd.Timestamp('2000-01-04', tz='UTC'))
# us.update_bundle(summaryfolder="http://aqm-algo.oss-cn-hongkong.aliyuncs.com/US_ETF_2018/")
#
# cnfund = BaseBundle('aqm102_cnfund', 'AQMCN',pd.Timestamp('2000-01-04', tz='UTC'))
# cnfund.update_bundle(summaryfolder="http://aqm-algo.oss-cn-hongkong.aliyuncs.com/DataBundles/China_Mutual_Fund_20180315")


# hketf = BaseBundle('aqm102_hketf', 'AQMCN',pd.Timestamp('2000-01-04', tz='UTC'))
# hketf.update_bundle(True, summaryfolder="ftp://aqumon:magnum666@192.168.9.120/share/algodata/DataBundles/HK_ETF_20180608/")

# hketf = BaseBundle('aqm102_hketf', 'AQMHK',pd.Timestamp('2000-01-04', tz='UTC'))
# hketf.update_bundle(False, summaryfolder="http://aqm-algo.oss-cn-hongkong.aliyuncs.com/DataBundles/HK_ETF_20170907/")