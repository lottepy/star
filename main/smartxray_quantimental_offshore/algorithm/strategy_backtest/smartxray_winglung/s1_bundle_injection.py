from os import listdir
from os.path import join
import pandas as pd
from algorithm.addpath import data_path
from lib.commonalgo.bundles.base import BaseBundle


def bundle_injection():
	bundles_path = join(data_path, 'bundles')
	bundles_daily_path = join(bundles_path, 'daily')
	fundsfile_list = listdir(bundles_daily_path)
	try:
		fundsfile_list.remove('.DS_Store')
	except ValueError:
		print('No .DS_Store file')

	summary = pd.DataFrame()
	summary['Name'] = [c[:-4] for c in fundsfile_list]
	summary.to_csv(join(bundles_path, 'Summary.csv'), index=False)
	funds_bundle = BaseBundle('aqm130_mf_HKUS', 'AQMHK', pd.Timestamp('2000-01-04', tz='UTC'))
	funds_bundle.update_bundle(fromcsv=True, summaryfolder=bundles_path, prefillcsvfolder=bundles_daily_path)


if __name__ == "__main__":
	bundle_injection()
