from os.path import dirname, abspath, join
import sys
algo_path = dirname(__file__)
root_path = dirname(dirname(abspath(__file__)))
util_path = join(root_path, 'utils')
data_path = join(root_path, 'data')
result_path = join(root_path, 'result')
config_path = join(root_path, 'config')
lib_path = join(root_path,'lib')

bundle_path = join(data_path, 'bundles')
historical_path = join(data_path, 'historical_data')
model_path = join(data_path, 'models')

# Historical Datasets
temp_data_path = join(historical_path, 'temp_data')
beta_path = join(historical_path, 'beta_factor')
categorization_path = join(historical_path, 'categorization')

ref_path = join(config_path, 'cn_fund_full.csv')

if not util_path in sys.path:
    # sys.path.append(util_path)
    sys.path.insert(1, root_path)
    # sys.path.append(smartglobal_path)
    # print sys.path
