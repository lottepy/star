from os.path import dirname, abspath, join
import sys
algo_path = dirname(__file__)
root_path = dirname(dirname(abspath(__file__)))
util_path = join(root_path, 'utils')
data_path = join(root_path, 'data')
result_path = join(root_path, 'results')
config_path = join(root_path, 'config')
strategy_path = join(root_path, 'strategy')

sgu_v2_path = join(strategy_path, 'smartglobal_ultimax', 'v2')

smart_rotation_path = join(data_path, 'smart_rotation')
smart_rotation_ref_path = join(smart_rotation_path, 'reference_data')

if not util_path in sys.path:
    # sys.path.append(util_path)
    sys.path.insert(1, root_path)
    # sys.path.append(smartglobal_path)
    # print sys.path
