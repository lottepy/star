from os.path import dirname, abspath, join
import sys
algo_path = dirname(__file__)
root_path = dirname(dirname(abspath(__file__)))
util_path = join(root_path, 'utilities')
ext_path = join(util_path,'aqm_extensions')
data_path = join(root_path, 'data')
result_path = join(root_path, 'result')
config_path = join(root_path, 'config')
lib_path = join(root_path,'lib')

result_strategy_kit_path = join(result_path, "strategy_kit")
result_chartsite_path = join(result_path, "chartsite")


if not util_path in sys.path:
    # sys.path.append(util_path)
    sys.path.insert(1, root_path)
    # sys.path.append(smartglobal_path)
    # print sys.path
