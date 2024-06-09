import sys
from os.path import dirname, abspath
root_path = dirname(dirname(abspath(__file__)))
if root_path not in sys.path:
	sys.path.insert(1, root_path)
print(root_path)