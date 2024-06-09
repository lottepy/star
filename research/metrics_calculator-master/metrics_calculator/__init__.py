from metrics_calculator.main import metrics_calculator
from metrics_calculator.core.data_reader import read_data
from metrics_calculator.core.core import _calculate_metrics as calculate_metrics
from metrics_calculator.utils.update_benchmark import update_benchmark
from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
