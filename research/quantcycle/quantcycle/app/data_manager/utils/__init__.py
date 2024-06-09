from .parse_configuration import parse_config
from pathlib import Path

DATA_MANAGER_PATH = Path(__file__).parent.parent.resolve()
# DATA_MANAGER_LOG_ROOT = DATA_MANAGER_PATH.joinpath('logs')
DATA_MANAGER_LOG_ROOT = Path('.').resolve()
DATA_MANAGER_LOG_PARENT = ''