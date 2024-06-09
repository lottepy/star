from quantcycle.app.data_manager.utils import DATA_MANAGER_LOG_ROOT, DATA_MANAGER_LOG_PARENT
from quantcycle.utils.get_logger import get_logger


class ProxyBase():
    dm_info_summary = {}
    
    def __init__(self):
        logger_name = DATA_MANAGER_LOG_PARENT+f'.DataManager.DataLoader.{self.__class__.__name__}' if DATA_MANAGER_LOG_PARENT else f'DataManager.DataLoader.{self.__class__.__name__}'
        self.logger = get_logger(
            logger_name,
            str(DATA_MANAGER_LOG_ROOT.joinpath(
                f'{self.__class__.__name__}.log'))
        )

    def on_data_bundle(self):
        raise NotImplementedError

    def collect_data(self):
        raise NotImplementedError
