from ..proxy_base import ProxyBase


class LocalDC(ProxyBase):
    def __init__(self, name='Local-Main', config_dict=None):
        super().__init__()
        if config_dict is None:
            config_dict = {}

    def dump_data(self):
        _dump_data()

    def load_data(self):
        _load_data()

    def fetch_data_list(self):
        _fetch_data_list()


def _dump_data():
    raise NotImplementedError


def _load_data():
    raise NotImplementedError


def _fetch_data_list(dc_name: str):
    raise NotImplementedError
