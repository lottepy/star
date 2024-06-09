import pytest

from quantcycle.app.data_manager.data_distributor import (DataDistributorMain,
                                                          DataDistributorSub)
from quantcycle.app.data_manager.data_loader import DataLoader
from quantcycle.app.data_manager.data_processor import DataProcessor
from quantcycle.app.data_manager.utils import parse_config


def test_import_each_part():
    # check out numpy repo tests/public_api for more information
    # better use a white list
    assert DataDistributorMain.__name__ == 'DataDistributorMain'
    assert DataDistributorMain.__module__ == \
        'quantcycle.app.data_manager.data_distributor.data_distributor_main'

    assert DataDistributorSub.__name__ == 'DataDistributorSub'
    assert DataDistributorSub.__module__ == \
        'quantcycle.app.data_manager.data_distributor.data_distributor_sub'

    assert DataLoader.__name__ == 'DataLoader'
    assert DataLoader.__module__ == \
        'quantcycle.app.data_manager.data_loader.data_loader'

    assert DataProcessor.__name__ == 'DataProcessor'
    assert DataProcessor.__module__ == \
        'quantcycle.app.data_manager.data_processor.data_processor'

    assert callable(parse_config)
