from unittest import TestCase, mock

import pytest

from quantcycle.app.data_manager.data_loader.data_loader import DataLoader

mock_parse_config_path = \
    'quantcycle.app.data_manager.data_loader.' +\
    'data_loader.parse_config'
mock_route_data_request_path = \
    'quantcycle.app.data_manager.data_loader.' +\
    'data_loader.route_data_request'
mock_download_data_bundle_path = \
    'quantcycle.app.data_manager.data_loader.' +\
    'data_loader.download_data_bundle'
mock_classify_data_bundle_path = \
    'quantcycle.app.data_manager.data_loader.' +\
    'data_loader.classify_data_bundle'
mock_update_data_path = \
    'quantcycle.app.data_manager.data_loader.' +\
    'data_loader.update_data'
mock_data_loader_handle_list_path = \
    'quantcycle.app.data_manager.data_loader.' +\
    'data_loader.DataLoader._handle_bundle_list'
mock_establish_queue_path =\
    'quantcycle.app.data_manager.data_loader.' +\
    'data_loader.establish_queue'


class DataLoaderCallTestCase(TestCase):

    @mock.patch(mock_establish_queue_path)
    @mock.patch(mock_update_data_path)
    @mock.patch(mock_download_data_bundle_path)
    @mock.patch(mock_classify_data_bundle_path)
    def test_handle_bundle_list(self, m_classify, m_down,
                                m_update, m_est):
        class m_dp():
            def __init__(self):
                self.connect_queue = mock.Mock()
                self.register = mock.Mock()
        class m_dm():
            def __init__(self):
                self.config_dict = {}
        m_classify.return_value = ([{'1':1}], [{'2':2}], [{'3':3}])
        m_est.return_value = 'A Queue'
        dl = DataLoader()
        dl.data_processor = m_dp()
        dl.data_manager = m_dm()
        dl._handle_bundle_list([{'1'}, {'2'}, {'3'}])
        m_classify.assert_called()
        m_down.assert_called_once()
        m_update.assert_called_once()
        m_est.assert_called_once()
        dl.data_processor.register.assert_called_once()
