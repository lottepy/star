import copy
from unittest import TestCase, mock

import pytest

from quantcycle.app.data_manager.utils.handle_data_bundle import (_find_proxy, classify_data_bundle,
                                  download_data_bundle, establish_queue)
from quantcycle.app.data_manager.utils.samples import (sample_data_bundle_INFO, sample_data_bundle_INT,
                       sample_data_bundle_MAIN)
from quantcycle.app.data_manager.utils.update_nested_dict import update_dict


class ClassifyDataBundleTestCase(TestCase):
    def test_correctly_classify(self):
        output = classify_data_bundle([
            sample_data_bundle_INFO,
            sample_data_bundle_MAIN,
            sample_data_bundle_INT])
        assert len(output[0]) == 2
        assert len(output[1]) == 0
        assert len(output[2]) == 1

    def test_data_bundle_with_wrong_type(self):
        self.assertRaises(ValueError,
                          classify_data_bundle,
                          [update_dict(sample_data_bundle_INFO, {"Type": "Other"})])

    def test_no_type(self):
        self.assertRaises(AssertionError,
                          classify_data_bundle,
                          [{"Label": "EmptyBundle"}])


class FindProxyTestCase(TestCase):
    @mock.patch('quantcycle.app.data_manager.utils.handle_data_bundle.create_proxy')
    def test_find_created_proxy(self, m_cp):
        proxies = {"DataMaster": 1, "LocalHDF5": 2}
        proxy = _find_proxy(sample_data_bundle_INFO, proxies)
        m_cp.assert_not_called()
        assert proxy == 1

    @mock.patch('quantcycle.app.data_manager.utils.handle_data_bundle.create_proxy')
    def test_find_no_proxy(self, m_cp):
        m_cp.return_value = 1
        proxies = {"DataMaster": 1}
        data_bundle = {"DataCenter": "LocalHDF5"}
        proxy = _find_proxy(data_bundle, proxies)
        m_cp.assert_called_once()
        assert proxy == 1
        assert "LocalHDF5" in proxies.keys()


class Mock_Proxy():
    def __init__(self):
        self.on_data_bundle = mock.Mock()


class DownloadDataBundleTestCase(TestCase):

    def test_pass_to_proxy(self):
        proxies = {}
        proxy = Mock_Proxy()
        proxies["DataMaster"] = proxy
        data_bundle = {"DataCenter": "DataMaster"}
        download_data_bundle(data_bundle, proxies)
        proxy.on_data_bundle.assert_called_once()

    @mock.patch('quantcycle.app.data_manager.utils.handle_data_bundle.create_proxy')
    def test_create_and_pass(self, m_cp):
        m_cp.return_value = Mock_Proxy()
        proxies = {}
        data_bundle = {"DataCenter": "DataMaster"}
        download_data_bundle(data_bundle, proxies)
        proxies["DataMaster"].on_data_bundle.assert_called_once()
