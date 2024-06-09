from unittest import TestCase, mock

import pytest

from quantcycle.app.data_manager.data_loader.data_center.proxy_utils import create_proxy

DM_path = 'quantcycle.app.data_manager.data_loader.data_center.' +\
    'proxy_utils.DataMaster'
LHDF_path = 'quantcycle.app.data_manager.data_loader.data_center.' +\
    'proxy_utils.LocalDC'


class CreateProxyTestCase(TestCase):
    @mock.patch(DM_path)
    @mock.patch(LHDF_path)
    def test_correct_data_center(self, m_lhdf, m_dm):
        create_proxy({"DataCenter": "DataMaster"})
        m_dm.assert_called()
        create_proxy({"DataCenter": "LocalHDF5"})
        m_lhdf.assert_called()

    @mock.patch(DM_path)
    @mock.patch(LHDF_path)
    def test_incorrect_data_center(self, m_lhdf, m_dm):
        self.assertRaises(ValueError,
                          create_proxy,
                          {"DataCenter": "Other"})
