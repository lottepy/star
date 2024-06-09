from unittest import TestCase, mock

import numpy as np
import pytest

from quantcycle.app.data_manager.utils.data_collection import DataCollection, TwoDimData


class DataCollectionTestCase(TestCase):
    def test_init(self):
        data_collection = DataCollection()
        data_collection["some symbol"].data = np.array([[1, 2, 3], [2, 3, 4]])
        assert isinstance(data_collection["some symbol"], TwoDimData)

# class NDimDataTestCase(TestCase):
#     def test_2d_verify(self):
#         d = TwoDimData(
#             symbol='AUDCAD',
#             fields='close',
#             data=np.array([1,2,3]),
#             timestamp = np.array([1,2,3]))
#         self.assertRaises(ValueError,
#                           d.verify)
