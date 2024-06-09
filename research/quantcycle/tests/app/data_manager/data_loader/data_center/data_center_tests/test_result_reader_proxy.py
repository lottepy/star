import pandas as pd
import pytest
from unittest import TestCase, mock

from quantcycle.app.data_manager.utils.handle_data_bundle import \
    download_data_bundle
from quantcycle.app.data_manager.utils.samples import sample_data_bundle_RR

from quantcycle.app.data_manager.data_loader.data_center.result_reader.proxy import ResultReaderProxy

# TODO
 
# class ResultReaderProxyTestCase(TestCase):
#     @mock.patch('quantcycle.app.data_manager.data_loader.data_center.result_reader.proxy.ResultReader')
#     def test_get_data_from_hdf5_with_ResultReader(self, mock_rr):
#         class fakeResultReader:
#             def __init__(self, result_file: str):
#                 assert True # assert os.path.exists(result_file)
#                 self.result_file = result_file

#                 self.all_historical_names = ['position', 'pnl', 'cost']
#                 self.all_attribute_names = ['params', 'ref_aum', 'symbols']
                
#             def get_strategy(self, a, b, c):
#                 return {a[0]: pd.DataFrame()}
            
#             def get_attr(self,a,b):
#                 return pd.DataFrame()
            

#         mock_rr.return_value = fakeResultReader('testpath.hdf5')
        
#         proxies = {}
#         data = download_data_bundle(sample_data_bundle_RR, proxies)
#         assert (set(data.keys()) == set({'Strat1/symbols', 'Strat1/params', 'Strat2/pnl',
#                                          'Strat2/position', 'Strat2/symbols', 'Strat1/position', 'Strat1/pnl', 'Strat2/params'}))