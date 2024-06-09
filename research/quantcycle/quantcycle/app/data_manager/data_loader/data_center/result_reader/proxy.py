import pandas as pd
from quantcycle.app.result_exporter.result_reader import ResultReader

from ..proxy_base import ProxyBase


class ResultReaderProxy(ProxyBase):
    def __init__(self, d: dict):
        super().__init__()
        self.data_bundle = d
        self.hdf5_path = d['DataCenterArgs']['DataPath']
        self.rr = ResultReader(self.hdf5_path)

    def on_data_bundle(self, data_bundle):
        '''
            download one data bundle
        '''
        self.logger.info(f'ResultReaderProxy On data bundle: {data_bundle}')
        results = None
        symbols = data_bundle.get('Symbol', [])
        fields = data_bundle.get('Fields', [])
        if type(symbols) == str:
            id_list = [symbols]
        if type(fields) == str:
            fields = [fields]
        
        self.name = data_bundle['Label'].split('-')[0]
        #id_list = [int(sym.split('_')[-1]) for sym in symbols]
        id_list = symbols
        if str(id_list) == 'ALL':
            id_list = list(self.rr.get_all_sids())
        data = {}
        
        pnl_type = data_bundle['DataCenterArgs'].get('pnl_type', None)
        start_end_time = tuple(
            [data_bundle.get("StartTS", 1577836800), data_bundle.get("EndTS", 1580428800)])

        # dependency on ResultReader
        hist_field = [
            field for field in fields if field in self.rr.all_historical_names]
        attr_field = [
            field for field in fields if field in self.rr.all_attribute_names]

        results = self._get_historical(id_list, hist_field, start_end_time, pnl_type, self)
        results = _parse_result(results)
        data.update(results)

        results = self._get_attributes(id_list, attr_field, start_end_time, self)
        results = _parse_result(results)
        data.update(results)

        try:
            results = self._get_id_map(self)
            data.update(results)
        except:
            pass

        return data


    def _get_historical(self, id_list: list, fields, start_end_time, pnl_type, proxies):
        data = {}
        for field in fields:
            dataarr, timearr, id_list, id_sym_map  = proxies.rr.get_strategy_3d(id_list, field, start_end_time, pnl_type)
            # assert timearr.size != 0, f"RR get_strategy_3d return empty np arr, probably start end date not correct. Start, end date: {start_end_time}, field: {field}"
            data[f'{self.name}/{field}'] = (dataarr, timearr, id_list, id_sym_map)
        return data


    def _get_attributes(self, id_list: list, fields, start_end_time, proxies):
        data = {}
        for field in fields:
            results = proxies.rr.get_attr(id_list,[field])
            # assert results.empty != True, f"RR _get_attribution return empty df, probably start end date not correct. Start, end date: {start_end_time}, field: {field}"            
            data[f'{self.name}/{field}'] = results
        return data

    def _get_id_map(self, proxies):
        return {f'{self.name}/id_map': pd.DataFrame(proxies.rr.get_id_mapping())}


def _parse_result(results):
    return results
