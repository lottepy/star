"""
Result reader module provides interface to read HDF5 storage

Reference: https://confluence-algo.aqumon.com/pages/viewpage.action?pageId=29655886 (outdated)
"""
import os
import h5py
import numpy as np
import pandas as pd
from quantcycle.app.result_exporter.result_export import get_keyfile
from quantcycle.utils.time_manipulation import *


class ResultReader:
    # Settings
    chunk_cache = dict(rdcc_nbytes=3145728)

    def __init__(self, result_file: str):
        # Indexing
        self.__hf_list = []
        self.__sid_list = []
        self.__sid2hf = {}
        result_file = self.__check_file_ext(result_file)
        self.__result_file = result_file
        self.__is_multi = self.__is_multi_files(result_file)
        self.index_files(result_file)       # for results spread in multiple files
        self.__sid2pos = {}
        self.build_sid2pos()

        # Global settings
        #! New names should be added here when new fields are created.
        self.all_historical_names = ['position', 'pnl', 'cost']     # each must exist in hdf5
        self.all_valid_historical_names = ['position', 'pnl', 'cost', 'phase']
        self.all_names_no_symbols = ['pnl', 'phase']     # fields without symbols
        self.all_attribute_names = ['params', 'ref_aum', 'symbols']

        # Caching 3d-array datasets
        self.__dataset_cache = []           # corresponding to self.__hf_list

    def __del__(self):
        for hf_in in self.__hf_list:
            if hf_in:
                try:
                    hf_in.close()
                except Exception:
                    pass

    @staticmethod
    def __check_file_ext(result_file):
        split_path = os.path.splitext(result_file)
        if not split_path[1]:
            result_file = split_path[0] + '.hdf5'       # assume default extension .hdf5
        return result_file

    @staticmethod
    def __is_multi_files(result_file: str):
        if os.path.exists(result_file):
            return False
        else:
            result_files = ResultReader.__get_filenames(result_file)
            if len(result_files) > 0:
                return True
            else:
                raise ValueError('File not found.')

    @staticmethod
    def __get_filenames(result_file: str):
        # Get all valid files that are parts of result_file
        filename = os.path.basename(result_file)
        filename_split = os.path.splitext(filename)
        filename_main = filename_split[0]
        filename_ext = filename_split[1]
        path_dir = os.path.dirname(result_file)
        result_files = [name for name in os.listdir(path_dir)
                        if name.find(filename_main) == 0 and name.endswith(filename_ext)]
        return result_files


    def index_files(self, result_file: str):
        """
        :return: a. id2hf dict {strategy_id: hf_in_index}
                 b. hf_list
                 c. sid_list
        """
        self.__hf_list = []         # opened hdf5 files
        self.__sid_list = []        # strategy ID list in each hdf5 file
        self.__sid2hf = {}          # strategy ID to hdf5 lookup
        if not self.__is_multi:
            # ---------------
            # For single file
            # ---------------
            hf_in = h5py.File(result_file, 'r', **self.chunk_cache)
            self.__hf_list.append(hf_in)
            keyfile = get_keyfile(result_file)
            if os.path.exists(keyfile):
                lines = open(keyfile, 'r').readlines()
                sid_list = list(map(int, lines))
            elif 'strategy_id' in hf_in:        # find backup
                sid_list = list(hf_in['strategy_id'])
            else:
                raise FileNotFoundError('Could not retrieve strategy ID sequence as key files are missing.')
                # print("Warning: Missing key files. Indexing from HDF5 instead but it will be slow.")
                # self.__sid2hf = {int(key): 0 for key in hf_in['strategy_attr'].keys()}      # slow for large HDF5 file
            self.__sid_list.append(sid_list)
            self.__sid2hf = {strategy_id: 0 for strategy_id in sid_list}
        else:
            # -------------------------------------------------------------------
            # For multi files, given a strategy_id, index which HDF5 contains it
            # -------------------------------------------------------------------
            filenames = self.__get_filenames(result_file)
            folder = os.path.dirname(result_file)
            for idx, filename in enumerate(filenames):
                hf_in = h5py.File(os.path.join(folder, filename), 'r', **self.chunk_cache)
                self.__hf_list.append(hf_in)
                keyfile_name = get_keyfile(filename)
                keyfile = os.path.join(folder, keyfile_name)
                if os.path.exists(keyfile):
                    lines = open(keyfile, 'r').readlines()
                    sid_list = list(map(int, lines))
                    self.__sid_list.append(sid_list)
                elif 'strategy_id' in hf_in:        # find backup
                    sid_list = list(hf_in['strategy_id'])
                    self.__sid_list.append(sid_list)
                else:
                    raise FileNotFoundError('Could not retrieve strategy ID sequence as key files are missing.')
                    # print('Warning: Missing key files. Indexing from HDF5 instead but it will be slow.')
                    # for key in hf_in['strategy_attr'].keys():       # slow for large HDF5 file
                    #     strategy_id = int(key)
                    #     if strategy_id in self.__sid2hf:
                    #         raise Exception(f'Index failed as files contain duplicate strategy_id {strategy_id}.')
                    #     self.__sid2hf[strategy_id] = idx
                for strategy_id in sid_list:
                    if strategy_id in self.__sid2hf:
                        raise Exception(f'Index failed as files contain duplicate strategy_id {strategy_id}.')
                    self.__sid2hf[strategy_id] = idx

        assert len(self.__hf_list) == len(self.__sid_list)
        return self.__sid2hf, self.__hf_list, self.__sid_list


    def build_sid2pos(self):
        """
        Index in order to get the strategy ID's position along the ID's dimension of the 3d-array datasets
        :return: sid2pos dict {strategy_id: id_dim_pos}
        """
        assert self.__sid_list
        self.__sid2pos = {}

        for sid_list in self.__sid_list:
            for idx, sid in enumerate(sid_list):
                self.__sid2pos[sid] = idx
        return self.__sid2pos

    def clear_cache(self, id_list: list):
        """
        Clear reader cache at the given strategy IDs.
        :param id_list: A list of strategy_id to request
        :return: None
        """
        if not self.__dataset_cache:
            return

        id_set = set(id_list)
        for hf_idx, cache in enumerate(self.__dataset_cache):
            if [sid for sid in self.__sid_list if sid in id_set]:
                cache = {}


    def get_strategy_3d_fast(self, id_range, field: str, start_end_time=None, phase=None):
        """
        Alternative to get_strategy(). This method returns a 3d array as a whole.
        :param id_range: A tuple of strategy ID range (start_id, end_id) to request
        :param field: A list of field names
        :param start_end_time: (Optional) Truncate time by (start, end)
        :param phase: (Optional) A list of time field filters, each is a string
        :return: (data, time_1d, sid_1d, symbol_2d)
                 data: A 3d array with dimensions defined as (time, strategy IDs, symbols) or (time, strategy IDs,
                 symbol/field) if the length of the third dimension is 1.
                 time_1d, sid_1d, symbol_2d: Indices of the three dimensions. Symbol indices are 2D arrays
                 (ID x symbols), while time and strategy IDs are in 1D arrays respectively.
        """
        raise NotImplementedError


    def get_strategy_3d(self, id_list, field: str, start_end_time=None, phase=None, **kwargs):
        """
        Alternative to get_strategy(). This method returns a 3d array as a whole.
        Recommend to load as many strategy IDs as possible. start_end_time and phase filters assume all strategies
        share the same time and phase sequences.
        :param id_list: A list of strategy_id to request
        :param field: A field name such as "position", "pnl" and "cost"
        :param start_end_time: (Optional) Truncate time by (start, end)
        :param phase: (Optional) A list of time field filters, each is a string
        :return: (data, time_1d, sid_1d, symbol_2d)
                 data: A 3d array with dimensions defined as (time, strategy IDs, symbols) or (time, strategy IDs,
                 symbol/field) if the length of the third dimension is 1.
                 time_1d, sid_1d, symbol_2d: Indices of the three dimensions. Symbol indices are 2D arrays
                 (ID x symbols), while time and strategy IDs are in 1D arrays respectively.
        :[Optional params]
        more_safety_checks: False by default. Set true if you need more checks on data integrity but expect drawbacks
                             in performance.
        """
        self.__check_fields(field)

        # Time filters
        start, end = None, None
        if start_end_time:
            start, end = start_end_time

        # Init cache
        if not self.__dataset_cache:
            self.__dataset_cache = [{} for i in range(len(self.__hf_list))]

        requested_sids = set(id_list)
        requested_sid2idx = {sid: idx for idx, sid in enumerate(id_list)}
        data = None         # should fill with 3d arrays
        time_1d, sid_1d, symbol_2d = None, None, None
        phase_vec = None
        timestamps = None
        time_idx = None
        # For safety checks
        last_timestamps_len = None
        last_phase_len = None
        for hf_idx, sid_list in enumerate(self.__sid_list):
            hf_in = self.__hf_list[hf_idx]

            sid_picked = np.array([[idx, sid] for idx, sid in enumerate(sid_list) if sid in requested_sids])
            if sid_picked.size == 0:        # no record is needed in this file, skip it
                continue
            sid_picked_idx = sid_picked[:, 0]

            self.__load_dataset_cache(hf_in, hf_idx, field, phase)

            if hf_idx == 0:         # run once
                # "phase" is an additional cross-filter for time selection used by live trading engine
                if phase is not None:
                    phase_vec = self.__dataset_cache[hf_idx]['phase'][0, :]   # all strategies share the same phase sequence
                    phase = set(phase) if not isinstance(phase, str) else {phase}

                # Get the indices of the requested time range
                timestamps = self.__dataset_cache[hf_idx]['time'][0, :]       # all strategies share the same time sequence
                time_idx = self.__get_filtered_time_idx(timestamps, start, end, phase_vec, phase)

                # Safety checks
                if 'more_safety_checks' in kwargs and kwargs['more_safety_checks']:
                    last_phase_len = phase_vec.shape[0] if phase is not None else None
                    last_timestamps_len = timestamps.shape[0]
                    if last_phase_len:
                        assert last_phase_len == last_timestamps_len
            else:
                if 'more_safety_checks' in kwargs and kwargs['more_safety_checks']:
                    if phase is not None:
                        phase_vec = self.__dataset_cache[hf_idx]['phase'][0, :]
                    timestamps = self.__dataset_cache[hf_idx]['time'][0, :]
                    if last_phase_len:
                        assert last_phase_len == phase_vec.shape[0]
                    assert last_timestamps_len == timestamps.shape[0]       # assert uniform data across hdf5 files

            # Data
            if time_idx is None:
                if data is None:
                    # Prepare indices
                    time_1d = timestamps
                    symbol_2d = self.__get_symbol_2d(hf_in, field, sid_picked)      # 2d (strategy_id, symbol)
                    sid_1d = sid_picked[:, 1]                                       # strategy IDs
                    data = self.__get_data_3d(hf_idx, field, sid_picked_idx)
                else:
                    symbol_2d = np.concatenate((symbol_2d, self.__get_symbol_2d(hf_in, field, sid_picked)), axis=0)
                    sid_1d = np.concatenate((sid_1d, sid_picked[:, 1]), axis=0)
                    data = np.concatenate((data, self.__get_data_3d(hf_idx, field, sid_picked_idx)), axis=1)
            else:       # time filtered
                if data is None:
                    time_1d = timestamps[time_idx]
                    symbol_2d = self.__get_symbol_2d(hf_in, field, sid_picked)      # 2d (strategy_id, symbol)
                    sid_1d = sid_picked[:, 1]
                    data = self.__get_data_3d(hf_idx, field, sid_picked_idx, time_idx)
                else:
                    symbol_2d = np.concatenate((symbol_2d, self.__get_symbol_2d(hf_in, field, sid_picked)), axis=0)
                    sid_1d = np.concatenate((sid_1d, sid_picked[:, 1]), axis=0)
                    data = np.concatenate((data, self.__get_data_3d(hf_idx, field, sid_picked_idx, time_idx)), axis=1)

        assert data.shape == (time_1d.shape[0], sid_1d.shape[0], symbol_2d.shape[1])

        # Permutation to match id_list
        sid2reqidx = np.array([requested_sid2idx[sid] for sid in sid_1d])
        data = data[:, sid2reqidx, :]
        # assert all(sid_1d[sid2reqidx] == np.array(id_list))

        return data, time_1d, np.array(id_list), symbol_2d

    @staticmethod
    def __get_filtered_time_idx(timestamps, start, end, phase_vec, phase):
        time_idx = None
        if start is not None and end is not None:
            if phase is None:
                time_idx = [i for i, v in enumerate(timestamps) if start <= v <= end]
            else:
                time_idx = [i for i, v in enumerate(timestamps) if start <= v <= end
                            and phase_vec[i].decode() in phase]
        elif phase is not None:
            time_idx = [i for i, v in enumerate(phase_vec) if v.decode() in phase]
        if time_idx:
            time_idx = np.array(time_idx)
        return time_idx

    def __get_data_3d(self, hf_idx, field, sid_filter, time_filter=None):
        data = self.__dataset_cache[hf_idx][field]
        data = data[sid_filter]
        if data.ndim == 3:
            data = data.transpose((1, 0, 2))
        elif data.ndim == 2:
            data = data.transpose()
            data = data[:, :, np.newaxis]  # extend to 3d
        else:
            raise ValueError(f'Shape of the {field} dataset is unexpected.')
        assert data.ndim == 3
        data = data[time_filter] if time_filter is not None else data
        return data

    def __get_symbol_2d(self, hf_in, field, sid_picked):
        symbol_2d = []
        if field in set(self.all_names_no_symbols):
            symbol_2d = [[field] for i in range(sid_picked.shape[0])]
        else:
            for sid in sid_picked[:, 1]:
                try:
                    group_attr = hf_in[f'/strategy_attr/{sid}']
                except KeyError:
                    raise KeyError(f'Strategy {sid} not found')
                symbols = group_attr.attrs['symbols']
                if isinstance(symbols, str):
                    symbols = [symbols]
                symbol_2d.append(symbols)
        symbol_2d = np.array(symbol_2d)
        assert symbol_2d.ndim == 2
        return symbol_2d


    def get_strategy(self, id_list, fields: list, start_end_time=None, phase=None, df_disable=False):
        """
        Get historical fields, i.e. position, pnl, and cost by strategy IDs.
        :param id_list: A list of strategy_id to request
        :param fields: A list of field names
        :param start_end_time: (Optional) Truncate time by (start, end)
        :param phase: (Optional) A list of time field filters, each is a string
        :param df_disable: True/False. Set True for much better performance.
        :return: For performance, explicitly set df_disable True. Return a dictionary {strategy_id: [list of results]}.
                 The order of results in the list matches the order of requested fields. Each result is a tuple
                 (np_array, row_indices, column_indices). By default, df_disable=False, each result is a DataFrame.
        """
        data = {}
        self.__check_fields(fields)

        # Init cache
        if not self.__dataset_cache:
            self.__dataset_cache = [{} for i in range(len(self.__hf_list))]

        start, end = None, None
        if start_end_time:
            start, end = start_end_time
        for sid in id_list:
            sid = int(sid)
            hf_idx = self.__sid2hf[sid]
            try:
                hf_in = self.__hf_list[hf_idx]
                # noinspection PyTypeChecker
                group_attr = hf_in[f'/strategy_attr/{sid}']
            except KeyError:
                raise KeyError(f'Strategy {sid} not found')

            self.__load_dataset_cache(hf_in, hf_idx, fields, phase)

            # "phase" is an additional cross-filter for time selection used by live trading engine
            sid_pos = self.__sid2pos[sid]
            phase_vec = None
            if phase is not None:
                phase_vec = self.__dataset_cache[hf_idx]['phase'][sid_pos, :]
                phase = set(phase) if not isinstance(phase, str) else {phase}

            # Get the indices of the requested time range
            timestamps = self.__dataset_cache[hf_idx]['time'][sid_pos, :]
            time_idx = self.__get_filtered_time_idx(timestamps, start, end, phase_vec, phase)

            symbols = group_attr.attrs['symbols']
            if isinstance(symbols, str):            # when there is only one symbol, e.g. 'XYZ'
                symbols = [symbols]
            dt_list = []
            for field in fields:
                dt = self.__dataset_cache[hf_idx][field][sid_pos]       # dt can be 2D or 1D
                row_idx = timestamps
                if dt.ndim == 1 and len(symbols) != 1:
                    col_idx = [field]
                else:
                    col_idx = symbols
                assert dt.shape[0] == len(row_idx)
                if dt.ndim == 2:
                    assert dt.shape[1] == len(col_idx)
                if df_disable:
                    if time_idx is not None:
                        dt_list.append((dt[time_idx, :], [row_idx[i] for i in time_idx], col_idx))
                    else:
                        dt_list.append((dt, row_idx, col_idx))
                else:
                    if time_idx is not None:
                        dt = dt[time_idx]
                        dt_list.append(pd.DataFrame(data=dt, index=[row_idx[i] for i in time_idx],
                                                    columns=col_idx))
                    else:
                        dt_list.append(pd.DataFrame(data=dt, index=row_idx, columns=col_idx))
            data[sid] = dt_list
        return data

    def __load_dataset_cache(self, hf_in, hf_idx, fields, phase=None):
        # Load datasets
        if isinstance(fields, str):
            fields = [fields]
        cache_fields = fields[:]
        cache_fields.append('time')
        if phase:
            cache_fields.append('phase')
        for field in cache_fields:
            if field not in self.__dataset_cache[hf_idx]:
                try:
                    self.__dataset_cache[hf_idx][field] = np.array(hf_in[field])
                except KeyError:
                    raise KeyError(f'Cannot find field "{field}" in File {hf_idx}.')

    def __check_fields(self, fields):
        if isinstance(fields, str):
            fields = [fields]
        valid_fields = set(self.all_valid_historical_names)
        bad_fields = [f for f in fields if f not in valid_fields]
        if bad_fields:
            raise ValueError(f'Invalid field name "{bad_fields[0]}"')


    def get_attr(self, id_list, fields: list):
        """
        Get non-historical attributes, i.e. strategy_params, ref_aum, and symbols by strategy IDs.
        :param id_list: A list of strategy_id to request
        :param fields: A list of field names
        :return: A DataFrame with requested fields as columns
        """
        valid_fields = set(self.all_attribute_names)
        bad_fields = [f for f in fields if f not in valid_fields]
        if bad_fields:
            raise ValueError(f'Invalid field name "{bad_fields[0]}"')

        # assert len(set(id_list)) == len(id_list)        # assert no duplicates, performance dips otherwise
        df = pd.DataFrame(index=id_list, columns=fields)
        df.index.name = 'strategy_id'
        for idx, sid in enumerate(id_list):
            hf_in = self.__hf_list[self.__sid2hf[int(sid)]]
            try:
                # noinspection PyTypeChecker
                group = hf_in[f'/strategy_attr/{sid}']
            except KeyError:
                raise KeyError(f'Strategy {sid} not found')
            values = [group.attrs[name] for name in fields]
            row = dict(zip(fields, values))
            df.iloc[idx] = pd.Series(row)
        return df


    def get_id_mapping(self):
        """
        Get strategy ID mapping table (after flattening -> before flattening). If flatten is false, the
        post-flattened and pre-flattened columns are the same.
        :return: A 2D-array. Its 1st column is the post-flattened IDs and 2nd column is the corresponding
                 pre-flattened IDs.
        """
        id_mapping = np.empty((0, 2), dtype=int)
        for hf_in in self.__hf_list:
            id_mapping = np.concatenate((id_mapping, hf_in['id_mapping']), axis=0)
        return id_mapping


    def to_csv(self, export_folder: str, id_list=None, fields=None, start_end_time=None):
        """
        Save results to csv files
        :param export_folder: The folder to export
        :param id_list: (Optional) A list of strategy_id to request
        :param fields: (Optional) A list of field names or a string '+phase'. The func exports all fields
                       if ignored. To ease live trading debugging, '+phase' will also export phase field.
        :param start_end_time: (Optional) Truncate time by (start, end)
        :return: None
        """
        if not os.path.exists(export_folder):
            os.makedirs(export_folder)
        if not id_list:
            id_list = self.__sid2hf.keys()

        if not fields:
            fields = self.all_historical_names
        elif fields == '+phase':    # to ease live trading debugging
            fields = self.all_historical_names.copy()
            fields.append('phase')
        historical_data = self.get_strategy(id_list, fields, start_end_time, df_disable=True)
        df_attrib = self.get_attr(id_list, self.all_attribute_names)
        df_attrib.to_csv(os.path.join(export_folder, 'attrib.csv'))

        for sid, df_list in historical_data.items():
            strategy_folder = os.path.join(export_folder, str(sid))
            if not os.path.exists(strategy_folder):
                os.mkdir(strategy_folder)
            for idx, data in enumerate(df_list):
                filename = fields[idx] + '.csv'
                df = pd.DataFrame(data=data[0], index=data[1], columns=data[2])
                readable_timestamps = timestamp2datetimestring(df.index)
                df.insert(0, 'datetime', readable_timestamps)
                df.to_csv(os.path.join(strategy_folder, filename))


    def export_summary(self, summary_file: str, id_list=None):
        """
        Generate result summary for strategies. The columns include symbols, final pnl, and strategy parameters.
        :param summary_file: File path to the output
        :param id_list: (Optional) A list of strategy_id to request
        :return: None
        """
        output_folder = os.path.dirname(summary_file)
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
        if not id_list:
            id_list = self.__sid2hf.keys()

        historical_pnl = self.get_strategy(id_list, ['pnl'], df_disable=True)
        final_pnl_data = [historical_pnl[i][0][0][-1] for i in id_list]
        df_attrib = self.get_attr(id_list, self.all_attribute_names)
        df_attrib.insert(1, 'final_pnl', final_pnl_data)
        df_attrib.to_csv(summary_file)


    def get_all_sids(self):
        """
        :return: All strategy IDs
        """
        return self.__sid2hf.keys()
