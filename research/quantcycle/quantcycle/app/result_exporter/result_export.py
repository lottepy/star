"""
Result export module that works to
    - save PMS history from backtesting in HDF5 format
    - extract data from PMS in live trading
    - save data from PMS in live trading

The core functions are save_realtime_data() and export(). They share the similar logic.

Reference: https://confluence-algo.aqumon.com/pages/viewpage.action?pageId=29655886 (outdated)
"""
import os
import h5py
import numpy as np
from multiprocessing import Process


def get_keyfile(hdf5_file):
    return os.path.splitext(hdf5_file)[0] + '.keys'


def patch_filename_for_parallel(filename, kwargs):
    if 'process_id' in kwargs:
        process_id = int(kwargs['process_id'])
        if process_id >= 0:
            filename_split = os.path.splitext(filename)
            if filename_split[1]:
                filename = filename_split[0] + f'-p{process_id}' + filename_split[1]
            else:
                filename = filename_split[0] + f'-p{process_id}.hdf5'
        else:
            raise ValueError('process_id should not be < 0.')
    else:       # single process
        filename_split = os.path.splitext(filename)
        if not filename_split[1]:       # no file extension
            filename += '.hdf5'
    return filename


class ResultExport:
    # Settings
    chunkcache_backtest = dict(rdcc_nbytes=3145728, rdcc_w0=1)
    compression_backtest = dict(compression='gzip', compression_opts=1)
    compression_realtime = dict(compression='gzip', compression_opts=1)

    def __init__(self, export_folder: str):
        self.export_folder = export_folder
        self.__strategy_id_export = 0
        self.__strategy_id_extract = 0
        self.__strategy_id_save_realtime = 0
        self.__pms_id_export = 0
        self.__pms_id_save_realtime = 0
        if not os.path.exists(export_folder):
            os.makedirs(export_folder)

        # For save_realtime_data
        self.__save_last_filename = None
        self.__save_hf_out = None
        self.__sid2pos = {}                     # writing status register
        self.__sid_n_timestamps = {}            # writing status register
        self.__last_pms_pack_length = 0

    def __del__(self):
        if self.__save_hf_out:
            try:
                self.__save_hf_out.close()
            except Exception:
                pass

    def save_realtime_data_flush(self):
        """
        Write cache into file on call.
        :return: None
        """
        if self.__save_hf_out:
            self.__save_hf_out.flush()
        else:
            raise IOError('The HDF5 file was closed before the flush call?')

    def save_realtime_data(self, filename, *pms_symb_params_time, phase='', flatten=False, reset_id=-1, **kwargs):
        """
        Save data from PMS to HDF5 in live trading
        :param filename: Output file name, excluding folder path.
                         Empty string or '#close#' will explicitly close HDF5 file.
        :param pms_symb_params_time: Each is a tuple (PMS, symbols, strategy_params, timestamp).
        :param phase: A phase indicator (string) assigned to the time-series fields for this timestamp.
        :param flatten: True/False, if true, each symbol in PMS is treated as a strategy
                        instead of a PMS as a whole.
        :param reset_id: >= 0 to reset strategy_id
        :return: None
        :[Optional params]
        process_id: >= 0 for multiprocessing. Among all processes, process_id should start from 0 and be continuous.
                    Don't overlap strategy_id between processes!
        check_pms_length: True/False. Set False to disable pms package length check when appending data to existing
                          strategy IDs.
        """

        if reset_id >= 0:
            self.__strategy_id_save_realtime = reset_id
            self.__pms_id_save_realtime = reset_id

        # Opening multiple hf_out simultaneously is NOT supported by this logic
        # To workaround, you need to start a new instance.
        hf_out = None
        keyfile_path = None
        if filename != self.__save_last_filename:       # new hdf5 file
            self.__save_last_filename = filename
            if self.__save_hf_out:
                self.__save_strategy_status(self.__save_hf_out)
                self.__save_hf_out.close()
            self.__last_pms_pack_length = 0             # reset
            self.__sid_n_timestamps = {}                # reset

            if filename and filename != '#close#':
                filename = patch_filename_for_parallel(filename, kwargs)
                filepath = os.path.join(self.export_folder, filename)
                keyfile_name = get_keyfile(filename)
                keyfile_path = os.path.join(self.export_folder, keyfile_name)
                # File append mode is no longer supported for the complication of
                # status maintained for writing each file
                if os.path.exists(filepath) or os.path.exists(keyfile_path):
                    raise ValueError('[Overwrite Abort] The output files already exist.')
                hf_out = h5py.File(filepath, 'w')       # mode=append is not supported currently
                self.__save_hf_out = hf_out
                self.__build_sid2pos(filename)
                #! Disable in production mode!
                # if 'strategy_attr' in hf_out:
                #     assert len(self.__sid2pos) == len(hf_out['strategy_attr'].keys())
            else:
                # hf_out = None
                self.__save_hf_out = None
        else:
            hf_out = self.__save_hf_out
            # Simple safety check, not perfect
            if reset_id in self.__sid2pos:
                if 'check_pms_length' in kwargs and not kwargs['check_pms_length']:
                    pass
                elif self.__last_pms_pack_length and len(pms_symb_params_time) != self.__last_pms_pack_length:
                    raise ValueError("PMS package length doesn't match the length in the last call.")
            if reset_id >= 0:
                self.__last_pms_pack_length = len(pms_symb_params_time)

        if hf_out is None:
            return

        if not keyfile_path:
            keyfile_path = os.path.join(self.export_folder, get_keyfile(filename))
        with open(keyfile_path, 'a') as keyf_out:
            for pms, symbols, params, time in pms_symb_params_time:
                position = np.array(pms.current_holding, ndmin=3)
                pnl = np.array(pms.pnl, ndmin=3)
                pnl_sum = np.sum(pnl, axis=2)       #! pnl patch as pms doesn't provide pnl per portfolio
                cost = np.array(pms.historial_commission_fee[-1], ndmin=3)
                assert len(symbols) == position.shape[2] == pnl.shape[2] == cost.shape[2]
                if isinstance(time, str):
                    try:
                        time_ext = np.array(time.encode('ascii'), ndmin=2)
                    except UnicodeEncodeError:
                        raise ValueError(f'Timestamp f{time} contains non-ascii chars.')
                else:
                    time_ext = np.array(time, ndmin=2)
                try:
                    phase_ext = np.array(phase.encode('ascii'), ndmin=2)
                except UnicodeEncodeError:
                    raise ValueError(f'Phase f{phase} contains non-ascii chars.')
                ref_aum = pms.init_cash
                params_vec = np.array(params)

                if not flatten:
                    self.__write_a_strategy_realtime(hf_out, symbols, position, pnl_sum, cost, time_ext, phase_ext,
                                                     ref_aum, params_vec, self.__pms_id_save_realtime, keyf_out)
                else:
                    n_symbols = len(symbols)
                    for j in range(n_symbols):
                        pnl_sum = pnl[:, :, j]
                        self.__write_a_strategy_realtime(hf_out, symbols[j], position[:, :, j], pnl_sum, cost[:, :, j],
                                                         time_ext, phase_ext, ref_aum / n_symbols, params_vec,
                                                         self.__pms_id_save_realtime, keyf_out)
                self.__pms_id_save_realtime += 1

        # Save how many time stamps are written to every strategy ID
        #! Disabled due to performance impact!
        # Explicitly call save_realtime_data('#close#') instead to write the status.
        # strategy_status = [item for item in self.__sid_n_timestamps.items()]
        # if 'strategy_status' not in hf_out:
        #     hf_out.create_dataset('strategy_status', data=strategy_status, maxshape=(None, 2))
        # else:
        #     hf_out['strategy_status'].resize(len(strategy_status), axis=0)
        #     hf_out['strategy_status'][...] = strategy_status

    def __save_strategy_status(self, hf_out):
        strategy_status = [item for item in self.__sid_n_timestamps.items()]
        hf_out.create_dataset('strategy_status', data=strategy_status)

    def __build_sid2pos(self, filename):
        self.__sid2pos = {}
        keyfile_name = get_keyfile(filename)
        keyfile_path = os.path.join(self.export_folder, keyfile_name)
        if not os.path.exists(keyfile_path):        # new hdf5
            return self.__sid2pos

        lines = open(keyfile_path, 'r').readlines()
        sid_list = map(int, lines)
        self.__sid2pos = {sid: idx for idx, sid in enumerate(sid_list)}
        return self.__sid2pos

    def __write_a_strategy_realtime(self, hf_out, symbols, position, pnl, cost, time, phase, ref_aum, params,
                                    pms_id, keyf_out):
        if isinstance(symbols, str):        # symbols may be str
            symbols = [symbols]

        strategy_id = self.__strategy_id_save_realtime
        if strategy_id not in self.__sid2pos:
            group = hf_out.create_group(f'/strategy_attr/{strategy_id}')
            keyf_out.write(f'{strategy_id}\n')
            group.attrs.create('ref_aum', ref_aum)
            group.attrs.create('symbols', symbols)
            group.attrs.create('params', params)

            # ----------------------------------------
            # Dataset's 3d-array dimension definition: (strategy_id, time, symbol)
            n_symbols = len(symbols)
            id_pair = [[self.__strategy_id_save_realtime, pms_id]]    # save mapping: ID after flattening -> before flattening
            if 'position' not in hf_out:     # new hdf5
                assert len(self.__sid2pos) == 0
                self.__sid2pos[strategy_id] = 0
                self.__sid_n_timestamps[strategy_id] = 1
                hf_out.create_dataset('id_mapping', data=id_pair, maxshape=(None, 2), **self.compression_realtime)
                if position.ndim == 3:       # no flatten
                    hf_out.create_dataset('position', data=position, maxshape=(None, None, n_symbols),
                                          **self.compression_realtime)
                    hf_out.create_dataset('pnl', data=pnl, maxshape=(None, None), **self.compression_realtime)
                    hf_out.create_dataset('cost', data=cost, maxshape=(None, None, n_symbols),
                                          **self.compression_realtime)
                    hf_out.create_dataset('time', data=time, maxshape=(None, None), **self.compression_realtime)
                    dtype = h5py.string_dtype(encoding='ascii')
                    hf_out.create_dataset('phase', data=phase, maxshape=(None, None), dtype=dtype,
                                          **self.compression_realtime)
                else:
                    hf_out.create_dataset('position', data=position, maxshape=(None, None),
                                          **self.compression_realtime)
                    hf_out.create_dataset('pnl', data=pnl, maxshape=(None, None), **self.compression_realtime)
                    hf_out.create_dataset('cost', data=cost, maxshape=(None, None),
                                          **self.compression_realtime)
                    hf_out.create_dataset('time', data=time, maxshape=(None, None), **self.compression_realtime)
                    dtype = h5py.string_dtype(encoding='ascii')
                    hf_out.create_dataset('phase', data=phase, maxshape=(None, None), dtype=dtype,
                                          **self.compression_realtime)
            else:       # new strategy ID
                n_records = hf_out['position'].shape[0]
                self.__sid2pos[strategy_id] = n_records
                self.__sid_n_timestamps[strategy_id] = 1
                self.__dataset_append_row(hf_out, 'position', n_records, position)
                self.__dataset_append_row(hf_out, 'pnl', n_records, pnl)
                self.__dataset_append_row(hf_out, 'cost', n_records, cost)
                self.__dataset_append_row(hf_out, 'time', n_records, time)
                self.__dataset_append_row(hf_out, 'phase', n_records, phase)
                self.__dataset_append_row(hf_out, 'id_mapping', n_records, id_pair)
        else:           # strategy_id is seen
            n_records = hf_out['position'].shape[1]         # size of time dimension
            row_idx = self.__sid2pos[strategy_id]
            resize = True if self.__sid_n_timestamps[strategy_id] == n_records else False
            self.__dataset_append_time(hf_out, 'position', n_records, row_idx, position[0], resize)
            self.__dataset_append_time(hf_out, 'pnl', n_records, row_idx, pnl[0], resize)
            self.__dataset_append_time(hf_out, 'cost', n_records, row_idx, cost[0], resize)
            self.__dataset_append_time(hf_out, 'time', n_records, row_idx, time[0], resize)
            self.__dataset_append_time(hf_out, 'phase', n_records, row_idx, phase[0], resize)
            self.__sid_n_timestamps[strategy_id] += 1
        self.__strategy_id_save_realtime += 1

    @staticmethod
    def __dataset_append_row(h5_group, dataset_name, n_records, data):
        h5_group[dataset_name].resize(n_records + 1, axis=0)            # may lower efficiency
        h5_group[dataset_name][-1:] = data          # append to the dataset

    @staticmethod
    def __dataset_append_time(h5_group, dataset_name, n_records, row_idx, data, resize=False):
        if resize:
            h5_group[dataset_name].resize(n_records + 1, axis=1)
        h5_group[dataset_name][row_idx, -1:] = data

    def extract_data(self, *pms_list, flatten=False, reset_id=-1):
        """
        Extract data from PMS in live trading
        :param pms_list: Each is a PortfolioManager class.
        :param flatten: If true, each symbol in PMS is treated as a strategy
                        instead of a PMS as a whole.
        :param reset_id: >= 0 to reset strategy_id
        :return: Yield position, pnl, cost, and strategy_id.
        :[Optional params]
        process_id: >= 0 for multiprocessing. Among all processes, process_id should start from 0 and be continuous.
                    Don't overlap strategy_id between processes!
        """

        if reset_id >= 0:
            self.__strategy_id_extract = reset_id

        for pms in pms_list:
            position = pms.current_holding                      # not using np.array for speed
            pnl = pms.pnl
            cost = pms.historial_commission_fee[-1]
            assert len(position) == len(pnl) == len(cost)       # len = n_symbols

            if not flatten:
                yield position, pnl, cost, self.__strategy_id_extract
                self.__strategy_id_extract += 1
            else:
                for i in range(len(position)):
                    yield position[i], pnl[i], cost[i], self.__strategy_id_extract
                    self.__strategy_id_extract += 1

    def export_parallel(self, filename, n_workers: int, all_pms: list, flatten=False):
        """
        An encapsulated parallel export for easy use. Note that this method manages to partition the pms list
        and feed them to multiple workers. As a result, all pms must be put at once.
        :param filename: Output file name, excluding folder path
        :param n_workers: Number of processes for concurrency.
        :param all_pms: A list of tuples, each is a (PMS, symbols, strategy_params) tuple.
        :param flatten: True/False, if true, each symbol in PMS is treated as a strategy
                        instead of a PMS as a whole.
        :return: None
        """
        if len(all_pms) == 0:
            return      # nothing to write
        if n_workers > len(all_pms) > 0:
            # raise ValueError(f'Too few pms for {n_workers} workers.')
            n_workers = len(all_pms)
        batch_size = len(all_pms) // n_workers
        jobs = []
        n_symbols_processed = 0
        for batch_id in range(n_workers):
            slice_idx = (batch_id * batch_size, (batch_id + 1) * batch_size)
            if batch_id != n_workers - 1:               # not the last batch
                pms_slice = all_pms[slice_idx[0]:slice_idx[1]]
            else:
                pms_slice = all_pms[slice_idx[0]:]      # last batch

            if not flatten:
                id_start = batch_id * batch_size
            else:
                id_start = n_symbols_processed
            n_symbols_processed += self.__get_num_symbols_from_slice(pms_slice)
            p = Process(target=self.export_parallel_worker, args=(filename, batch_id, pms_slice, id_start, flatten))
            jobs.append(p)
            p.start()
        # Block until all jobs are done
        for job in jobs:
            job.join()

    @staticmethod
    def __get_num_symbols_from_slice(pms_slice: list):
        n_symbols = 0
        for pms, symbols, params in pms_slice:
            n_symbols += len(symbols) if isinstance(symbols, list) else 1           # symbols may be str
        return n_symbols

    def export_parallel_worker(self, filename, worker_id, pms_pack, id_start, flatten):
        result_export = ResultExport(self.export_folder)
        result_export.export(filename, *pms_pack, flatten=flatten, reset_id=id_start,
                             process_id=worker_id)
        result_export.finish_export(filename, process_id=worker_id)

    def export(self, filename, *pms_symbols_params, flatten=False, reset_id=-1, **kwargs):
        """
        Save PMS history to HDF5 file
        :param filename: Output file name, excluding folder path
        :param pms_symbols_params: Each is a tuple (PMS, symbols, strategy_params).
        :param flatten: True/False, if true, each symbol in PMS is treated as a strategy
                        instead of a PMS as a whole.
        :param reset_id: >= 0 to reset strategy_id
        :return: None
        :[Optional params]
        process_id: >= 0 for multiprocessing. Among all processes, process_id should start from 0 and be continuous.
                    Don't overlap strategy_id between processes!
        """

        if reset_id >= 0:
            self.__strategy_id_export = reset_id
            self.__pms_id_export = reset_id

        filename = patch_filename_for_parallel(filename, kwargs)
        keyfile_name = get_keyfile(filename)
        with h5py.File(os.path.join(self.export_folder, filename), 'w', **self.chunkcache_backtest) as hf_out:
            with open(os.path.join(self.export_folder, keyfile_name), 'w') as keyf_out:
                for pms, symbols, params in pms_symbols_params:
                    position = pms.historial_position
                    pnl = pms.historial_pnl
                    cost = pms.historial_commission_fee
                    time = pms.historial_time
                    assert len(time) > 0
                    assert len(time) == len(position) == len(pnl) == len(cost)
                    assert len(symbols) == len(position[0]) == len(pnl[0]) == len(cost[0])

                    position_mat = np.array(position, ndmin=3)
                    pnl_mat = np.array(pnl, ndmin=3)
                    pnl_vec = np.sum(pnl_mat, axis=2)       #! pnl patch as pms doesn't provide pnl per portfolio
                    cost_mat = np.array(cost, ndmin=3)
                    time_vec = np.array(time, ndmin=2)
                    ref_aum = pms.init_cash
                    params_vec = np.array(params)

                    if not flatten:
                        self.__write_a_strategy(hf_out, symbols, position_mat, pnl_vec, cost_mat,
                                                time_vec, ref_aum, params_vec, self.__pms_id_export, keyf_out)
                    else:
                        n_symbols = len(symbols)
                        for j in range(n_symbols):
                            position_vec = position_mat[:, :, j]
                            pnl_vec = pnl_mat[:, :, j]
                            cost_vec = cost_mat[:, :, j]
                            self.__write_a_strategy(hf_out, symbols[j], position_vec, pnl_vec, cost_vec,
                                                    time_vec, ref_aum / n_symbols, params_vec, self.__pms_id_export,
                                                    keyf_out)
                    self.__pms_id_export += 1

    def finish_export(self, filename, **kwargs):
        """
        Optionally call this method to write strategy IDs into HDF5 file when all results are exported.
        Though strategy IDs are kept in a separate .keys file, call this method for added safety since
        this file is very important.
        :param filename: Same name when you call export()
        :return: None
        :[Optional params]
        process_id: Should be exactly the same as export()
        """
        filename = patch_filename_for_parallel(filename, kwargs)
        keyfile_name = get_keyfile(filename)
        try:
            lines = open(os.path.join(self.export_folder, keyfile_name), 'r').readlines()
        except FileNotFoundError:
            raise FileNotFoundError(f'Key file {keyfile_name} does not exist.')
        sid_list = []
        for line in lines:
            strategy_id = int(line)
            sid_list.append(strategy_id)
        with h5py.File(os.path.join(self.export_folder, filename), 'a', **self.chunkcache_backtest) as hf_out:
            hf_out.create_dataset('strategy_id', data=sid_list, **self.compression_backtest)

    def __write_a_strategy(self, hf_out, symbols, position_mat, pnl_vec, cost_mat, time_vec, ref_aum,
                           params_mat, pms_id, keyf_out):
        if isinstance(symbols, str):        # symbols may be str
            symbols = [symbols]

        h5_group = hf_out.create_group(f'/strategy_attr/{self.__strategy_id_export}')       # no prefix in group name
        keyf_out.write(f'{self.__strategy_id_export}\n')
        id_pair = [[self.__strategy_id_export, pms_id]]     # save mapping: ID after flattening -> before flattening
        self.__strategy_id_export += 1
        h5_group.attrs.create('ref_aum', ref_aum)
        h5_group.attrs.create('symbols', symbols)
        h5_group.attrs.create('params', params_mat)

        # ----------------------------------------
        # Dataset's 3d-array dimension definition: (strategy_id, time, symbol)
        if 'position' not in hf_out:         # new hdf5
            n_time_vec = time_vec.shape[1]
            n_symbols = len(symbols)
            hf_out.create_dataset('id_mapping', data=id_pair, maxshape=(None, 2), **self.compression_backtest)
            if position_mat.ndim == 3:       # no flatten
                hf_out.create_dataset('position', data=position_mat, maxshape=(None, n_time_vec, n_symbols),
                                      **self.compression_backtest)
                hf_out.create_dataset('pnl', data=pnl_vec, maxshape=(None, n_time_vec), **self.compression_backtest)
                hf_out.create_dataset('cost', data=cost_mat, maxshape=(None, n_time_vec, n_symbols),
                                      **self.compression_backtest)
                hf_out.create_dataset('time', data=time_vec, maxshape=(None, n_time_vec), **self.compression_backtest)
            else:
                hf_out.create_dataset('position', data=position_mat, maxshape=(None, n_time_vec),
                                      **self.compression_backtest)
                hf_out.create_dataset('pnl', data=pnl_vec, maxshape=(None, n_time_vec), **self.compression_backtest)
                hf_out.create_dataset('cost', data=cost_mat, maxshape=(None, n_time_vec), **self.compression_backtest)
                hf_out.create_dataset('time', data=time_vec, maxshape=(None, n_time_vec), **self.compression_backtest)
        else:
            n_records = hf_out['position'].shape[0]
            self.__dataset_append_row(hf_out, 'position', n_records, position_mat)
            self.__dataset_append_row(hf_out, 'pnl', n_records, pnl_vec)
            self.__dataset_append_row(hf_out, 'cost', n_records, cost_mat)
            self.__dataset_append_row(hf_out, 'time', n_records, time_vec)
            self.__dataset_append_row(hf_out, 'id_mapping', n_records, id_pair)
