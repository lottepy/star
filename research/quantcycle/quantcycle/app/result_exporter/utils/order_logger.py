"""
This module provides logging support for
   - order checker
"""

import os
from quantcycle.utils.time_manipulation import *


class OrderLogger:
    def __init__(self, save_folder='.'):
        """
        :param save_folder: (Optional) The default is the current dir.
        """
        self.save_folder = save_folder
        if not os.path.exists(save_folder):
            os.makedirs(save_folder)

        self.__checker_fout = None
        self.__last_checker_file = None
        self.__last_checker_len = 0

    def __del__(self):
        if self.__checker_fout:
            try:
                self.__checker_fout.close()
            except Exception:
                pass

    def flush_checker_log(self):
        if self.__checker_fout:
            self.__checker_fout.flush()
        else:
            raise IOError('Checker log file is unexpectedly closed or never opened?')

    def dump_checker_log(self, filename: str, log_dict_list: list):
        """
        Dump messages in log_dict. This function will clear every log_dict in log_dict_list.
        The function expects the number of dicts in log_dict_list = number of strategies.
        This implementation is not thread safe.
        :param filename: The text file name excluding folder path
                         Empty string or '#close#' will explicitly close this file.
        :param log_dict_list: Each is a dict {timestamp: msg}
        :return: None
        """

        if filename != self.__last_checker_file:   # new file
            self.__last_checker_file = filename
            self.__last_checker_len = len(log_dict_list)
            if self.__checker_fout:
                self.__checker_fout.close()
            self.__checker_fout = open(os.path.join(self.save_folder, filename), 'w')

        if len(log_dict_list) != self.__last_checker_len:
            raise ValueError('The number of dicts in log_dict_list does not match the previous record.')

        # Collect all timestamps and sort
        all_timestamps = set()
        for log_dict in log_dict_list:
            all_timestamps.update(log_dict)
        all_timestamps = list(all_timestamps)
        all_timestamps.sort()
        all_timestamps_repr = timestamp2datetimestring(all_timestamps)

        # Dump
        for idx_t, timestamp in enumerate(all_timestamps):
            timestamp_repr = all_timestamps_repr[idx_t]
            for idx_l, log_dict in enumerate(log_dict_list):
                if timestamp not in log_dict:
                    continue

                sid = idx_l
                msg = log_dict[timestamp]
                line = '{} {} {:>3}: {}\n'.format(timestamp, timestamp_repr, sid, msg)
                self.__checker_fout.write(line)

        # Clear
        for log_dict in log_dict_list:
            log_dict.clear()
