"""
User save module provides interfaces for strategy algorithms to
   - save regularized data
"""
import os
import pandas as pd
import numpy as np
from quantcycle.utils.time_manipulation import *


class UserSave:
    def __init__(self, save_folder: str):
        self.save_folder = save_folder
        if not os.path.exists(save_folder):
            os.makedirs(save_folder)

        # Signal remark
        self.__remark_filename = None
        self.__all_remarks_dict = {}
        self.__new_remarks_dict = {}
        self.__all_remark_fields = []
        self.__buffer_size = 100    # num of records buffered before dumping to the file

    def flush_signal_remark(self):
        """
        Dump signal remarks' cache into the remark file.
        :return: None
        """
        data, field_list, timestrs = self.__get_df_data_from_remark(self.__new_remarks_dict)

        output_file = os.path.join(self.save_folder, self.__remark_filename)
        if not (set(field_list) - set(self.__all_remark_fields)):       # append ok
            df = pd.DataFrame(index=timestrs, data=data, columns=field_list)
            df_frame = pd.DataFrame(columns=self.__all_remark_fields)
            df_out = df_frame.append(df, sort=True)
            df_out.index.name = 'datetime'
            df_out.to_csv(output_file, mode='a', header=False)
            self.__all_remarks_dict.update(self.__new_remarks_dict)     #! risk of memory full
        else:   # new fields are detected
            self.__all_remarks_dict.update(self.__new_remarks_dict)
            data, field_list, timestrs = self.__get_df_data_from_remark(self.__all_remarks_dict)
            df = pd.DataFrame(index=timestrs, data=data, columns=field_list)
            df.index.name = 'datetime'
            df.to_csv(output_file)
            self.__save_field_names(self.__remark_filename, field_list)
            self.__all_remark_fields = field_list
        self.__new_remarks_dict = {}

    def open_signal_remark(self, filename, buffer_size=100):
        """
        For live trading engines to init a remark file.
        :param filename: Output file name, excluding folder path
        :param buffer_size: Number of records buffered before dumping to the file
        :return: None
        """
        self.__remark_filename = filename
        self.__all_remarks_dict = {}
        self.__new_remarks_dict = {}
        self.__all_remark_fields = []
        if buffer_size >= 0:
            self.__buffer_size = buffer_size
        else:
            raise ValueError('Wrong buffer size.')

    def update_signal_remark(self, new_remark: dict):
        """
        For live trading engines to add new remarks into the opened file.
        :param new_remark: A dict of new remarks {timestamp, {field_name, float}}
        :return: None
        """
        self.__new_remarks_dict.update(new_remark)
        if len(self.__new_remarks_dict) > self.__buffer_size:
            self.flush_signal_remark()

    def close_signal_remark(self):
        """
        For live trading engines to finish the remark file.
        :return: None
        """
        if len(self.__new_remarks_dict) > 0:
            self.flush_signal_remark()


    # ------------------------------------------
    # Independent implementation for backtesting
    # ------------------------------------------
    def save_signal_remark(self, filename, signal_remark: dict):
        """
        An independent interface for backtesting engines to save remarks as a whole.
        :param filename: Output file name, excluding folder path
        :param signal_remark: A dict {timestamp, {field_name, float}}
        :return: None
        """

        data, field_list, timestrs = self.__get_df_data_from_remark(signal_remark)

        output_file = os.path.join(self.save_folder, filename)
        df = pd.DataFrame(index=timestrs, data=data, columns=field_list)
        df.index.name = 'datetime'
        df.to_csv(output_file)
        self.__save_field_names(filename, field_list)

    @staticmethod
    def __get_df_data_from_remark(signal_remark: dict):
        all_field_names = set()
        for user_dict in signal_remark.values():
            all_field_names.update(user_dict.keys())
        n_rows = len(signal_remark)
        n_cols = len(all_field_names)
        # Indexing
        field_list = list(all_field_names)
        field_list.sort()
        field2idx = {name: idx for idx, name in enumerate(field_list)}
        # Unpack
        timestamps = [timestamp for timestamp in signal_remark]
        timestamps.sort()
        timestrs = timestamp2datetimestring(timestamps)
        data = np.empty((n_rows, n_cols), dtype=float)
        data[...] = np.nan
        for idx, timestamp in enumerate(timestamps):
            user_dict = signal_remark[timestamp]
            for field, value in user_dict.items():
                data[idx, field2idx[field]] = value
        return data, field_list, timestrs

    def __save_field_names(self, filename, field_list: list):
        filename_split = os.path.splitext(filename)
        filename = filename_split[0] + '.fields'
        output_file = os.path.join(self.save_folder, filename)
        output_str = '\n'.join(field_list)
        open(output_file, 'w').write(output_str + '\n')
