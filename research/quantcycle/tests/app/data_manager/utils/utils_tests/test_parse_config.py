import copy
from unittest import TestCase, mock

import pytest

from quantcycle.app.data_manager.utils.parse_configuration import (DATA_GROUP_MUST_HAVE, _examine_component,
                                   _parse_start_end_date,
                                   _read_local_csv_symbol,
                                   _type_confirm_transform, parse_config)

minimum_data_group = {
    "Label": "FX-RATE",
    "DataCenter": "DataMaster",
    "Symbol": ["AUDCAD", "USDTWD"]
}
rich_data_group = {
    "Label": "FX",
    "Type": "Download",
    "DataCenter": "DataMaster",
    "Symbol": ["AUDCAD", "USDTWD"],
    "SymbolArgs": {
        "DataSource": "BT150",
        "BackupDataSource": "BGNL"
    },
    "Fields": "OHLC",
    "StartDate": {
        "Year": 2012,
        "Month": 1,
        "Day": 1
    },
    "EndDate": {
        "Year": 2020,
        "Month": 2,
        "Day": 29
    },
    "Frequency": "DAILY"
}
default_values = {
    "Type": "Download",
    "Fields": "OHLC",
    "StartDate": {
        "Year": 2012,
        "Month": 1,
        "Day": 1
    },
    "EndDate": {
        "Year": 2020,
        "Month": 2,
        "Day": 29
    },
    "Frequency": "DAILY"
}

sample_config_dict = {
    "Data": {
        "DataGroup1": rich_data_group,
        "DataGroup2": minimum_data_group,
        "DefaultValues": default_values,
    }
}

mock_read_local_csv_symbol_path = \
    'quantcycle.app.data_manager.utils.' +\
    'parse_configuration._read_local_csv_symbol'
mock_parse_date_path = \
    'quantcycle.app.data_manager.utils.' +\
    'parse_configuration._parse_start_end_date'


class ExamineComponentTestCase(TestCase):

    def test_with_correct_dict(self):
        try:
            _examine_component(sample_config_dict)
        except AssertionError as e:
            self.fail(f"Assertion Error Raises with {e}")

    def test_without_data(self):
        self.assertRaises(AssertionError,
                          _examine_component,
                          {})

    def test_with_0_data_group(self):
        self.assertRaises(AssertionError,
                          _examine_component,
                          {"Data": {
                              "DefaultValues": default_values
                          }})

    def test_data_group_must(self):
        # either datagroup own all infomation
        try:
            _examine_component({
                "Data": {
                    "DataGroup1": rich_data_group
                }
            })
        except AssertionError as e:
            self.fail(f"Assertion Error Raises with {e}")
        # or some are stored at default values
        try:
            _examine_component({
                "Data": {
                    "DataGroup1": minimum_data_group,
                    "DefaultValues": default_values
                }
            })
        except AssertionError as e:
            self.fail(f"Assertion Error Raises with {e}")

    def test_data_group_lack_something(self):
        # delete something in rich_data_group and see if an Assertion Error will be raised.
        keys = list(rich_data_group.keys())
        for key in keys:
            if key not in DATA_GROUP_MUST_HAVE:
                continue
            dict_copy = copy.deepcopy(rich_data_group)
            dict_copy.pop(key)
            self.assertRaises(AssertionError,
                              _examine_component,
                              {"Data": {
                                  "DataGroup1": dict_copy
                              }})
        keys = list(minimum_data_group.keys())
        for key in keys:
            if key not in DATA_GROUP_MUST_HAVE:
                continue
            dict_copy = copy.deepcopy(minimum_data_group)
            dict_copy.pop(key)
            print(key)
            self.assertRaises(AssertionError,
                              _examine_component,
                              {"Data": {
                                  "DataGroup1": dict_copy,
                                  "DefaultValues": default_values
                              }})
        keys = list(default_values.keys())
        for key in keys:
            if key not in DATA_GROUP_MUST_HAVE:
                continue
            dict_copy = copy.deepcopy(default_values)
            dict_copy.pop(key)
            print(key)
            self.assertRaises(AssertionError,
                              _examine_component,
                              {"Data": {
                                  "DataGroup1": minimum_data_group,
                                  "DefaultValues": dict_copy
                              }})

    def test_with_skipped_data_group_number(self):
        try:
            _examine_component({
                "Data": {
                    "DataGroup2": rich_data_group,
                    "DataGroup4": minimum_data_group,
                    "DefaultValues": default_values
                }
            })
        except AssertionError as e:
            self.fail(f"Assertion Error Raises with {e}")

    def test_with_same_label(self):
        self.assertRaises(AssertionError,
                          _examine_component,
                          {"Data": {
                              "DataGroup1": rich_data_group,
                              "DataGroup2": rich_data_group
                          }})

    def test_output_length(self):
        output = _examine_component(sample_config_dict)
        assert len(output) == 2


class TypeConfirmTransformTestCase(TestCase):

    @mock.patch(mock_parse_date_path)
    @mock.patch(mock_read_local_csv_symbol_path)
    def test_label_type_correct(self, mock_read_csv, mock_parse_date):
        try:
            _type_confirm_transform({
                "Label": "FX"
            })
        except AssertionError as e:
            self.fail(f"Assertion Error Raises with {e}")

    @mock.patch(mock_parse_date_path)
    @mock.patch(mock_read_local_csv_symbol_path)
    def test_label_type_incorrect(self, mock_read_csv, mock_parse_date):
        self.assertRaises(AssertionError,
                          _type_confirm_transform,
                          {"Label": 1})

    @mock.patch(mock_parse_date_path)
    @mock.patch(mock_read_local_csv_symbol_path)
    def test_type_value_correct(self, mock_read_csv, mock_parse_date):
        try:
            _type_confirm_transform({
                "Type": "Download"
            })
        except AssertionError as e:
            self.fail(f"Assertion Error Raises with {e}")
        try:
            _type_confirm_transform({
                "Type": "Queue"
            })
        except AssertionError as e:
            self.fail(f"Assertion Error Raises with {e}")

    @mock.patch(mock_parse_date_path)
    @mock.patch(mock_read_local_csv_symbol_path)
    def test_type_value_incorrect(self, mock_read_csv, mock_parse_date):
        self.assertRaises(AssertionError,
                          _type_confirm_transform,
                          {"Type": "Other"})

    @mock.patch(mock_parse_date_path)
    @mock.patch(mock_read_local_csv_symbol_path)
    def test_data_center_type_correct(self, mock_read_csv, mock_parse_date):
        try:
            _type_confirm_transform({
                "DataCenter": "DataMaster"
            })
        except AssertionError as e:
            self.fail(f"Assertion Error Raises with {e}")

    @mock.patch(mock_parse_date_path)
    @mock.patch(mock_read_local_csv_symbol_path)
    def test_data_center_type_incorrect(self, mock_read_csv, mock_parse_date):
        self.assertRaises(AssertionError,
                          _type_confirm_transform,
                          {"DataCenter": 1})
        self.assertRaises(AssertionError,
                          _type_confirm_transform,
                          {"DataCenter": []})

    @mock.patch(mock_parse_date_path)
    @mock.patch(mock_read_local_csv_symbol_path)
    def test_data_center_value_incorrect(self, mock_read_csv, mock_parse_date):
        self.assertRaises(AssertionError,
                          _type_confirm_transform,
                          {"DataCenter": "FakeDataCenter"})

    @mock.patch(mock_parse_date_path)
    @mock.patch(mock_read_local_csv_symbol_path)
    def test_data_center_value_correct(self, mock_read_csv, mock_parse_date):
        try:
            _type_confirm_transform({
                "DataCenter": "DataMaster"
            })
        except AssertionError as e:
            self.fail(f"Assertion Error Raises with {e}")
        try:
            _type_confirm_transform({
                "DataCenter": "LocalHDF5"
            })
        except AssertionError as e:
            self.fail(f"Assertion Error Raises with {e}")

    @mock.patch(mock_parse_date_path)
    @mock.patch(mock_read_local_csv_symbol_path)
    def test_symbol_type_correct(self, mock_read_csv, mock_parse_date):
        try:
            # for one symbol
            _type_confirm_transform({
                "Symbol": "123456"
            })
        except AssertionError as e:
            self.fail(f"Assertion Error Raises with {e}")
        try:
            # for a list of symbols
            _type_confirm_transform({
                "Symbol": ["123456", "234567"]
            })
        except AssertionError as e:
            self.fail(f"Assertion Error Raises with {e}")
        try:
            # for a local csv contains symbols
            _type_confirm_transform({
                "Symbol": "local.csv"
            })
        except AssertionError as e:
            self.fail(f"Assertion Error Raises with {e}")

    @mock.patch(mock_parse_date_path)
    @mock.patch(mock_read_local_csv_symbol_path)
    def test_symbol_type_incorrect(self, mock_read_csv, mock_parse_date):
        self.assertRaises(AssertionError,
                          _type_confirm_transform,
                          {"Symbol": 1})
        self.assertRaises(AssertionError,
                          _type_confirm_transform,
                          {"Symbol": [1, 2]})

    @mock.patch(mock_parse_date_path)
    @mock.patch(mock_read_local_csv_symbol_path)
    def test_fields_type_correct(self, mock_read_csv, mock_parse_date):
        try:
            # for one symbol
            _type_confirm_transform({
                "Fields": "123456"
            })
        except AssertionError as e:
            self.fail(f"Assertion Error Raises with {e}")
        try:
            _type_confirm_transform({
                "Fields": ["123456", "234567"]
            })
        except AssertionError as e:
            self.fail(f"Assertion Error Raises with {e}")

    @mock.patch(mock_parse_date_path)
    @mock.patch(mock_read_local_csv_symbol_path)
    def test_fields_type_incorrect(self, mock_read_csv, mock_parse_date):
        self.assertRaises(AssertionError,
                          _type_confirm_transform,
                          {"Fields": 1})
        self.assertRaises(AssertionError,
                          _type_confirm_transform,
                          {"Fields": [1, 2]})

    @mock.patch(mock_parse_date_path)
    @mock.patch(mock_read_local_csv_symbol_path)
    def test_start_date_type_correct(self, mock_read_csv, mock_parse_date):
        try:
            _type_confirm_transform({
                "StartDate": {}
            })
        except AssertionError as e:
            self.fail(f"Assertion Error Raises with {e}")

    @mock.patch(mock_parse_date_path)
    @mock.patch(mock_read_local_csv_symbol_path)
    def test_start_date_type_incorrect(self, mock_read_csv, mock_parse_date):
        self.assertRaises(AssertionError,
                          _type_confirm_transform,
                          {"StartDate": 1})

    @mock.patch(mock_parse_date_path)
    @mock.patch(mock_read_local_csv_symbol_path)
    def test_end_date_type_correct(self, mock_read_csv, mock_parse_date):
        try:
            _type_confirm_transform({
                "EndDate": {}
            })
        except AssertionError as e:
            self.fail(f"Assertion Error Raises with {e}")

    @mock.patch(mock_parse_date_path)
    @mock.patch(mock_read_local_csv_symbol_path)
    def test_end_date_type_incorrect(self, mock_read_csv, mock_parse_date):
        self.assertRaises(AssertionError,
                          _type_confirm_transform,
                          {"EndDate": 1})

    @mock.patch(mock_parse_date_path)
    @mock.patch(mock_read_local_csv_symbol_path)
    def test_frequency_type_correct(self, mock_read_csv, mock_parse_date):
        try:
            _type_confirm_transform({
                "Frequency": "DAILY"
            })
        except AssertionError as e:
            self.fail(f"Assertion Error Raises with {e}")

    @mock.patch(mock_parse_date_path)
    @mock.patch(mock_read_local_csv_symbol_path)
    def test_frequency_type_incorrect(self, mock_read_csv, mock_parse_date):
        self.assertRaises(AssertionError,
                          _type_confirm_transform,
                          {"Frequency": []})


class ParseDateTestCase(TestCase):
    def test_must_terms(self):
        self.assertRaises(AssertionError,
                          _parse_start_end_date,
                          {"Year": 2019, "Month": 12})
        self.assertRaises(AssertionError,
                          _parse_start_end_date,
                          {"Year": 2019, "Day": 1})
        self.assertRaises(AssertionError,
                          _parse_start_end_date,
                          {"Month": 12, "Day": 1})

    def test_hour_minute_second(self):
        ts = _parse_start_end_date({"Year": 2019,
                                    "Month": 12,
                                    "Day": 1})
        assert type(ts) == int and ts == 1575158400
        ts = _parse_start_end_date({"Year": 2019,
                                    "Month": 12,
                                    "Day": 1,
                                    "Hour": 5})
        assert type(ts) == int and ts == 1575176400
        ts = _parse_start_end_date({"Year": 2019,
                                    "Month": 12,
                                    "Day": 1,
                                    "Hour": 5,
                                    "Minute": 12})
        assert type(ts) == int and ts == 1575177120
        ts = _parse_start_end_date({"Year": 2019,
                                    "Month": 12,
                                    "Day": 1,
                                    "Hour": 5,
                                    "Minute": 12,
                                    "Second": 39})
        assert type(ts) == int and ts == 1575177159

    def test_missing_hour_and_minute(self):
        self.assertRaises(AssertionError,
                          _parse_start_end_date,
                          {"Year": 2019,
                           "Month": 12,
                           "Day": 1,
                           "Minute": 12})
        self.assertRaises(AssertionError,
                          _parse_start_end_date,
                          {"Year": 2019,
                           "Month": 12,
                           "Day": 1,
                           "Second": 39})


class ParseConfigTestCase(TestCase):
    def test_output(self):
        output = parse_config(sample_config_dict)
        assert "SymbolArgs" in output[0].keys()
