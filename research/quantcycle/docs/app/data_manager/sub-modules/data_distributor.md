# Data Manager (DATA DISTRIBUTOR)

## Introduction

This module is under the DataManager module. 

This module aims to convert the 3D numpy array into Raw Array or vice versa.

## Module Structure

The module has two major files, `data_distributor_main.py` and `data_distributor_sub.py`.

### data_distributor_main.py 
consists of methods for data packaging.

- `pack_data`: To pack the numpy array into Raw Array
    - from_pkl: if true, get data from pickle
    - to_pkl: if true, backup data save to pickle for later 

Notes: 
- If `to_pkl` is flase and `from_pkl` is true, DM will skip the whole download process and load local pickle data ONLY.

- input format:
    ```{python}
        '''
        To pack whole dict of numpy array into dict of raw array

        Before pack from DataProcessor:
        np array needed to be converted into RawArray
            {
                data_bundle["Label"]: 
                    {
                        'data_arr':...,
                        'time_arr':..., 
                        'symbol_arr':...,
                        'fields_arr':...
                    },
                data_bundle["Label"]: 
                    {
                        'data_arr':...,
                        'time_arr':..., 
                        'symbol_arr':...,
                        'fields_arr':...
                    },
                ...
            }
        others data to be saved directly into the data_package
            {
                data_bundle["Label"]: value 1
                data_bundle["Label"]: value 2
            }        
        '''
    ```

- output format:
    ```{python}

        After packing:
            data_package: dict
            key == 'raw': dict
                {data_bundle["Label"]: tuple1,
                 data_bundle["Label"]: tuple2,
                 data_bundle["Label"]: tuple3 ....}
            key == 'others': dict
                {data_bundle["Label"]: value1,
                 data_bundle["Label"]: value2,
                 data_bundle["Label"]: value3 ....}

            tuple format:
                index 0: Raw array of data array body
                index 1: shape of data array body
                index 2: Raw array of time array body
                index 3: shape of time array body
                index 4: list of symbol names
                index 5: list of fields names
    ```    

### data_distributor_sub.py 
consists of methods for data unpackaging.

- `unpack_data`: To unpack the Raw Array into numpy array 
