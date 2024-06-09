import multiprocessing
from ctypes import c_double, c_float, c_int32, c_int64, c_uint32, c_uint64

import numpy as np


def array_to_raw_array(array: np.ndarray):
    """
        从numpy array转为multiprocessing.RawArray，自动判断numpy array的dtype
    """
    TYPE_STR_TO_CTYPE = {
        'float32': c_float,
        'float64': c_double,
        'double': c_double,
        'int64': c_int64,
        'uint64': c_uint64,
        'int32': c_int32,
        'uint32': c_uint32
    }
    return multiprocessing.RawArray(TYPE_STR_TO_CTYPE[array.dtype.name], array.ravel())


def raw_array_to_array(raw_array, shape: tuple):
    """
        从multiprocessing.RawArray转为numpy array
    """
    c_type = raw_array._type_

    if c_type is c_float:
        dtype = np.float32
    elif c_type is c_double:
        dtype = np.float64
    elif c_type is c_int64:
        dtype = np.int64
    elif c_type is c_uint64:
        dtype = np.uint64
    elif c_type is c_int32:
        dtype = np.int32
    elif c_type is c_uint32:
        dtype = np.uint32
    else:
        raise ValueError(str(raw_array._type_))
    return np.frombuffer(raw_array, dtype).reshape(shape)
