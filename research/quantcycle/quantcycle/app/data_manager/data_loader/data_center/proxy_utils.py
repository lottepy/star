from .data_master.proxy import DataMaster
from .local_hdf5.proxy import LocalDC
from .result_reader.proxy import ResultReaderProxy
from .local_csv.proxy import LocalCSV


def create_proxy(d):
    '''
        given a dict, create proxy accordingly.
    '''
    if d["DataCenter"] == "DataMaster":
        tmp = DataMaster()
    elif d["DataCenter"] == "LocalHDF5":
        tmp = LocalDC(d)
    elif d["DataCenter"] == "LocalCSV":
        tmp = LocalCSV()
    elif d["DataCenter"] == "ResultReader":
        tmp = ResultReaderProxy(d)
    else:
        raise ValueError(f"DataCenter: {d['DataCenter']} not found!")
    return tmp
