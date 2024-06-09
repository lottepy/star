from metrics_calculator.utils.hdf5_connector import HDF5_Connector
import pytest
import os
import numpy as np
import h5py

def test_clean():
    filename = 'tmp/test.hdf5'
    with open(filename,'a') as f:
        pass
    assert os.path.exists(filename)
    h5 = HDF5_Connector(filename)
    h5.clean()
    assert not os.path.exists(filename)
    
def test_init_hdf5():
    filename = 'tmp/test.hdf5'
    h5 = HDF5_Connector(filename)
    h5._init_hdf5(attr1 = 1, attr2 = np.array([1,2,3]))
    
    try:
        with h5py.File(filename,'r') as f:
            assert f.attrs['attr1'] == 1
            assert np.allclose(f.attrs['attr2'],np.array([1,2,3]))
    finally:
        if os.path.exists(filename):
            os.remove(filename)

def test_connect_and_disconnect():
    filename = 'tmp/test.hdf5'
    filename2 = 'tmp/test2.hdf5'
    h5 = HDF5_Connector(filename)
    try:
        f = h5.connect()
        with h5py.File(filename2,'w') as g:
            assert isinstance(f,type(g))
    except Exception as e:
        print(e)
    finally:
        h5.disconnect()
        if os.path.exists(filename):
            os.remove(filename)
        if os.path.exists(filename2):
            os.remove(filename2)
    
def test_init_no_existing_file():
    filename = 'tmp/test.hdf5'
    if os.path.exists(filename):
        os.remove(filename)
    h5 = HDF5_Connector(filename)
    h5.init(uuid='123', attr1=1, attr2=np.array([1,2,3]))
    
    with h5py.File(filename,'r') as f:
        assert f.attrs['uuid'] == '123'
        assert f.attrs['attr1'] == 1
        assert np.allclose(f.attrs['attr2'],np.array([1,2,3]))
    os.remove(filename)
    
def test_init_with_matched_uuid():
    filename = 'tmp/test.hdf5'
    if os.path.exists(filename):
        os.remove(filename)
    with h5py.File(filename,'w') as f:
        f.attrs['uuid'] = '123'
        f.attrs['attr1'] = 1
    
    h5 = HDF5_Connector(filename)
    h5.init(uuid='123', attr2=np.array([1,2,3]))
    
    with h5py.File(filename,'r') as f:
        assert f.attrs['uuid'] == '123'
        assert f.attrs['attr1'] == 1
        assert np.allclose(f.attrs['attr2'],np.array([1,2,3]))
    os.remove(filename)
    
def test_init_with_different_uuid():
    filename = 'tmp/test.hdf5'
    if os.path.exists(filename):
        os.remove(filename)
    with h5py.File(filename,'w') as f:
        f.attrs['uuid'] = '456'
        f.attrs['attr1'] = 1
    
    h5 = HDF5_Connector(filename)
    h5.init(uuid='123', attr2=np.array([1,2,3]))
    
    with h5py.File(filename,'r') as f:
        assert f.attrs['uuid'] == '123'
        assert 'attr1' not in f.attrs.keys()
        assert np.allclose(f.attrs['attr2'],np.array([1,2,3]))
    os.remove(filename)