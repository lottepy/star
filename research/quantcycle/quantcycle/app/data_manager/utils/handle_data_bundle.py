from ..data_loader.data_center.proxy_utils import create_proxy


def download_data_bundle(data_bundle, proxies):
    '''
        For the specific data_bundle, find the proxy for it and call proxy's on data bundle
    '''
    proxy = _find_proxy(data_bundle, proxies)
    data = proxy.on_data_bundle(data_bundle)
    return data


def classify_data_bundle(data_bundle_list):
    '''
        verify each data bundle has a Type, and classify by type
    '''
    _download = []
    _queue = []
    _process = []
    for data_bundle in data_bundle_list:
        assert "Type" in data_bundle.keys()
        tmp = data_bundle["Type"]
        if tmp == "Download":
            _download.append(data_bundle)
        elif tmp == "Queue":
            _queue.append(data_bundle)
        elif tmp == "Process":
            _process.append(data_bundle)
        else:
            raise ValueError(
                f"Wrong Type in Data Bundle: {data_bundle['Label']}")
    dm, others = [], []
    for db in _download:
        if db['DataCenter']=='DataMaster': dm.append(db)
        else: others.append(db)
    _download = dm + others
    return _download, _queue, _process


def establish_queue(data_bundle, proxies):
    raise NotImplementedError


def _find_proxy(data_bundle, proxies):
    '''
        Find proxy for data bundle,
        If not found, try create one
    '''
    data_center = data_bundle["DataCenter"]
    if data_center in proxies.keys():
        if data_center == 'ResultReader':
            # TODO this bug fix is too ugly
            if proxies[data_center].hdf5_path != data_bundle['DataCenterArgs']['DataPath']:
                new_dc = create_proxy(data_bundle)
                proxies[data_center] = new_dc
                return new_dc               
        return proxies[data_center]
    else:
        new_dc = create_proxy(data_bundle)
        proxies[data_center] = new_dc
        return new_dc
