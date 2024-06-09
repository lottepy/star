import collections.abc
import numpy as np

def update_dict(old, new):
    '''
        https://stackoverflow.com/questions/3232943/update-value-of-a-nested-dictionary-of-varying-depth
    '''
    for k, v in new.items():
        if isinstance(v, collections.abc.Mapping):
            old[k] = update_dict(old.get(k, {}), v)
        else:
            old[k] = v
    return old

def deep_compare_dict(d1: dict, d2: dict) -> bool:
    '''
        Assumptions: 
            data structure: {str: str/list[str]/dict({str: list[str], str})}
            only list, dict, tuple are possible value types
            ignore orders or duplicates 
            ['bypass_arr2raw'] != 'bypass_arr2raw' 
        Deep compare two dict, ONLY return True if all keys and values are EXACTLY the same
    '''
    res = True
    if set(d1.keys()) != set(d2.keys()):
        return False
    for k in d1:
        if type(d1[k]) == type(d2[k]):
            if isinstance(d1[k], list):
                res = set(d1[k]) == set(d2[k])
            elif isinstance(d1[k], dict):
                res = deep_compare_dict(d1[k], d2[k])
            elif isinstance(d1[k], str) or isinstance(d1[k],int) or isinstance(d1[k],float):
                res = d1[k] == d2[k]
            elif isinstance(d1[k], np.ndarray):
                res = set(d1[k]) == set(d2[k])
            else:
                raise ValueError(f'deep_compare_dict: Unsupported data type inside dictionary: {type(d1[k])}')
        else:
            return False
        if not res:
            return False
    return True