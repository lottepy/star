import json


def read_json(file, encoding='utf-8'):
    return json.load(open(file, 'r', encoding=encoding))


def write_json(obj, file):
    json.dump(obj, open(file, 'w'), indent=2)
    return
