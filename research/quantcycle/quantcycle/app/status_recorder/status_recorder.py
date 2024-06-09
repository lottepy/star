import pickle
import os


# status_dict={key(strategy id) : value(object)}

class BaseStatusRecorder:
    def __init__(self):
        self.original_path= os.path.abspath('.')
        pass

    def dump(self, status_dict: dict, status_name, path):
        name = "{}.pkl".format(status_name)
        if path == '':
            dump_dir_path = self.original_path
        else:
            dump_dir_path = os.path.join(self.original_path, path)
        dump_path = os.path.join(dump_dir_path, name)

        # if os.path.exists(dump_path):
        #     print(f"info: {dump_path}状态文件已经存在，默认新状态覆盖原文件")

        with open(dump_path, 'wb') as f:
            pickle.dump(status_dict, f, pickle.HIGHEST_PROTOCOL)
        print('info: 策略状态已上传')

    def load(self, status_name, path):
        name = "{}.pkl".format(status_name)
        if path == '':
            load_dir_path = self.original_path
        else:
            load_dir_path = os.path.join(self.original_path, path)
        load_path = os.path.join(load_dir_path, name)

        try:
            with open(load_path, 'rb') as f:
                pickle_dict = pickle.load(f)
                print(f"info: {load_path}状态文件成功读取")
                return pickle_dict
        except FileNotFoundError:
            print(f"info: {load_path}状态文件未找到，默认不载入状态")
            return None
