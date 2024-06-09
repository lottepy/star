import pickle
import os

class BaseStrategy():
    def dump(self, file_path):
        with open(file_path,'wb') as f:
            pickle.dump(self,f)
    
    def load(self, file_path):
        with open(file_path,'rb') as f:
            tmp = pickle.load(f)
        self.__dict__.update(tmp.__dict__)