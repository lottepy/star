import hashlib
import os

class md5_verification(object):
    def __init__(self):
        pass
    def create_md5_code_folder(self,source_folder_filepath):
        if not os.path.exists(source_folder_filepath):
            raise Exception("filepath is not exist")
        else:
            file_list = os.listdir(source_folder_filepath)
            file_list = [x for x in file_list if x.split('.')[-1]!= 'md5']
            for file in file_list:
                with open(os.path.join(source_folder_filepath,file)) as f:
                    data = ''.join(f.readlines())
                f = open(os.path.join(source_folder_filepath,file.split('.')[0]+'.md5'), 'w')
                f.write(hashlib.md5(bytes(data,"ascii")).hexdigest())
                f.close()

    def verify_md5_folder(self,source_folder_filepath):
        if not os.path.exists(source_folder_filepath):
            raise Exception("filepath is not exist")
        else:
            #获取路径下的csv文件
            file_list = os.listdir(source_folder_filepath)
            file_list = [x for x in file_list if x.split('.')[-1]!= 'md5']
            for file in file_list:
                with open(os.path.join(source_folder_filepath, file)) as f:
                    data = ''.join(f.readlines())
                source_md5_code = hashlib.md5(bytes(data,"ascii")).hexdigest()

                if not os.path.exists(os.path.join(source_folder_filepath, file.split('.')[0] + '.md5')):
                    raise Exception("{} 's md5 code is not exist".format(file))
                else:
                    f = open(os.path.join(source_folder_filepath,file.split('.')[0]+'.md5'))
                    reader = f.read()
                    s = [i for i in reader]
                    md5_code = ''.join(s)
                    if source_md5_code == md5_code:
                        print('{} \'s verification completed'.format(file))
                    else:
                        raise Exception('{} md5 code is not same'.format(file))


    def create_md5_code_file(self,source_filepath):
        if not os.path.exists(source_filepath):
            raise Exception("file is not exist")
        else:
            with open(os.path.join(source_filepath)) as f:
                data = ''.join(f.readlines())
            f = open(os.path.join(source_filepath.split('.')[0]+'.md5'), 'w')
            f.write(hashlib.md5(bytes(data,"ascii")).hexdigest())
            f.close()

    def verify_md5_file(self,source_filepath):
        if not os.path.exists(source_filepath):
            raise Exception("filepath is not exist")
        else:
            with open(source_filepath) as f:
                data = ''.join(f.readlines())
            source_md5_code = hashlib.md5(bytes(data,"ascii")).hexdigest()

            if not os.path.exists(os.path.join(source_filepath.split('.')[0]+'.md5')):
                raise Exception("{} 's md5 code is not exist".format(os.path.join(source_filepath.split('.')[0]+'.md5')))
            else:
                f = open(os.path.join(source_filepath.split('.')[0]+'.md5'))
                reader = f.read()
                s = [i for i in reader]
                md5_code = ''.join(s)
                if source_md5_code == md5_code:
                    print('{} \'s verification completed'.format(source_filepath))
                else:
                    raise Exception('{} md5 code is not same'.format(source_filepath))





if __name__ == '__main__':
    a = md5_verification()
    # a.create_md5_code_folder('test')
    # a.verify_md5_folder('test')

    #a.create_md5_code_file('spot_bgnl.csv')
    a.verify_md5_file('spot_bgnl.csv')
