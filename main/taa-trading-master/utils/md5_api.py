import hashlib
import os
import oss2

# root_url = 'https://aqm-algo.oss-cn-hongkong.aliyuncs.com/0_DymonFx/Dymon_FTP/'
auth = oss2.Auth('LTAIdXteU42OrXqy', 'Cggf8jCyJ8KTAKaizDQGJOcFZDGojM')
bucket = oss2.Bucket(auth, 'http://oss-cn-hongkong.aliyuncs.com', 'aqm-algo')

class md5_verification(object):
    def __init__(self):
        pass
    def create_md5_code_file(self,source_filepath):
        # 从 0_DymonFx/Dymon_FTP/ 拉到本地
        # md5编码后放到oss，本地移除源文件与oss文件
        bucket.get_object_to_file(os.path.join('0_DymonFx/Dymon_FTP/',source_filepath), source_filepath)
        with open(source_filepath) as f:
            data = ''.join(f.readlines())
        f = open(source_filepath.split('.')[0]+'.md5', 'w')
        f.write(hashlib.md5(bytes(data,"ascii")).hexdigest())
        f.close()
        bucket.put_object_from_file(os.path.join('0_DymonFx/Dymon_FTP/', source_filepath.split('.')[0]+'.md5'),source_filepath.split('.')[0]+'.md5')
        os.remove(source_filepath)
        os.remove(source_filepath.split('.')[0]+'.md5')


    def verify_md5_file(self,source_filepath):
        # 从 0_DymonFx/Dymon_FTP/ 拉到本地 做校验
        # 本地移除源文件与oss文件
        bucket.get_object_to_file(os.path.join('0_DymonFx/Dymon_FTP/', source_filepath), source_filepath)
        bucket.get_object_to_file(os.path.join('0_DymonFx/Dymon_FTP/', source_filepath.split('.')[0]+'.md5'), source_filepath.split('.')[0]+'.md5')
        with open(source_filepath) as f:
            data = ''.join(f.readlines())
        source_md5_code = hashlib.md5(bytes(data,"ascii")).hexdigest()
        f = open(source_filepath.split('.')[0]+'.md5')
        reader = f.read()
        s = [i for i in reader]
        md5_code = ''.join(s)
        if source_md5_code == md5_code:
            print('{} \'s verification completed'.format(source_filepath))
        else:
            raise Exception('{} md5 code is not same'.format(source_filepath))
        f.close()
        os.remove(source_filepath)
        os.remove(source_filepath.split('.')[0]+'.md5')

# 重构md5,在本地创建md5码
class md5_verification_local(object):
    def __init__(self):
        pass

    def create_md5_code_file(self,source_filepath):
        # 从 0_DymonFx/Dymon_FTP/ 拉到本地
        # md5编码后放到oss，本地移除源文件与oss文件
        with open(source_filepath) as f:
            data = ''.join(f.readlines())
        f = open(source_filepath.split('.')[0]+'.md5', 'w')
        f.write(hashlib.md5(bytes(data,"ascii")).hexdigest())
        f.close()

    def verify_md5_file(self,source_filepath):
        # 从 0_DymonFx/Dymon_FTP/ 拉到本地 做校验
        # 本地移除源文件与oss文件
        with open(source_filepath) as f:
            data = ''.join(f.readlines())
        source_md5_code = hashlib.md5(bytes(data,"ascii")).hexdigest()
        f = open(source_filepath.split('.')[0]+'.md5')
        reader = f.read()
        s = [i for i in reader]
        md5_code = ''.join(s)
        if source_md5_code == md5_code:
            print('{} \'s verification completed'.format(source_filepath))
        else:
            raise Exception('{} md5 code is not same'.format(source_filepath))
        f.close()

if __name__ == '__main__':
    from lib.commonalgo.data.oss_client import oss_client
    import time

    # retry_count = 0
    # while (True):
    #     retry_count += 1
    #     if retry_count > 10:
    #         print('error!')
    #     if oss_client.exists(os.path.join('0_DymonFx/Dymon_FTP/', "Dymon2Aqumon_Executed_20200429.csv")) and oss_client.exists(
    #             os.path.join('0_DymonFx/Dymon_FTP/', "Dymon2Aqumon_Position_20200429.csv")):
    #         break
    #     time.sleep(1)
    #     print(f'retry {retry_count} times uploading file to oss.')
    a = md5_verification_local()
    a.create_md5_code_file("Dymon2Aqumon_Position_20200103.csv")
    a.verify_md5_file("Dymon2Aqumon_Position_20200103.csv")
    # a.create_md5_code_file('Dymon2Aqumon_Executed_20200429.csv')
    #文件必须处于路径 alioss/0_DymonFx/Dymon_FTP/中
    # a.verify_md5_file('Dymon2Aqumon_Executed_20200429.csv')
