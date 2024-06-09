import abc
import warnings

import oss2
import six

ACCESS_KEY_ID = 'LTAIdXteU42OrXqy'
ACCESS_KEY_SECRET = 'Cggf8jCyJ8KTAKaizDQGJOcFZDGojM'
ALGO_EP = 'https://oss-cn-hongkong.aliyuncs.com'
ALGO_BUCKET = 'aqm-algo'



@six.add_metaclass(abc.ABCMeta)
class OSS_Client(object):

    def __init__(self, AccessKeyId=ACCESS_KEY_ID, AccessKeySecret=ACCESS_KEY_SECRET,
                 endpoint=ALGO_EP, bucket_name=ALGO_BUCKET):

        self.bucket = oss2.Bucket(oss2.Auth(AccessKeyId, AccessKeySecret), endpoint, bucket_name)

    def upload_file(self, local_path, oss_path='Strategy_Kit/temp.csv'):
        self.bucket.put_object_from_file(oss_path, local_path)

    def download_file(self, local_path, oss_path='Strategy_Kit/temp.csv'):
        """ 下载文件到本地，若失败将抛出异常 """
        self.bucket.get_object_to_file(key=oss_path, filename=local_path)

    def download_file_to_stream(self, oss_path) -> str:
        """ 下载文件为字符串，若失败将抛出异常 """
        obj = self.bucket.get_object(oss_path)
        return obj.read()

    def exists(self, oss_path) -> bool:
        return self.bucket.object_exists(oss_path)

    def list_files(self, oss_path='', object_details=False) -> list:
        """ 列出给定目录（应以'/'结尾）下的所有文件的相对路径，不包括文件夹，根目录请输入空字符串

            若object_details=True，则返回所有文件的对象的列表
        """
        if (len(oss_path) > 1) and (not oss_path.endswith('/')):
            warnings.warn(f'Directory should end with "/", you input {oss_path}, the result may be wrong', RuntimeWarning)

        results = self.bucket.list_objects(prefix=oss_path, delimiter='/').object_list
        if object_details:
            return results
        else:
            return [obj.key for obj in results]

    def list_directories(self, oss_path='') -> list:
        """ 列出目录下的所有文件夹的相对路径，以'/'结尾。根目录请输入空字符串 """
        if (len(oss_path) > 1) and (not oss_path.endswith('/')):
            warnings.warn(f'Directory should end with "/", you input {oss_path}, the result may be wrong', RuntimeWarning)

        dirs = self.bucket.list_objects(prefix=oss_path, delimiter='/').prefix_list
        return dirs

    def delete_file(self, oss_path):
        self.bucket.delete_object(oss_path)

    def copy_file(self, src_oss_path, dst_oss_path):
        self.bucket.copy_object(ALGO_BUCKET, src_oss_path, dst_oss_path)

oss_client = OSS_Client()
# if __name__ == "__main__":
#     oss_client = OSS_Client()
