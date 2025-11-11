import boto3
from botocore.exceptions import ClientError


class S3Storage:
    def __init__(self, bucket_name):
        self.s3 = boto3.client('s3')
        self.bucket = bucket_name

    def upload_file(self, user_id, file_obj, folder=''):
        # 入库压缩（封装在工具层，可通过配置开关控制）
        from config import Config
        from utils.compress import compress_for_storage
        raw = file_obj.read()
        compressed = compress_for_storage(raw, enabled=getattr(Config, "ENABLE_COMPRESSION", True))
        key = f"{user_id}/{folder}/{file_obj.filename}" if folder else f"{user_id}/{file_obj.filename}"
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=compressed)
        return key

    def download_file(self, user_id, filename, folder=''):
        # 出库解压（封装在工具层）
        from config import Config
        from utils.compress import decompress_from_storage
        key = f"{user_id}/{folder}/{filename}" if folder else f"{user_id}/{filename}"
        obj = self.s3.get_object(Bucket=self.bucket, Key=key)
        blob = obj['Body'].read()
        return decompress_from_storage(blob, enabled=getattr(Config, "ENABLE_COMPRESSION", True))

    def list_files(self, user_id, folder=''):
        prefix = f"{user_id}/{folder}/" if folder else f"{user_id}/"
        resp = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
        files = []
        for obj in resp.get('Contents', []):
            key = obj['Key']
            # 去掉前缀，只返回文件名或相对路径
            name = key[len(prefix):].rstrip('/')
            if name:
                files.append(name)
        return files

    def delete_file(self, user_id, filename, folder=''):
        key = f"{user_id}/{folder}/{filename}" if folder else f"{user_id}/{filename}"
        try:
            self.s3.delete_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError:
            return False

    def create_folder(self, user_id, foldername):
        # S3不需要真正创建文件夹，只需创建空对象或直接使用路径
        key = f"{user_id}/{foldername}/" if foldername else f"{user_id}/"
        self.s3.put_object(Bucket=self.bucket, Key=key)
        return True

    def rename_file(self, user_id, old_path, new_path):
        """S3 无原子 rename，用 copy -> delete 实现"""
        old_key = f"{user_id}/{old_path}"
        new_key = f"{user_id}/{new_path}"
        try:
            self.s3.copy_object(
                Bucket=self.bucket,
                CopySource={'Bucket': self.bucket, 'Key': old_key},
                Key=new_key
            )
            self.s3.delete_object(Bucket=self.bucket, Key=old_key)
            return True
        except ClientError:
            return False

    def create_archive(self, user_id, folder, archive_name):
        # 直接在对象存储端打包较复杂；此处返回 None 表示未实现
        return None

    def extract_archive(self, user_id, archive_path, dest_folder):
        # 未实现：需要拉到本地或用 Lambda 处理
        return False
 