# services/file_service.py
from services.storage.local_storage import LocalStorage
from services.storage.s3_storage import S3Storage
from config import Config

# 根据配置选择存储后端
if getattr(Config, "STORAGE_BACKEND", "local") == "s3":
    storage = S3Storage(bucket_name=Config.S3_BUCKET)
else:
    storage = LocalStorage()

class FileService:
    @staticmethod
    def upload(user_id, file_obj, folder=''):
        file_path = storage.upload_file(user_id, file_obj, folder)
        print(f"[上传] {file_obj.filename} -> {file_path}")
        return {"filename": file_obj.filename, "status": "上传成功"}

    @staticmethod
    def download(user_id, filename, folder=''):
        content = storage.download_file(user_id, filename, folder)
        return {"filename": filename, "content": content, "folder": folder}

    @staticmethod
    def list_files(user_id, folder=''):
        filenames = storage.list_files(user_id, folder)
        return [{"filename": f, "folder": folder} for f in filenames]

    @staticmethod
    def delete_file(user_id, filename, folder=''):
        return storage.delete_file(user_id, filename, folder)

    @staticmethod
    def create_folder(user_id, foldername):
        return storage.create_folder(user_id, foldername)

    @staticmethod
    def rename_file(user_id, old_path, new_path):
        ok = storage.rename_file(user_id, old_path, new_path)
        return ok

    @staticmethod
    def create_archive(user_id, folder, archive_name):
        return storage.create_archive(user_id, folder, archive_name)

    @staticmethod
    def extract_archive(user_id, archive_path, dest_folder):
        return storage.extract_archive(user_id, archive_path, dest_folder)