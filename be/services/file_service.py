# services/file_service.py
from services.storage.local_storage import LocalStorage
from services.storage.s3_storage import S3Storage
from config import Config
from common.db import db
from models.file import File

# 根据配置选择存储后端
if getattr(Config, "STORAGE_BACKEND", "local") == "s3":
    storage = S3Storage(bucket_name=Config.S3_BUCKET)
else:
    storage = LocalStorage()

class FileService:
    @staticmethod
    def upload(user_id, file_obj, folder=''):
        result = storage.upload_file(user_id, file_obj, folder)
        # result may be path string (legacy) or dict with md5/path
        if isinstance(result, dict):
            file_path = result.get("path")
            md5_hex = result.get("md5")
        else:
            file_path = result
            md5_hex = None
        print(f"[上传] {file_obj.filename} -> {file_path}")

        # 写入文件元数据（若相同 user+folder+filename 已存在则更新 hash/更新时间）
        try:
            record = File.query.filter_by(user_id=user_id, folder=folder, filename=file_obj.filename).first()
            if record is None:
                record = File(user_id=user_id, folder=folder, filename=file_obj.filename, hash=md5_hex or '')
                db.session.add(record)
            else:
                if md5_hex:
                    record.hash = md5_hex
            db.session.commit()
        except Exception:
            db.session.rollback()
        return {"filename": file_obj.filename, "status": "上传成功", "md5": md5_hex}

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