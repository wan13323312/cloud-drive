# services/storage/local_storage.py
import os
import shutil
import zipfile
from config import Config
from utils.compress import decompress_from_storage
from services.dedup.md5_store import Md5Store

UPLOAD_DIR = "./uploads"
STORE_DIR = os.path.join(UPLOAD_DIR, ".store")  # content-addressed blob store

class LocalStorage:
    def __init__(self):
        self._md5_store = Md5Store(uploads_root=UPLOAD_DIR)
    def _get_user_dir(self, user_id, folder=''):
        path = os.path.join(UPLOAD_DIR, str(user_id), folder)
        os.makedirs(path, exist_ok=True)
        return path

    def _ensure_store(self):
        os.makedirs(STORE_DIR, exist_ok=True)

    def upload_file(self, user_id, file_obj, folder=''):
        user_dir = self._get_user_dir(user_id, folder)
        file_path = os.path.join(user_dir, file_obj.filename)
        # 读原始数据，通过 Md5Store 去重
        data = file_obj.read()
        md5_hex = self._md5_store.ensure_blob(data)
        self._md5_store.inc_ref(md5_hex)

        # 在用户目录写入“指针文件”，内容为 REF:<md5>
        with open(file_path, 'wb') as pf:
            pf.write(self._md5_store.make_pointer(md5_hex))
        return {"path": file_path, "md5": md5_hex}

    def download_file(self, user_id, filename, folder=''):
        file_path = os.path.join(self._get_user_dir(user_id, folder), filename)
        with open(file_path, 'rb') as f:
            content = f.read()
        if self._md5_store.is_pointer(content):
            md5_hex = self._md5_store.parse_pointer(content)
            blob = self._md5_store.read_blob(md5_hex)
            if blob is None:
                return b""
            return blob
        else:
            # 兼容旧文件：直接按以前的方式处理
            return decompress_from_storage(content, enabled=getattr(Config, "ENABLE_COMPRESSION", True))

    def list_files(self, user_id, folder=''):
        user_dir = self._get_user_dir(user_id, folder)
        if not os.path.exists(user_dir):
            return []
        return [f for f in os.listdir(user_dir) if os.path.isfile(os.path.join(user_dir, f))]

    def delete_file(self, user_id, filename, folder=''):
        file_path = os.path.join(self._get_user_dir(user_id, folder), filename)
        if not os.path.exists(file_path):
            return False
        # 如果是指针文件，需要递减引用计数并可能清理 blob
        try:
            with open(file_path, "rb") as f:
                content = f.read()
            if self._md5_store.is_pointer(content):
                md5_hex = self._md5_store.parse_pointer(content)
                self._md5_store.dec_ref(md5_hex)
        finally:
            os.remove(file_path)
        return True

    def create_folder(self, user_id, foldername):
        self._get_user_dir(user_id, foldername)
        return True

    def rename_file(self, user_id, old_path, new_path):
        user_root = os.path.join(UPLOAD_DIR, str(user_id))
        old_abs = os.path.join(user_root, old_path)
        new_abs = os.path.join(user_root, new_path)
        new_dir = os.path.dirname(new_abs)
        os.makedirs(new_dir, exist_ok=True)
        if not os.path.exists(old_abs):
            return False
        os.replace(old_abs, new_abs)
        return True

    def create_archive(self, user_id, folder, archive_name):
        """Create zip from folder, returns relative path to the created zip."""
        user_root = os.path.join(UPLOAD_DIR, str(user_id))
        src_dir = os.path.join(user_root, folder) if folder else user_root
        if not os.path.isdir(src_dir):
            return None
        # Ensure .zip suffix
        if not archive_name.endswith(".zip"):
            archive_name = f"{archive_name}.zip"
        archive_rel = os.path.join(folder, archive_name) if folder else archive_name
        archive_abs = os.path.join(user_root, archive_rel)
        os.makedirs(os.path.dirname(archive_abs), exist_ok=True)
        # Write zip
        with zipfile.ZipFile(archive_abs, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
            for root, _, files in os.walk(src_dir):
                for name in files:
                    abs_path = os.path.join(root, name)
                    # Store path relative to folder
                    arcname = os.path.relpath(abs_path, start=src_dir)
                    zf.write(abs_path, arcname)
        return archive_rel

    def extract_archive(self, user_id, archive_path, dest_folder):
        """Extract zip into dest_folder. Paths are relative to user root."""
        user_root = os.path.join(UPLOAD_DIR, str(user_id))
        src_zip = os.path.join(user_root, archive_path)
        dest_dir = os.path.join(user_root, dest_folder) if dest_folder else user_root
        if not os.path.isfile(src_zip):
            return False
        os.makedirs(dest_dir, exist_ok=True)
        with zipfile.ZipFile(src_zip, 'r') as zf:
            zf.extractall(dest_dir)
        return True