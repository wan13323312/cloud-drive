# client/api/file_api.py
import os
from client.api.base import BaseAPI

class FileAPI(BaseAPI):
    def upload(self, filepath, folder=""):
        filename = os.path.basename(filepath)
        with open(filepath, "rb") as f:
            files = {"file": (filename, f), "folder": (None, folder)}
            return self.request("POST", "/file/upload", files=files)

    def list(self, folder=""):
        return self.request("GET", "/file/list", params={"folder": folder} if folder else None)

    def download(self, filename, save_path, folder=""):
        params = {"filename": filename}
        if folder:
            params["folder"] = folder
        res = self.session.get(f"{self.base_url}/file/download", params=params, stream=True)
        if res.status_code != 200:
            raise RuntimeError(f"Download failed: {res.text}")
        with open(save_path, "wb") as f:
            f.write(res.content)
        return save_path

    def delete(self, filename, folder=""):
        payload = {"filename": filename}
        if folder:
            payload["folder"] = folder
        return self.request("POST", "/file/delete", json=payload)

    def create_folder(self, foldername):
        return self.request("POST", "/file/create_folder", json={"foldername": foldername})

    def rename(self, old_path, new_path):
        """old_path/new_path: 相对云端用户根目录的路径，如 'docs/a.txt' -> 'docs/b.txt'"""
        return self.request("POST", "/file/rename", json={"old_path": old_path, "new_path": new_path})

    # ---------- 压缩/解压 ----------
    def create_archive(self, folder="", archive_name="archive.zip"):
        return self.request("POST", "/file/archive/create", json={"folder": folder, "archive_name": archive_name})

    def extract_archive(self, archive_path, dest_folder=""):
        return self.request("POST", "/file/archive/extract", json={"archive_path": archive_path, "dest_folder": dest_folder})
