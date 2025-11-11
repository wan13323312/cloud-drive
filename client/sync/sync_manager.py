import os
from api.file_api import FileAPI

class SyncManager:
    def __init__(self, base_path, file_api: FileAPI):
        self.base_path = base_path
        self.file_api = file_api
        self.ignore_suffixes = ('.swp', '.tmp', '.temp')
        self.ignore_prefixes = ('~$',)

    def _should_ignore(self, name: str) -> bool:
        if name.startswith(self.ignore_prefixes):
            return True
        if name.endswith(self.ignore_suffixes):
            return True
        return False

    def _rel(self, path: str) -> str:
        return os.path.relpath(path, self.base_path).replace("\\", "/")

    # 本地->云端
    def upload_file(self, local_path):
        relative_path = self._rel(local_path)
        print(f"[同步] 上传文件: {relative_path}")
        self.file_api.upload(local_path, folder=os.path.dirname(relative_path))

    def delete_file(self, local_path):
        relative_path = self._rel(local_path)
        print(f"[同步] 删除文件: {relative_path}")
        self.file_api.delete(os.path.basename(local_path), folder=os.path.dirname(relative_path))

    def create_folder(self, local_path):
        relative_path = self._rel(local_path)
        print(f"[同步] 创建文件夹: {relative_path}")
        self.file_api.create_folder(relative_path)

    def rename_file(self, old_path, new_path):
        old_rel = self._rel(old_path)
        new_rel = self._rel(new_path)
        print(f"[同步] 重命名: {old_rel} -> {new_rel}")
        # 优先使用云端 rename API；若失败可回退为删除旧+上传新（可选）
        try:
            self.file_api.rename(old_rel.replace("\\", "/"), new_rel.replace("\\", "/"))
        except Exception as _:
            # 兜底方案（可选）：删除旧文件并上传新文件
            try:
                self.file_api.delete(os.path.basename(old_rel), folder=os.path.dirname(old_rel))
            except Exception:
                pass
            if os.path.isfile(new_path):
                self.file_api.upload(new_path, folder=os.path.dirname(new_rel))

    def initial_sync(self):
        """一次性把本地整个目录结构与文件上传到云端（单向：本地 -> 云端）。
        规则：
        - 忽略临时/编辑器缓存文件
        - 先创建目录，再上传文件
        """
        print(f"[同步] 初始同步开始：{self.base_path}")
        for root, dirs, files in os.walk(self.base_path):
            # 先创建目录（相对路径）
            rel_root = self._rel(root)
            if rel_root != '.':
                # '.' 表示 base_path，自身无需创建
                self.file_api.create_folder(rel_root)
            # 上传文件
            for name in files:
                if self._should_ignore(name):
                    continue
                local_path = os.path.join(root, name)
                rel_path = self._rel(local_path)
                self.file_api.upload(local_path, folder=os.path.dirname(rel_path))
        print(f"[同步] 初始同步完成")
