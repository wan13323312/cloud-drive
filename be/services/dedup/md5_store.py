import os
from typing import Optional
from utils.hash import md5_bytes
from utils.compress import compress_for_storage, decompress_from_storage
from config import Config
from models.md5_ref import Md5Ref


class Md5Store:
    """
    File-level deduplication store based on MD5.
    - Stores compressed blob by md5 under a shared store directory
    - Maintains database-based reference counts
    - Exposes pointer encode/decode utilities
    """
    POINTER_PREFIX = b"REF:"

    def __init__(self, uploads_root: str = "./uploads"):
        self.uploads_root = uploads_root
        self.store_dir = os.path.join(self.uploads_root, ".store")
        os.makedirs(self.store_dir, exist_ok=True)

    # -------- pointer helpers --------
    def is_pointer(self, content: bytes) -> bool:
        return isinstance(content, (bytes, bytearray)) and content.startswith(self.POINTER_PREFIX)

    def make_pointer(self, md5_hex: str) -> bytes:
        return self.POINTER_PREFIX + md5_hex.encode("utf-8")

    def parse_pointer(self, content: bytes) -> str:
        return content.decode("utf-8").split(":", 1)[1]

    # -------- paths --------
    def _blob_path(self, md5_hex: str) -> str:
        return os.path.join(self.store_dir, md5_hex)

    # -------- refcount ops (database-based) --------
    def _read_ref(self, md5_hex: str) -> int:
        """读取引用计数"""
        return Md5Ref.get_ref_count(md5_hex)

    def inc_ref(self, md5_hex: str, blob_size: int = None) -> int:
        """增加引用计数，返回新的计数值"""
        return Md5Ref.increment_ref(md5_hex, blob_size)

    def dec_ref(self, md5_hex: str) -> int:
        """减少引用计数，如果计数为0则删除blob文件，返回新的计数值"""
        cnt = Md5Ref.decrement_ref(md5_hex)
        if cnt == 0:
            # 引用计数为0时删除blob文件
            try:
                os.remove(self._blob_path(md5_hex))
            except FileNotFoundError:
                pass
        return cnt

    def exists_ref(self, md5_hex: str) -> bool:
        """检查MD5引用是否存在"""
        return Md5Ref.exists(md5_hex)

    # -------- blob ops --------
    def ensure_blob(self, data: bytes) -> str:
        """Ensure blob exists for data; returns md5 hex."""
        md5_hex = md5_bytes(data)
        blob_path = self._blob_path(md5_hex)
        
        # 检查blob文件是否存在，如果不存在则创建
        if not os.path.exists(blob_path):
            compressed = compress_for_storage(data, enabled=getattr(Config, "ENABLE_COMPRESSION", True))
            with open(blob_path, "wb") as bf:
                bf.write(compressed)
        
        # 增加引用计数（如果是新文件会自动创建记录）
        self.inc_ref(md5_hex, len(data))
        
        return md5_hex

    def read_blob(self, md5_hex: str) -> Optional[bytes]:
        path = self._blob_path(md5_hex)
        if not os.path.isfile(path):
            return None
        with open(path, "rb") as f:
            blob = f.read()
        return decompress_from_storage(blob, enabled=getattr(Config, "ENABLE_COMPRESSION", True))

    # -------- additional utility methods --------
    def get_storage_stats(self):
        """获取存储统计信息"""
        return Md5Ref.get_storage_stats()

    def cleanup_orphaned_blobs(self):
        """清理孤立的blob文件（数据库中没有引用记录的文件）"""
        if not os.path.exists(self.store_dir):
            return 0
        
        cleaned_count = 0
        for filename in os.listdir(self.store_dir):
            if filename.endswith('.ref'):  # 跳过旧的ref文件
                continue
            
            # 检查是否是有效的MD5文件名
            if len(filename) == 32 and all(c in '0123456789abcdef' for c in filename.lower()):
                if not Md5Ref.exists(filename):
                    # 数据库中没有引用记录，删除孤立文件
                    try:
                        os.remove(os.path.join(self.store_dir, filename))
                        cleaned_count += 1
                    except FileNotFoundError:
                        pass
        
        return cleaned_count


