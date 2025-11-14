import os
from typing import Optional
from services.dedup.chunk_store import DatabaseChunkStore


class Md5Store:
    """
    File-level deduplication store based on MD5.
    现在使用块级去重作为底层存储，保持向后兼容的API
    - Stores compressed blob by md5 under a shared store directory
    - Maintains database-based reference counts
    - Exposes pointer encode/decode utilities
    """
    POINTER_PREFIX = b"REF:"

    def __init__(self, uploads_root: str = "./uploads"):
        self.uploads_root = uploads_root
        # 使用DatabaseChunkStore作为底层存储
        self.chunk_store = DatabaseChunkStore(uploads_root)

    # -------- pointer helpers --------
    def is_pointer(self, content: bytes) -> bool:
        return isinstance(content, (bytes, bytearray)) and content.startswith(self.POINTER_PREFIX)

    def make_pointer(self, md5_hex: str) -> bytes:
        return self.POINTER_PREFIX + md5_hex.encode("utf-8")

    def parse_pointer(self, content: bytes) -> str:
        return content.decode("utf-8").split(":", 1)[1]


    # -------- refcount ops (delegated to chunk store) --------
    def _read_ref(self, file_hash: str) -> int:
        """读取引用计数（兼容性接口）"""
        # 在块存储中，这个概念有所不同，返回文件是否存在
        return 1 if self.chunk_store.file_exists(file_hash) else 0

    def inc_ref(self, file_hash: str, blob_size: int = None) -> int:
        """增加引用计数（兼容性接口）"""
        return self.chunk_store.inc_ref(file_hash, blob_size)

    def dec_ref(self, file_hash: str) -> int:
        """减少引用计数（兼容性接口）"""
        return self.chunk_store.dec_ref(file_hash)

    def exists_ref(self, file_hash: str) -> bool:
        """检查文件引用是否存在（兼容性接口）"""
        return self.chunk_store.exists_ref(file_hash)

    # -------- blob ops --------
    def ensure_blob(self, data: bytes) -> str:
        """Ensure blob exists for data; returns file hash."""
        # 使用块存储系统存储数据
        return self.chunk_store.ensure_blob(data)

    def read_blob(self, file_hash: str) -> Optional[bytes]:
        """读取文件数据"""
        return self.chunk_store.read_blob(file_hash)

    # -------- additional utility methods --------
    def get_storage_stats(self):
        """获取存储统计信息"""
        return self.chunk_store.get_storage_stats()

    def cleanup_orphaned_blobs(self):
        """清理孤立的blob文件（数据库中没有引用记录的文件）"""
        return self.chunk_store.cleanup_orphaned_chunks()


