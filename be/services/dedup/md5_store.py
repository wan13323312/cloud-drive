import os
from typing import Optional
from utils.hash import md5_bytes
from utils.compress import compress_for_storage, decompress_from_storage
from config import Config


class Md5Store:
    """
    File-level deduplication store based on MD5.
    - Stores compressed blob by md5 under a shared store directory
    - Maintains simple file-based reference counts
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

    def _ref_path(self, md5_hex: str) -> str:
        return os.path.join(self.store_dir, f"{md5_hex}.ref")

    # -------- refcount ops --------
    def _read_ref(self, md5_hex: str) -> int:
        ref_file = self._ref_path(md5_hex)
        if not os.path.exists(ref_file):
            return 0
        try:
            with open(ref_file, "r") as f:
                return int(f.read().strip() or "0")
        except Exception:
            return 0

    def inc_ref(self, md5_hex: str) -> None:
        cnt = self._read_ref(md5_hex) + 1
        with open(self._ref_path(md5_hex), "w") as f:
            f.write(str(cnt))

    def dec_ref(self, md5_hex: str) -> None:
        cnt = max(0, self._read_ref(md5_hex) - 1)
        ref_file = self._ref_path(md5_hex)
        if cnt == 0:
            # remove blob and ref
            try:
                os.remove(self._blob_path(md5_hex))
            except FileNotFoundError:
                pass
            try:
                os.remove(ref_file)
            except FileNotFoundError:
                pass
        else:
            with open(ref_file, "w") as f:
                f.write(str(cnt))

    # -------- blob ops --------
    def ensure_blob(self, data: bytes) -> str:
        """Ensure blob exists for data; returns md5 hex."""
        md5_hex = md5_bytes(data)
        blob_path = self._blob_path(md5_hex)
        if not os.path.exists(blob_path):
            compressed = compress_for_storage(data, enabled=getattr(Config, "ENABLE_COMPRESSION", True))
            with open(blob_path, "wb") as bf:
                bf.write(compressed)
        return md5_hex

    def read_blob(self, md5_hex: str) -> Optional[bytes]:
        path = self._blob_path(md5_hex)
        if not os.path.isfile(path):
            return None
        with open(path, "rb") as f:
            blob = f.read()
        return decompress_from_storage(blob, enabled=getattr(Config, "ENABLE_COMPRESSION", True))


