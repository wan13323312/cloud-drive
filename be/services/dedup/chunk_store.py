import os
import json
from dataclasses import dataclass
from typing import List, Optional, Iterable
from utils.hash import md5_bytes
from utils.compress import compress_for_storage, decompress_from_storage
from config import Config


@dataclass(frozen=True)
class ChunkRef:
    hash_md5: str
    size_bytes: int
    offset: int


@dataclass
class Manifest:
    total_size: int
    chunks: List[ChunkRef]
    compression: str = "gzip" if getattr(Config, "ENABLE_COMPRESSION", True) else "none"
    version: int = 1

    def to_json(self) -> str:
        return json.dumps({
            "version": self.version,
            "total_size": self.total_size,
            "compression": self.compression,
            "chunks": [c.__dict__ for c in self.chunks],
        })

    @staticmethod
    def from_json(data: str) -> "Manifest":
        obj = json.loads(data)
        return Manifest(
            total_size=obj["total_size"],
            compression=obj.get("compression", "none"),
            version=obj.get("version", 1),
            chunks=[ChunkRef(**c) for c in obj["chunks"]],
        )


class ChunkStore:
    """Abstract chunk store interface (scaffolding for future use)."""
    def ensure_chunk(self, data: bytes) -> str:
        raise NotImplementedError

    def read_chunk(self, hash_md5: str) -> Optional[bytes]:
        raise NotImplementedError

    def inc_ref(self, hash_md5: str) -> None:
        raise NotImplementedError

    def dec_ref(self, hash_md5: str) -> None:
        raise NotImplementedError

    def write_manifest(self, key: str, manifest: Manifest) -> None:
        raise NotImplementedError

    def read_manifest(self, key: str) -> Optional[Manifest]:
        raise NotImplementedError


class LocalChunkStore(ChunkStore):
    """
    Local filesystem chunk store.
    - Chunks under uploads/.chunks/<md5>
    - Refcounts as uploads/.chunks/<md5>.ref
    - Manifests under uploads/.manifests/<key>.json
    Note: Not yet wired into upload/download paths.
    """
    def __init__(self, uploads_root: str = "./uploads"):
        self.uploads_root = uploads_root
        self.chunks_dir = os.path.join(uploads_root, ".chunks")
        self.manifests_dir = os.path.join(uploads_root, ".manifests")
        os.makedirs(self.chunks_dir, exist_ok=True)
        os.makedirs(self.manifests_dir, exist_ok=True)

    def _chunk_path(self, md5_hex: str) -> str:
        return os.path.join(self.chunks_dir, md5_hex)

    def _ref_path(self, md5_hex: str) -> str:
        return os.path.join(self.chunks_dir, f"{md5_hex}.ref")

    def _read_ref(self, md5_hex: str) -> int:
        p = self._ref_path(md5_hex)
        if not os.path.exists(p):
            return 0
        try:
            with open(p, "r") as f:
                return int(f.read().strip() or "0")
        except Exception:
            return 0

    def ensure_chunk(self, data: bytes) -> str:
        md5_hex = md5_bytes(data)
        p = self._chunk_path(md5_hex)
        if not os.path.exists(p):
            blob = compress_for_storage(data, enabled=getattr(Config, "ENABLE_COMPRESSION", True))
            with open(p, "wb") as f:
                f.write(blob)
        return md5_hex

    def read_chunk(self, hash_md5: str) -> Optional[bytes]:
        p = self._chunk_path(hash_md5)
        if not os.path.isfile(p):
            return None
        with open(p, "rb") as f:
            blob = f.read()
        return decompress_from_storage(blob, enabled=getattr(Config, "ENABLE_COMPRESSION", True))

    def inc_ref(self, hash_md5: str) -> None:
        cnt = self._read_ref(hash_md5) + 1
        with open(self._ref_path(hash_md5), "w") as f:
            f.write(str(cnt))

    def dec_ref(self, hash_md5: str) -> None:
        cnt = max(0, self._read_ref(hash_md5) - 1)
        refp = self._ref_path(hash_md5)
        if cnt == 0:
            try:
                os.remove(self._chunk_path(hash_md5))
            except FileNotFoundError:
                pass
            try:
                os.remove(refp)
            except FileNotFoundError:
                pass
        else:
            with open(refp, "w") as f:
                f.write(str(cnt))

    def write_manifest(self, key: str, manifest: Manifest) -> None:
        mp = os.path.join(self.manifests_dir, f"{key}.json")
        os.makedirs(os.path.dirname(mp), exist_ok=True)
        with open(mp, "w", encoding="utf-8") as f:
            f.write(manifest.to_json())

    def read_manifest(self, key: str) -> Optional[Manifest]:
        mp = os.path.join(self.manifests_dir, f"{key}.json")
        if not os.path.isfile(mp):
            return None
        with open(mp, "r", encoding="utf-8") as f:
            return Manifest.from_json(f.read())


