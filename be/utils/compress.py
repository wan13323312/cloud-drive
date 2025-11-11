import gzip

# GZIP magic header bytes
_GZIP_MAGIC = b"\x1f\x8b"


def is_gzip(data: bytes) -> bool:
    return isinstance(data, (bytes, bytearray)) and len(data) >= 2 and bytes(data[:2]) == _GZIP_MAGIC


def compress_for_storage(data: bytes, enabled: bool = True) -> bytes:
    """Compress with gzip if enabled and not already gzipped. Fail-safe to return original on error."""
    if not enabled or not data or is_gzip(data):
        return data
    try:
        return gzip.compress(data)
    except Exception:
        return data


def decompress_from_storage(blob: bytes, enabled: bool = True) -> bytes:
    """Decompress if looks like gzip and enabled, otherwise return original. Fail-safe."""
    if not enabled or not blob or not is_gzip(blob):
        return blob
    try:
        return gzip.decompress(blob)
    except Exception:
        return blob


