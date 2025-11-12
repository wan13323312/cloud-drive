import hashlib


def md5_bytes(data: bytes) -> str:
    h = hashlib.md5()
    h.update(data)
    return h.hexdigest()


def md5_fileobj(file_obj) -> str:
    """Compute MD5 without leaving file pointer at end."""
    pos = file_obj.tell()
    file_obj.seek(0)
    h = hashlib.md5()
    for chunk in iter(lambda: file_obj.read(8192), b""):
        h.update(chunk)
    digest = h.hexdigest()
    file_obj.seek(pos)
    return digest


