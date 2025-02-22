import os
import hashlib
import re
from typing import List

def get_local_files(local_dir: str, exclude: List[str] = None) -> List[str]:
    """获取本地文件列表"""
    files = []
    exclude_patterns = [re.compile(pattern) for pattern in (exclude or [])]

    for root, _, filenames in os.walk(local_dir):
        for filename in filenames:
            path = os.path.join(root, filename)
            if not any(pattern.match(path) for pattern in exclude_patterns):
                files.append(path)
    return files

def get_file_hash(path: str) -> str:
    """获取文件的MD5哈希值"""
    hasher = hashlib.md5()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b''):
            hasher.update(chunk)
    return hasher.hexdigest()
