import os
import hashlib
from typing import List, Optional


def get_file_hash(filepath: str) -> str:
    """
    获取文件的MD5哈希值

    Args:
        filepath: 文件路径

    Returns:
        str: 文件的MD5哈希值
    """
    hash_md5 = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def get_local_files(directory: str, exclude: Optional[List[str]] = None) -> List[str]:
    """
    递归获取本地目录中的所有文件路径

    Args:
        directory: 本地目录路径
        exclude: 要排除的文件/目录模式列表

    Returns:
        List[str]: 文件路径列表
    """
    exclude = exclude or []
    result = []

    for root, dirs, files in os.walk(directory):
        # 排除不需要遍历的目录
        dirs[:] = [d for d in dirs if not any(
            os.path.join(root, d).startswith(os.path.join(directory, ex)) for ex in exclude)]

        for file in files:
            file_path = os.path.join(root, file)
            # 检查文件是否应该被排除
            if not any(file_path.startswith(os.path.join(directory, ex)) for ex in exclude):
                result.append(file_path)

    return result


def is_path_excluded(path: str, base_path: str, exclude_patterns: List[str]) -> bool:
    """
    检查路径是否应该被排除

    Args:
        path: 要检查的路径
        base_path: 基础路径
        exclude_patterns: 排除模式列表

    Returns:
        bool: 如果路径应该被排除则返回True，否则False
    """
    rel_path = os.path.relpath(path, base_path)
    return any(rel_path.startswith(pattern) for pattern in exclude_patterns)
