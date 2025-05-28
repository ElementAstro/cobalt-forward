import os
import hashlib
from typing import List, Optional


def get_file_hash(filepath: str) -> str:
    """
    Calculate MD5 hash of a file

    Args:
        filepath: Path to the file

    Returns:
        str: MD5 hash of the file as hexadecimal string
    """
    hash_md5 = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def get_local_files(directory: str, exclude: Optional[List[str]] = None) -> List[str]:
    """
    Recursively get all file paths from a local directory

    Args:
        directory: Local directory path
        exclude: List of file/directory patterns to exclude

    Returns:
        List[str]: List of file paths
    """
    exclude = exclude or []
    result: List[str] = []

    for root, dirs, files in os.walk(directory):
        # Exclude directories that shouldn't be traversed
        dirs[:] = [d for d in dirs if not any(
            os.path.join(root, d).startswith(os.path.join(directory, ex)) for ex in exclude)]

        for file in files:
            file_path = os.path.join(root, file)
            # Check if file should be excluded
            if not any(file_path.startswith(os.path.join(directory, ex)) for ex in exclude):
                result.append(file_path)

    return result


def is_path_excluded(path: str, base_path: str, exclude_patterns: List[str]) -> bool:
    """
    Check if a path should be excluded based on patterns

    Args:
        path: Path to check
        base_path: Base path for relative path calculation
        exclude_patterns: List of exclusion patterns

    Returns:
        bool: True if path should be excluded, False otherwise
    """
    rel_path = os.path.relpath(path, base_path)
    return any(rel_path.startswith(pattern) for pattern in exclude_patterns)
