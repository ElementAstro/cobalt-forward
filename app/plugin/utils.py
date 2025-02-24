import hashlib
from typing import Dict, Any
import yaml
import importlib.util
import sys


def get_file_hash(file_path: str) -> str:
    """获取文件哈希值"""
    with open(file_path, 'rb') as f:
        return hashlib.md5(f.read()).hexdigest()


def load_yaml_config(file_path: str) -> Dict[str, Any]:
    """加载YAML配置文件"""
    with open(file_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def safe_import_module(module_name: str, module_path: str):
    """安全导入模块"""
    try:
        spec = importlib.util.spec_from_file_location(module_name, module_path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        return module
    except Exception as e:
        raise ImportError(f"Failed to import {module_name}: {e}")
