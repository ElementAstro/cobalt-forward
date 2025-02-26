import hashlib
from typing import Dict, Any, Optional, Tuple
import yaml
import importlib.util
import sys
import os
from functools import lru_cache


@lru_cache(maxsize=128)
def get_file_hash(file_path: str) -> str:
    """获取文件哈希值，使用LRU缓存提高性能"""
    if not os.path.exists(file_path):
        return ""
    with open(file_path, 'rb') as f:
        return hashlib.md5(f.read()).hexdigest()


def load_yaml_config(file_path: str) -> Dict[str, Any]:
    """加载YAML配置文件"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f) or {}
    except Exception as e:
        print(f"Error loading config {file_path}: {e}")
        return {}


# 用于缓存已加载的模块
_MODULE_CACHE: Dict[str, Tuple[Any, float]] = {}


def safe_import_module(module_name: str, module_path: str, force_reload: bool = False) -> Any:
    """安全导入模块，支持缓存和强制重新加载"""
    try:
        file_mtime = os.path.getmtime(module_path) if os.path.exists(module_path) else 0
        
        # 检查缓存
        if not force_reload and module_name in _MODULE_CACHE:
            cached_module, cached_mtime = _MODULE_CACHE[module_name]
            if cached_mtime >= file_mtime:
                return cached_module
        
        spec = importlib.util.spec_from_file_location(module_name, module_path)
        if spec is None:
            raise ImportError(f"Module spec not found for {module_name} at {module_path}")
            
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        
        # 更新缓存
        _MODULE_CACHE[module_name] = (module, file_mtime)
        return module
    except Exception as e:
        raise ImportError(f"Failed to import {module_name}: {e}")
