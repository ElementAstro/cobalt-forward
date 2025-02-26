import contextlib
import resource
import os
from typing import Set, Generator, Dict, List, Optional
from functools import lru_cache
import platform
import threading
import time


class PluginSandbox:
    """插件沙箱环境，提供资源限制和访问控制"""

    def __init__(self):
        self.memory_limit = 1024 * 1024 * 100  # 100MB
        self.cpu_time_limit = 30  # 30秒
        self.file_access_paths: Set[str] = set()
        self.allowed_modules: Set[str] = set()
        self._resource_limits: Dict[int, tuple] = {}
        self._lock = threading.RLock()
        self._last_access_time = time.time()
        self._is_windows = platform.system() == 'Windows'

    @contextlib.contextmanager
    def apply(self) -> Generator[None, None, None]:
        """应用沙箱限制，Windows和Unix系统兼容"""
        with self._lock:
            self._last_access_time = time.time()
            
            # 在Windows系统上跳过资源限制设置
            if self._is_windows:
                yield
                return
                
            # 保存当前限制
            for resource_type in [resource.RLIMIT_AS, resource.RLIMIT_CPU]:
                self._resource_limits[resource_type] = resource.getrlimit(
                    resource_type)

            try:
                # 设置资源限制
                resource.setrlimit(resource.RLIMIT_AS,
                                (self.memory_limit, self.memory_limit))
                resource.setrlimit(resource.RLIMIT_CPU,
                                (self.cpu_time_limit, self.cpu_time_limit))
                yield
            finally:
                # 恢复原始限制
                for resource_type, limits in self._resource_limits.items():
                    resource.setrlimit(resource_type, limits)

    @lru_cache(maxsize=1024)
    def validate_module_access(self, module_name: str) -> bool:
        """验证模块访问权限(使用缓存)"""
        # 允许访问内置模块
        if module_name in sys.builtin_module_names:
            return True
        return module_name in self.allowed_modules

    @lru_cache(maxsize=1024)
    def validate_file_access(self, file_path: str) -> bool:
        """验证文件访问权限(使用缓存)"""
        if not file_path:
            return False
            
        normalized_path = os.path.normpath(os.path.abspath(file_path))
        # 优化文件路径匹配性能
        return any(normalized_path.startswith(os.path.normpath(os.path.abspath(path)))
                  for path in self.file_access_paths)
                   
    def reset_cache(self):
        """重置缓存，当配置变更时调用"""
        self.validate_module_access.cache_clear()
        self.validate_file_access.cache_clear()
        
    def get_status(self) -> Dict:
        """获取沙箱状态"""
        return {
            "memory_limit_mb": self.memory_limit / (1024 * 1024),
            "cpu_time_limit_sec": self.cpu_time_limit,
            "allowed_modules_count": len(self.allowed_modules),
            "file_access_paths_count": len(self.file_access_paths),
            "last_access_time": self._last_access_time
        }


# 确保跨平台兼容性
import sys
if 'resource' not in sys.modules and platform.system() == 'Windows':
    class DummyResource:
        """Windows系统的资源限制兼容层"""
        RLIMIT_AS = 0
        RLIMIT_CPU = 1
        
        @staticmethod
        def getrlimit(resource_type):
            return (0, 0)
            
        @staticmethod
        def setrlimit(resource_type, limits):
            pass
    
    # 如果是Windows系统则使用模拟实现
    if 'resource' not in globals():
        resource = DummyResource()
