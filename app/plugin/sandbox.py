import contextlib
import resource
import os
from typing import Set, Generator, Dict
from functools import lru_cache


class PluginSandbox:
    """插件沙箱环境"""

    def __init__(self):
        self.memory_limit = 1024 * 1024 * 100  # 100MB
        self.cpu_time_limit = 30  # 30秒
        self.file_access_paths: Set[str] = set()
        self.allowed_modules: Set[str] = set()
        self._resource_limits: Dict[int, tuple] = {}

    @contextlib.contextmanager
    def apply(self) -> Generator[None, None, None]:
        """应用沙箱限制"""
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
        return module_name in self.allowed_modules

    @lru_cache(maxsize=1024)
    def validate_file_access(self, file_path: str) -> bool:
        """验证文件访问权限(使用缓存)"""
        normalized_path = os.path.normpath(file_path)
        return any(normalized_path.startswith(os.path.normpath(path))
                   for path in self.file_access_paths)
