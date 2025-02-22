import contextlib
import resource
from typing import Set, Generator

class PluginSandbox:
    """插件沙箱环境"""
    def __init__(self):
        self.memory_limit = 1024 * 1024 * 100  # 100MB
        self.cpu_time_limit = 30  # 30秒
        self.file_access_paths: Set[str] = set()
        self.allowed_modules: Set[str] = set()

    @contextlib.contextmanager
    def apply(self) -> Generator[None, None, None]:
        """应用沙箱限制"""
        old_limits = resource.getrlimit(resource.RLIMIT_AS)
        try:
            # 设置内存限制
            resource.setrlimit(resource.RLIMIT_AS, (self.memory_limit, self.memory_limit))
            yield
        finally:
            # 恢复原始限制
            resource.setrlimit(resource.RLIMIT_AS, old_limits)

    def validate_module_access(self, module_name: str) -> bool:
        """验证模块访问权限"""
        return module_name in self.allowed_modules

    def validate_file_access(self, file_path: str) -> bool:
        """验证文件访问权限"""
        return any(file_path.startswith(path) for path in self.file_access_paths)
