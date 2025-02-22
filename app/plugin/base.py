from abc import ABC, abstractmethod
from typing import Any, Dict, List, Type, Optional, Callable, Set
import asyncio
from concurrent.futures import ThreadPoolExecutor
import threading
from .models import PluginMetadata, PluginState, PluginMetrics
from .sandbox import PluginSandbox
from .function import PluginFunction

class Plugin(ABC):
    def __init__(self):
        self.metadata: PluginMetadata = None
        self.state: str = PluginState.UNLOADED
        self.config: Dict[str, Any] = {}
        self.start_time: float = None
        self._hooks: Dict[str, List[Callable]] = {}
        self._event_handlers: Dict[str, List[Callable]] = {}
        self.health_status: Dict[str, Any] = {"status": "unknown"}
        self.last_health_check: float = None
        self._lifecycle_hooks = {
            "pre_initialize": [],
            "post_initialize": [],
            "pre_shutdown": [],
            "post_shutdown": []
        }
        self._functions: Dict[str, PluginFunction] = {}
        self._exported_types: Dict[str, Type] = {}
        self._permissions: Set[str] = set()
        self._sandbox = PluginSandbox()
        self._metrics = PluginMetrics()
        self._message_queue: asyncio.Queue = asyncio.Queue()
        self._thread_pool = ThreadPoolExecutor(max_workers=3)
        self._local_storage = threading.local()

    @abstractmethod
    async def initialize(self) -> None:
        """插件初始化"""
        pass

    @abstractmethod
    async def shutdown(self) -> None:
        """插件关闭"""
        pass

    # ...existing methods from Plugin class...
