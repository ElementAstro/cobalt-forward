from functools import wraps
from typing import Any, Dict, Optional, Callable, List, Set
import asyncio
import time
import logging
from enum import Enum, auto
from dataclasses import dataclass, field
from datetime import datetime

logger = logging.getLogger(__name__)


class ServiceType(Enum):
    """共享服务类型枚举"""
    MESSAGE_BUS = auto()
    EVENT_BUS = auto()
    CONFIG_MANAGER = auto()
    COMMAND_DISPATCHER = auto()
    PLUGIN_MANAGER = auto()
    PROTOCOL_CONVERTER = auto()
    STORAGE = auto()
    SECURITY = auto()
    TRANSFORMER = auto()
    LOGGER = auto()


@dataclass
class ServiceMetrics:
    """服务指标"""
    start_time: float = field(default_factory=time.time)
    call_count: int = 0
    error_count: int = 0
    processing_times: List[float] = field(default_factory=list)
    last_call_time: Optional[float] = None
    max_history: int = 1000

    def record_call(self, processing_time: float):
        """记录调用"""
        self.call_count += 1
        self.last_call_time = time.time()
        self.processing_times.append(processing_time)
        if len(self.processing_times) > self.max_history:
            self.processing_times.pop(0)

    def record_error(self):
        """记录错误"""
        self.error_count += 1

    @property
    def avg_processing_time(self) -> float:
        """获取平均处理时间"""
        return sum(self.processing_times) / len(self.processing_times) if self.processing_times else 0.0


class ServiceNotFoundException(Exception):
    """服务未找到异常"""
    pass


class ServiceRegistrationException(Exception):
    """服务注册异常"""
    pass


class DependencyResolutionException(Exception):
    """依赖解析异常"""
    pass


class SharedServices:
    """
    共享服务注册表和依赖注入管理器

    允许组件注册自己为服务提供者，并解析对其他服务的依赖。
    支持异步初始化和服务生命周期管理。
    """

    def __init__(self):
        """初始化共享服务管理器"""
        self._services: Dict[ServiceType, Dict[str, Any]] = {
            service_type: {} for service_type in ServiceType
        }
        self._dependencies: Dict[str, Set[str]] = {}
        self._initialized: Dict[str, bool] = {}
        self._lock = asyncio.Lock()
        self._observers: Dict[ServiceType, List[Callable]] = {
            service_type: [] for service_type in ServiceType
        }
        self._default_services: Dict[ServiceType, str] = {}
        self._transformers: Dict[str, Any] = {}

    async def register_service(self, service_type: ServiceType, name: str,
                               service: Any, make_default: bool = False) -> None:
        """
        注册服务到共享服务管理器

        Args:
            service_type: 服务类型
            name: 服务名称（唯一标识符）
            service: 服务实例
            make_default: 是否将此服务设为默认服务

        Raises:
            ServiceRegistrationException: 如果同名服务已存在
        """
        async with self._lock:
            if name in self._services[service_type]:
                raise ServiceRegistrationException(
                    f"服务 '{name}' (类型: {service_type.name}) 已存在")

            self._services[service_type][name] = service
            self._initialized[name] = False
            logger.debug(f"注册服务: {name} (类型: {service_type.name})")

            if make_default or name not in self._default_services.get(service_type, {}):
                self._default_services[service_type] = name
                logger.debug(f"设置默认 {service_type.name} 服务: {name}")

            # 通知观察者
            for observer in self._observers[service_type]:
                if asyncio.iscoroutinefunction(observer):
                    await observer(name, service)
                else:
                    observer(name, service)

    async def get_service(self, service_type: ServiceType, name: Optional[str] = None) -> Any:
        """
        获取服务实例

        Args:
            service_type: 服务类型
            name: 服务名称（如果为None，返回默认服务）

        Returns:
            服务实例

        Raises:
            ServiceNotFoundException: 如果服务未找到
        """
        if name is None:
            name = self._default_services.get(service_type)
            if name is None:
                raise ServiceNotFoundException(f"没有默认的 {service_type.name} 服务")

        if name not in self._services[service_type]:
            raise ServiceNotFoundException(
                f"服务 '{name}' (类型: {service_type.name}) 未找到")

        return self._services[service_type][name]

    def register_dependency(self, service_name: str, depends_on: str) -> None:
        """
        注册服务依赖

        Args:
            service_name: 依赖方服务名称
            depends_on: 被依赖的服务名称
        """
        if service_name not in self._dependencies:
            self._dependencies[service_name] = set()

        self._dependencies[service_name].add(depends_on)
        logger.debug(f"注册依赖: {service_name} -> {depends_on}")

    async def initialize_services(self) -> None:
        """
        初始化所有服务，确保按依赖顺序初始化

        Raises:
            DependencyResolutionException: 如果无法解析依赖关系
        """
        # 构建初始化顺序
        init_order = self._build_initialization_order()

        # 按顺序初始化服务
        for service_name in init_order:
            for service_type in ServiceType:
                if service_name in self._services[service_type]:
                    service = self._services[service_type][service_name]
                    await self._initialize_service(service_name, service)
                    break

        logger.info("所有服务初始化完成")

    async def _initialize_service(self, service_name: str, service: Any) -> None:
        """初始化单个服务"""
        if self._initialized.get(service_name, False):
            return

        logger.debug(f"初始化服务: {service_name}")

        # 如果服务有初始化方法，调用它
        if hasattr(service, "initialize") and callable(service.initialize):
            try:
                if asyncio.iscoroutinefunction(service.initialize):
                    await service.initialize()
                else:
                    service.initialize()
                self._initialized[service_name] = True
                logger.debug(f"服务 {service_name} 初始化完成")
            except Exception as e:
                logger.error(f"初始化服务 {service_name} 时出错: {e}")
                raise
        else:
            # 没有初始化方法的服务视为已初始化
            self._initialized[service_name] = True
            logger.debug(f"服务 {service_name} 无需初始化")

    def _build_initialization_order(self) -> List[str]:
        """
        构建服务初始化顺序，使用拓扑排序解析依赖

        Returns:
            服务名称列表，按初始化顺序排序

        Raises:
            DependencyResolutionException: 如果存在循环依赖
        """
        # 收集所有服务名称
        all_services = set()
        for service_type in ServiceType:
            all_services.update(self._services[service_type].keys())

        # 检查所有依赖是否存在
        for service, deps in self._dependencies.items():
            for dep in deps:
                if dep not in all_services:
                    raise DependencyResolutionException(
                        f"服务 {service} 依赖不存在的服务 {dep}")

        # 初始化访问状态
        visited = set()
        temp_visited = set()
        order = []

        # 拓扑排序
        def visit(service):
            if service in temp_visited:
                raise DependencyResolutionException(
                    f"检测到循环依赖，涉及服务: {service}")

            if service in visited:
                return

            temp_visited.add(service)

            # 访问此服务的所有依赖
            for dep in self._dependencies.get(service, set()):
                visit(dep)

            temp_visited.remove(service)
            visited.add(service)
            order.append(service)

        # 对所有服务执行拓扑排序
        for service in all_services:
            if service not in visited:
                visit(service)

        # 确保所有服务都被包含
        for service in all_services:
            if service not in order:
                order.append(service)

        return order

    def add_observer(self, service_type: ServiceType, observer: Callable) -> None:
        """
        添加服务观察者，在注册新服务时得到通知

        Args:
            service_type: 服务类型
            observer: 观察者函数，需接受服务名称和服务实例两个参数
        """
        self._observers[service_type].append(observer)

    def remove_observer(self, service_type: ServiceType, observer: Callable) -> bool:
        """
        移除服务观察者

        Args:
            service_type: 服务类型
            observer: 要移除的观察者函数

        Returns:
            是否成功移除
        """
        if observer in self._observers[service_type]:
            self._observers[service_type].remove(observer)
            return True
        return False

    def register_transformer(self, name: str, transformer: Any) -> None:
        """
        注册数据转换器

        Args:
            name: 转换器名称
            transformer: 转换器对象，必须有transform方法
        """
        if not hasattr(transformer, 'transform') or not callable(getattr(transformer, 'transform')):
            raise ServiceRegistrationException("转换器必须有transform方法")

        self._transformers[name] = transformer
        logger.debug(f"注册转换器: {name}")

    async def transform_data(self, data: Any) -> Any:
        """
        使用所有已注册的转换器处理数据

        Args:
            data: 原始数据

        Returns:
            转换后的数据
        """
        result = data
        for name, transformer in self._transformers.items():
            try:
                if asyncio.iscoroutinefunction(transformer.transform):
                    result = await transformer.transform(result)
                else:
                    result = transformer.transform(result)
            except Exception as e:
                logger.error(f"转换器 {name} 处理数据失败: {e}")

        return result

    def get_service_names(self, service_type: Optional[ServiceType] = None) -> Dict[ServiceType, List[str]]:
        """
        获取所有注册的服务名称

        Args:
            service_type: 如果提供，仅返回指定类型的服务

        Returns:
            按服务类型分组的服务名称字典
        """
        if service_type:
            return {service_type: list(self._services[service_type].keys())}

        return {
            st: list(self._services[st].keys())
            for st in ServiceType
        }

    def get_service_status(self) -> Dict[str, Dict[str, Any]]:
        """
        获取所有服务的状态

        Returns:
            服务状态信息字典
        """
        status = {}
        for service_type in ServiceType:
            for name, service in self._services[service_type].items():
                is_default = self._default_services.get(service_type) == name

                service_info = {
                    "type": service_type.name,
                    "initialized": self._initialized.get(name, False),
                    "is_default": is_default,
                    "dependencies": list(self._dependencies.get(name, set()))
                }

                # 如果服务有健康检查方法，调用它
                if hasattr(service, "health_check") and callable(service.health_check):
                    try:
                        service_info["health"] = service.health_check()
                    except Exception as e:
                        service_info["health"] = {
                            "status": "error", "message": str(e)}

                status[name] = service_info

        return status


def requires_service(service_type: ServiceType, name: Optional[str] = None):
    """
    服务依赖装饰器，确保方法执行前已获取所需服务

    Args:
        service_type: 所需的服务类型
        name: 服务名称，如果为None则使用默认服务

    用法示例:
    ```
    @requires_service(ServiceType.EVENT_BUS)
    async def publish_event(self, event_name, data):
        event_bus = self.shared_services.get_service(ServiceType.EVENT_BUS)
        await event_bus.publish(event_name, data)
    ```
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(self, *args, **kwargs):
            if not hasattr(self, "shared_services") or not isinstance(self.shared_services, SharedServices):
                raise AttributeError(
                    f"{self.__class__.__name__} 必须有 shared_services 属性")

            # 尝试获取服务
            try:
                service = await self.shared_services.get_service(service_type, name)
                # 调用原始方法
                return await func(self, *args, **kwargs)
            except ServiceNotFoundException as e:
                logger.error(f"执行 {func.__name__} 失败: {e}")
                raise

        return wrapper
    return decorator
