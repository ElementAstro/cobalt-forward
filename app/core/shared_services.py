from functools import wraps
from typing import Any, Dict, Optional, Callable, List, Set, TypeVar, Generic
from enum import Enum, auto
import logging
import time
import asyncio
from collections import defaultdict

logger = logging.getLogger(__name__)

T = TypeVar('T')

class ServiceType(Enum):
    """共享服务类型枚举"""
    MESSAGE_BUS = auto()
    EVENT_BUS = auto()
    COMMAND_DISPATCHER = auto()
    PLUGIN_MANAGER = auto()
    CONFIG_MANAGER = auto()
    TRANSFORMER = auto()
    PROTOCOL_CONVERTER = auto()
    SSH_FORWARDER = auto()
    UPLOAD_MANAGER = auto()
    STREAM_HANDLER = auto()
    HTTP_CLIENT = auto()
    DATABASE = auto()
    CACHE = auto()
    CUSTOM = auto()


class ServiceProvider(Generic[T]):
    """服务提供者，封装对一种服务的访问"""
    
    def __init__(self, service_type: ServiceType, service: T):
        """
        初始化服务提供者
        
        Args:
            service_type: 服务类型
            service: 服务实例
        """
        self.service_type = service_type
        self.service = service
        self.created_at = time.time()
        self.last_accessed = time.time()
        self.access_count = 0
    
    def access(self) -> T:
        """
        访问服务，更新访问计数和时间
        
        Returns:
            服务实例
        """
        self.last_accessed = time.time()
        self.access_count += 1
        return self.service


class ServiceRegistry:
    """服务注册表，管理系统中的服务实例"""
    
    def __init__(self):
        """初始化服务注册表"""
        self._services: Dict[str, ServiceProvider] = {}
        self._service_by_type: Dict[ServiceType, Dict[str, str]] = defaultdict(dict)
        self._hooks: Dict[str, List[Callable]] = defaultdict(list)
    
    def register(self, name: str, service: Any, service_type: ServiceType) -> None:
        """
        注册服务
        
        Args:
            name: 服务名称
            service: 服务实例
            service_type: 服务类型
        """
        if name in self._services:
            logger.warning(f"服务 '{name}' 已存在，将被覆盖")
        
        provider = ServiceProvider(service_type, service)
        self._services[name] = provider
        self._service_by_type[service_type][name] = name
        
        # 触发注册钩子
        for hook in self._hooks.get('register', []):
            try:
                hook(name, service, service_type)
            except Exception as e:
                logger.error(f"执行注册钩子时出错: {e}")
        
        logger.info(f"注册服务: {name} ({service_type.name})")
    
    def unregister(self, name: str) -> bool:
        """
        注销服务
        
        Args:
            name: 服务名称
            
        Returns:
            是否成功注销
        """
        if name not in self._services:
            return False
        
        service_type = self._services[name].service_type
        
        # 从类型映射中移除
        if name in self._service_by_type[service_type]:
            del self._service_by_type[service_type][name]
        
        # 从服务列表中移除
        del self._services[name]
        
        # 触发注销钩子
        for hook in self._hooks.get('unregister', []):
            try:
                hook(name, service_type)
            except Exception as e:
                logger.error(f"执行注销钩子时出错: {e}")
        
        logger.info(f"注销服务: {name}")
        return True
    
    def get(self, name: str) -> Optional[Any]:
        """
        获取服务实例
        
        Args:
            name: 服务名称
            
        Returns:
            服务实例，不存在则返回None
        """
        if name not in self._services:
            return None
        
        return self._services[name].access()
    
    def get_by_type(self, service_type: ServiceType) -> Dict[str, Any]:
        """
        获取指定类型的所有服务
        
        Args:
            service_type: 服务类型
            
        Returns:
            服务名称到实例的映射
        """
        return {
            name: self._services[service_id].access()
            for name, service_id in self._service_by_type[service_type].items()
            if service_id in self._services
        }
    
    def exists(self, name: str) -> bool:
        """
        检查服务是否存在
        
        Args:
            name: 服务名称
            
        Returns:
            服务是否存在
        """
        return name in self._services
    
    def add_hook(self, event: str, hook: Callable) -> None:
        """
        添加服务事件钩子
        
        Args:
            event: 事件名称 ('register', 'unregister')
            hook: 钩子函数
        """
        self._hooks[event].append(hook)
    
    def get_stats(self) -> Dict[str, Any]:
        """
        获取服务注册表统计信息
        
        Returns:
            统计信息
        """
        stats = {
            'total_services': len(self._services),
            'services_by_type': {
                service_type.name: len(services)
                for service_type, services in self._service_by_type.items()
            },
            'service_access': {
                name: {
                    'access_count': provider.access_count,
                    'last_accessed': provider.last_accessed,
                    'uptime': time.time() - provider.created_at
                }
                for name, provider in self._services.items()
            }
        }
        return stats


class SharedServices:
    """共享服务容器，为组件提供对共享服务的访问"""
    
    def __init__(self):
        """初始化共享服务容器"""
        self._registry = ServiceRegistry()
        self._transformers: Dict[str, Any] = {}
        self._converters: Dict[str, Any] = {}
        self._dependencies: Dict[str, Set[str]] = defaultdict(set)
        self._message_bus = None
        self._event_bus = None
        self._command_dispatcher = None
    
    def register_service(self, name: str, service: Any, service_type: ServiceType) -> None:
        """
        注册服务
        
        Args:
            name: 服务名称
            service: 服务实例
            service_type: 服务类型
        """
        self._registry.register(name, service, service_type)
    
    def unregister_service(self, name: str) -> bool:
        """
        注销服务
        
        Args:
            name: 服务名称
            
        Returns:
            是否成功注销
        """
        return self._registry.unregister(name)
    
    def get_service(self, name: str) -> Optional[Any]:
        """
        获取服务实例
        
        Args:
            name: 服务名称
            
        Returns:
            服务实例，不存在则返回None
        """
        return self._registry.get(name)
    
    def get_services_by_type(self, service_type: ServiceType) -> Dict[str, Any]:
        """
        获取指定类型的所有服务
        
        Args:
            service_type: 服务类型
            
        Returns:
            服务名称到实例的映射
        """
        return self._registry.get_by_type(service_type)
    
    def register_transformer(self, name: str, transformer: Any) -> None:
        """
        注册转换器
        
        Args:
            name: 转换器名称
            transformer: 转换器实例
        """
        self._transformers[name] = transformer
        self._registry.register(f"transformer:{name}", transformer, ServiceType.TRANSFORMER)
    
    def get_transformer(self, name: str) -> Optional[Any]:
        """
        获取转换器
        
        Args:
            name: 转换器名称
            
        Returns:
            转换器实例，不存在则返回None
        """
        return self._transformers.get(name)
    
    def register_protocol_converter(self, name: str, converter: Any) -> None:
        """
        注册协议转换器
        
        Args:
            name: 转换器名称
            converter: 转换器实例
        """
        self._converters[name] = converter
        self._registry.register(f"converter:{name}", converter, ServiceType.PROTOCOL_CONVERTER)
    
    def get_protocol_converter(self, name: str) -> Optional[Any]:
        """
        获取协议转换器
        
        Args:
            name: 转换器名称
            
        Returns:
            转换器实例，不存在则返回None
        """
        return self._converters.get(name)
    
    def register_dependency(self, component_name: str, depends_on: str) -> None:
        """
        注册组件依赖关系
        
        Args:
            component_name: 组件名称
            depends_on: 依赖的组件名称
        """
        self._dependencies[component_name].add(depends_on)
    
    def get_dependencies(self, component_name: str) -> Set[str]:
        """
        获取组件的所有依赖
        
        Args:
            component_name: 组件名称
            
        Returns:
            依赖组件名称集合
        """
        return self._dependencies.get(component_name, set())
    
    def get_dependent_components(self, component_name: str) -> Set[str]:
        """
        获取依赖于指定组件的所有组件
        
        Args:
            component_name: 组件名称
            
        Returns:
            依赖的组件名称集合
        """
        dependents = set()
        for name, deps in self._dependencies.items():
            if component_name in deps:
                dependents.add(name)
        return dependents
    
    @property
    def message_bus(self):
        """获取消息总线实例"""
        if not self._message_bus:
            self._message_bus = self._registry.get_by_type(ServiceType.MESSAGE_BUS).get('message_bus')
        return self._message_bus
    
    @property
    def event_bus(self):
        """获取事件总线实例"""
        if not self._event_bus:
            self._event_bus = self._registry.get_by_type(ServiceType.EVENT_BUS).get('event_bus')
        return self._event_bus
    
    @property
    def command_dispatcher(self):
        """获取命令分发器实例"""
        if not self._command_dispatcher:
            self._command_dispatcher = self._registry.get_by_type(ServiceType.COMMAND_DISPATCHER).get('command_dispatcher')
        return self._command_dispatcher
    
    def get_stats(self) -> Dict[str, Any]:
        """
        获取共享服务统计信息
        
        Returns:
            统计信息
        """
        return {
            'registry': self._registry.get_stats(),
            'transformers': len(self._transformers),
            'converters': len(self._converters),
            'dependencies': {
                name: list(deps)
                for name, deps in self._dependencies.items()
            }
        }


def require_service(service_name: str, service_type: Optional[ServiceType] = None):
    """
    服务依赖装饰器，用于注入服务依赖
    
    Args:
        service_name: 服务名称
        service_type: 服务类型，用于辅助查找
    
    Returns:
        装饰器函数
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(self, *args, **kwargs):
            # 获取共享服务容器
            shared_services = getattr(self, 'shared_services', None)
            if not shared_services:
                raise AttributeError(f"组件 {self.__class__.__name__} 没有 shared_services 属性")
            
            # 查找服务
            service = shared_services.get_service(service_name)
            if not service and service_type:
                # 尝试按类型查找
                services = shared_services.get_services_by_type(service_type)
                if services:
                    service = list(services.values())[0]
            
            if not service:
                raise ValueError(f"未找到服务: {service_name}")
            
            # 注册服务依赖
            component_name = getattr(self, 'name', self.__class__.__name__)
            shared_services.register_dependency(component_name, service_name)
            
            # 将服务添加到关键字参数
            kwargs[service_name] = service
            
            return await func(self, *args, **kwargs)
        return wrapper
    return decorator


class ServiceManagerMixin:
    """服务管理混入类，为组件提供服务管理功能"""
    
    def __init__(self, shared_services: Optional[SharedServices] = None):
        """
        初始化服务管理混入类
        
        Args:
            shared_services: 共享服务容器
        """
        self._shared_services = shared_services or SharedServices()
    
    def register_service(self, name: str, service: Any, service_type: ServiceType) -> None:
        """
        注册服务
        
        Args:
            name: 服务名称
            service: 服务实例
            service_type: 服务类型
        """
        self._shared_services.register_service(name, service, service_type)
    
    def get_service(self, name: str) -> Optional[Any]:
        """
        获取服务实例
        
        Args:
            name: 服务名称
            
        Returns:
            服务实例，不存在则返回None
        """
        return self._shared_services.get_service(name)
    
    @property
    def message_bus(self):
        """获取消息总线实例"""
        return self._shared_services.message_bus
    
    @property
    def event_bus(self):
        """获取事件总线实例"""
        return self._shared_services.event_bus
    
    @property
    def command_dispatcher(self):
        """获取命令分发器实例"""
        return self._shared_services.command_dispatcher
