from typing import Dict, Any, Optional, List, Callable
from dataclasses import dataclass, field
from datetime import datetime
import time
import uuid
import asyncio
import logging
import traceback

from app.core.shared_services import SharedServices, ServiceType

logger = logging.getLogger(__name__)

@dataclass
class ComponentMetrics:
    """组件性能指标"""
    start_time: float = field(default_factory=time.time)
    message_count: int = 0
    error_count: int = 0
    processing_times: List[float] = field(default_factory=list)
    max_history: int = 1000

    def record_message(self, processing_time: float):
        """记录一次消息处理"""
        self.message_count += 1
        self.processing_times.append(processing_time)
        if len(self.processing_times) > self.max_history:
            self.processing_times.pop(0)

    def record_error(self):
        """记录一次错误"""
        self.error_count += 1

    @property
    def avg_processing_time(self) -> float:
        """计算平均处理时间"""
        if not self.processing_times:
            return 0.0
        return sum(self.processing_times) / len(self.processing_times)


class BaseComponent:
    """
    消息系统基础组件抽象类

    为所有消息系统组件提供通用接口和功能，包括:
    - 生命周期管理（启动/停止）
    - 指标收集
    - 基本标识和配置
    - 共享服务访问
    - 错误处理
    - 监控集成
    """

    def __init__(self, name: Optional[str] = None):
        """
        初始化基础组件

        Args:
            name: 组件名称，如果不提供则自动生成
        """
        self.id = str(uuid.uuid4())
        self.name = name or f"{self.__class__.__name__}_{self.id[:8]}"
        self._start_time = 0
        self._component_metrics = ComponentMetrics()
        self._metrics = {
            'created_at': time.time(),
            'started_at': 0,
            'uptime': 0,
            'message_count': 0,
            'error_count': 0,
            'last_error': None,
            'component_type': self.__class__.__name__,
            'processing_times': []
        }
        self._running = False
        self._lifecycle_hooks = {
            'before_start': [],
            'after_start': [],
            'before_stop': [],
            'after_stop': []
        }
        self._transformers = []
        self._shared_services = SharedServices()
        self._message_bus = None
        self._last_health_check = time.time()
        self._health_check_interval = 60  # 默认60秒检查一次

    def add_lifecycle_hook(self, hook_type: str, hook: Callable) -> None:
        """
        添加生命周期钩子
        
        Args:
            hook_type: 钩子类型 ('before_start', 'after_start', 'before_stop', 'after_stop')
            hook: 钩子函数，接收组件实例作为参数
        """
        if hook_type in self._lifecycle_hooks:
            self._lifecycle_hooks[hook_type].append(hook)
        else:
            raise ValueError(f"未知的生命周期钩子类型: {hook_type}")

    async def set_message_bus(self, message_bus) -> None:
        """设置消息总线引用"""
        self._message_bus = message_bus

    async def start(self) -> None:
        """启动组件"""
        if self._running:
            logger.warning(f"组件 {self.name} 已经在运行中")
            return

        # 执行启动前钩子
        for hook in self._lifecycle_hooks['before_start']:
            await self._execute_hook(hook)

        try:
            await self._start_impl()
            self._running = True
            self._start_time = time.time()
            self._metrics['started_at'] = self._start_time
            logger.info(f"组件 {self.name} 已启动")

            # 执行启动后钩子
            for hook in self._lifecycle_hooks['after_start']:
                await self._execute_hook(hook)

        except Exception as e:
            logger.error(f"启动组件 {self.name} 失败: {e}")
            logger.debug(traceback.format_exc())
            self._metrics['last_error'] = {
                'time': time.time(),
                'message': str(e),
                'traceback': traceback.format_exc()
            }
            self._component_metrics.record_error()
            raise

    async def stop(self) -> None:
        """停止组件"""
        if not self._running:
            logger.warning(f"组件 {self.name} 已经停止")
            return

        # 执行停止前钩子
        for hook in self._lifecycle_hooks['before_stop']:
            await self._execute_hook(hook)

        try:
            await self._stop_impl()
            self._running = False
            self._metrics['uptime'] += time.time() - self._start_time
            logger.info(f"组件 {self.name} 已停止")

            # 执行停止后钩子
            for hook in self._lifecycle_hooks['after_stop']:
                await self._execute_hook(hook)

        except Exception as e:
            logger.error(f"停止组件 {self.name} 失败: {e}")
            logger.debug(traceback.format_exc())
            self._metrics['last_error'] = {
                'time': time.time(),
                'message': str(e),
                'traceback': traceback.format_exc()
            }
            self._component_metrics.record_error()
            raise

    async def _execute_hook(self, hook: Callable) -> None:
        """执行生命周期钩子"""
        try:
            if asyncio.iscoroutinefunction(hook):
                await hook(self)
            else:
                hook(self)
        except Exception as e:
            logger.error(f"执行钩子 {hook.__name__} 失败: {e}")
            logger.debug(traceback.format_exc())

    async def _start_impl(self) -> None:
        """组件启动实现 (可由子类重写)"""
        pass

    async def _stop_impl(self) -> None:
        """组件停止实现 (可由子类重写)"""
        pass

    async def handle_message(self, topic: str, data: Any) -> None:
        """
        处理接收到的消息 (可由子类重写)
        
        Args:
            topic: 消息主题
            data: 消息数据
        """
        pass

    def add_transformer(self, transformer: Callable) -> None:
        """
        添加数据转换器
        
        Args:
            transformer: 转换器对象，必须实现transform方法
        """
        self._transformers.append(transformer)
        self._shared_services.register_transformer(
            f"{self.name}_{len(self._transformers)}", 
            transformer
        )

    async def _transform_data(self, data: Any) -> Any:
        """
        应用所有转换器处理数据
        
        Args:
            data: 要转换的数据
            
        Returns:
            转换后的数据
        """
        start_time = time.time()
        try:
            result = data
            for transformer in self._transformers:
                if asyncio.iscoroutinefunction(transformer.transform):
                    result = await transformer.transform(result)
                else:
                    result = transformer.transform(result)
            
            # 记录性能指标
            processing_time = time.time() - start_time
            self._component_metrics.record_message(processing_time)
            return result
            
        except Exception as e:
            logger.error(f"转换数据失败: {e}")
            self._component_metrics.record_error()
            self._metrics['last_error'] = {
                'time': time.time(),
                'message': str(e),
                'traceback': traceback.format_exc()
            }
            raise

    async def check_health(self) -> Dict[str, Any]:
        """
        检查组件健康状态
        
        Returns:
            组件健康状态信息
        """
        now = time.time()
        if now - self._last_health_check < self._health_check_interval:
            # 返回缓存的健康状态
            return self._get_health_status()

        try:
            # 执行实际的健康检查
            await self._health_check_impl()
            self._last_health_check = now
            return self._get_health_status()
            
        except Exception as e:
            logger.error(f"健康检查失败: {e}")
            return self._get_health_status(False, str(e))

    async def _health_check_impl(self) -> None:
        """健康检查实现 (可由子类重写)"""
        pass

    def _get_health_status(self, healthy: bool = True, error: Optional[str] = None) -> Dict[str, Any]:
        """获取健康状态信息"""
        return {
            'component': self.name,
            'id': self.id,
            'type': self.__class__.__name__,
            'healthy': healthy,
            'running': self._running,
            'uptime': time.time() - self._start_time if self._running else 0,
            'last_error': error or self._metrics['last_error'],
            'last_check': datetime.fromtimestamp(self._last_health_check).isoformat()
        }

    @property
    def metrics(self) -> Dict[str, Any]:
        """获取组件指标"""
        current_metrics = self._metrics.copy()
        
        # 更新当前运行时间
        if self._running:
            current_metrics['uptime'] = time.time() - self._start_time
            
        # 添加组件指标
        current_metrics['component_metrics'] = {
            'message_count': self._component_metrics.message_count,
            'error_count': self._component_metrics.error_count,
            'avg_processing_time': self._component_metrics.avg_processing_time
        }
        
        return current_metrics

    def is_running(self) -> bool:
        """检查组件是否正在运行"""
        return self._running

    @property
    def shared_services(self) -> SharedServices:
        """获取共享服务实例"""
        return self._shared_services


class ComponentManager:
    """组件管理器，负责跟踪和管理所有组件的生命周期"""

    def __init__(self, shared_services: Optional[SharedServices] = None):
        """
        初始化组件管理器
        
        Args:
            shared_services: 共享服务实例
        """
        self.components: Dict[str, BaseComponent] = {}
        self.shared_services = shared_services or SharedServices()
        self._startup_order: List[str] = []
        self._shutdown_order: List[str] = []
        self._dependencies: Dict[str, List[str]] = {}

    def register_component(self, component: BaseComponent, 
                           dependencies: Optional[List[str]] = None) -> None:
        """
        注册组件
        
        Args:
            component: 要注册的组件
            dependencies: 组件的依赖项列表（组件名称）
        """
        if component.name in self.components:
            raise ValueError(f"组件 {component.name} 已注册")
        
        self.components[component.name] = component
        
        # 设置共享服务
        component._shared_services = self.shared_services
        
        # 记录依赖关系
        deps = dependencies or []
        self._dependencies[component.name] = deps
        
        # 重新计算启动和关闭顺序
        self._calculate_execution_order()
        
        logger.info(f"注册组件: {component.name}")

    def _calculate_execution_order(self) -> None:
        """计算组件的启动和关闭顺序，考虑依赖关系"""
        # 拓扑排序
        visited = set()
        temp_visited = set()
        order = []
        
        def visit(name):
            if name in temp_visited:
                raise ValueError(f"检测到组件依赖循环: {name}")
            
            if name in visited:
                return
            
            temp_visited.add(name)
            
            # 访问依赖项
            for dep in self._dependencies.get(name, []):
                if dep not in self.components:
                    logger.warning(f"组件 {name} 依赖未注册的组件 {dep}")
                    continue
                visit(dep)
            
            temp_visited.remove(name)
            visited.add(name)
            order.append(name)
        
        # 对所有组件进行拓扑排序
        for name in self.components:
            if name not in visited:
                visit(name)
        
        self._startup_order = order
        self._shutdown_order = list(reversed(order))

    async def start_all(self) -> None:
        """按依赖顺序启动所有组件"""
        logger.info("开始启动所有组件...")
        
        for name in self._startup_order:
            component = self.components[name]
            logger.info(f"正在启动组件: {name}")
            try:
                await component.start()
                logger.info(f"组件 {name} 启动成功")
            except Exception as e:
                logger.error(f"组件 {name} 启动失败: {e}")
                # 在生产环境中可能需要决定是继续还是中断启动过程
                raise
                
        logger.info("所有组件启动完成")

    async def stop_all(self) -> None:
        """按依赖的逆序停止所有组件"""
        logger.info("开始停止所有组件...")
        
        for name in self._shutdown_order:
            component = self.components[name]
            logger.info(f"正在停止组件: {name}")
            try:
                await component.stop()
                logger.info(f"组件 {name} 已停止")
            except Exception as e:
                logger.error(f"停止组件 {name} 失败: {e}")
                # 继续停止其他组件
        
        logger.info("所有组件已停止")
        
    def get_component(self, name: str) -> Optional[BaseComponent]:
        """
        获取指定名称的组件
        
        Args:
            name: 组件名称
            
        Returns:
            组件实例，如果不存在则返回None
        """
        return self.components.get(name)
    
    def get_components_by_type(self, component_type: str) -> List[BaseComponent]:
        """
        获取指定类型的所有组件
        
        Args:
            component_type: 组件类型名称
            
        Returns:
            符合类型的组件列表
        """
        return [
            component for component in self.components.values()
            if component.__class__.__name__ == component_type
        ]
        
    async def check_health(self) -> Dict[str, Dict[str, Any]]:
        """
        检查所有组件的健康状态
        
        Returns:
            组件名称到健康状态的映射
        """
        health_status = {}
        
        for name, component in self.components.items():
            try:
                health_status[name] = await component.check_health()
            except Exception as e:
                health_status[name] = {
                    'component': name,
                    'healthy': False,
                    'error': str(e)
                }
                
        return health_status
    
    def get_metrics(self) -> Dict[str, Dict[str, Any]]:
        """
        获取所有组件的性能指标
        
        Returns:
            组件名称到指标的映射
        """
        return {
            name: component.metrics
            for name, component in self.components.items()
        }
