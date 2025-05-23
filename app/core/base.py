from typing import Dict, Any, Optional, List, Callable, Set, TypeVar, Union, Coroutine
from dataclasses import dataclass, field
from datetime import datetime
import time
import uuid
import asyncio
import logging
import traceback

from app.core.shared_services import SharedServices

logger = logging.getLogger(__name__)

T = TypeVar('T')  # Define a type variable for transformations


class MessageBus:
    """Message Bus interface type for type checking"""
    pass


@dataclass
class ComponentMetrics:
    """Component performance metrics"""
    start_time: float = field(default_factory=time.time)
    message_count: int = 0
    error_count: int = 0
    # 修复: 为List添加明确的类型注解
    processing_times: List[float] = field(default_factory=lambda: [])
    max_history: int = 1000

    def record_message(self, processing_time: float):
        """Record a message processing"""
        self.message_count += 1
        self.processing_times.append(processing_time)
        if len(self.processing_times) > self.max_history:
            self.processing_times.pop(0)

    def record_error(self):
        """Record an error"""
        self.error_count += 1

    @property
    def avg_processing_time(self) -> float:
        """Calculate average processing time"""
        if not self.processing_times:
            return 0.0
        return sum(self.processing_times) / len(self.processing_times)


class BaseComponent:
    """
    Message system base component abstract class

    Provides common interfaces and functionality for all message system components, including:
    - Lifecycle management (start/stop)
    - Metrics collection
    - Basic identification and configuration
    - Shared service access
    - Error handling
    - Monitoring integration
    """

    def __init__(self, name: Optional[str] = None):
        """
        Initialize base component

        Args:
            name: Component name, automatically generated if not provided
        """
        self.id = str(uuid.uuid4())
        self.name = name or f"{self.__class__.__name__}_{self.id[:8]}"
        self._start_time = 0
        self._component_metrics = ComponentMetrics()
        self._metrics: Dict[str, Any] = {
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
        self._lifecycle_hooks: Dict[str, List[Callable[["BaseComponent"], Union[None, Coroutine[Any, Any, None]]]]] = {
            'before_start': [],
            'after_start': [],
            'before_stop': [],
            'after_stop': []
        }
        self._transformers: List[Any] = []  # Any object with transform method
        self._shared_services = SharedServices()
        self._message_bus: Optional[MessageBus] = None
        self._last_health_check = time.time()
        self._health_check_interval = 60  # Default 60 seconds

    def add_lifecycle_hook(self,
                           hook_type: str,
                           hook: Callable[["BaseComponent"], Union[None, Coroutine[Any, Any, None]]]) -> None:
        """
        Add lifecycle hook

        Args:
            hook_type: Hook type ('before_start', 'after_start', 'before_stop', 'after_stop')
            hook: Hook function, receives component instance as parameter
        """
        if hook_type in self._lifecycle_hooks:
            self._lifecycle_hooks[hook_type].append(hook)
        else:
            raise ValueError(f"Unknown lifecycle hook type: {hook_type}")

    async def set_message_bus(self, message_bus: MessageBus) -> None:
        """Set message bus reference"""
        self._message_bus = message_bus

    async def start(self) -> None:
        """Start component"""
        if self._running:
            logger.warning(f"Component {self.name} is already running")
            return

        # Execute pre-start hooks
        for hook in self._lifecycle_hooks['before_start']:
            await self._execute_hook(hook)

        try:
            await self._start_impl()
            self._running = True
            self._start_time = time.time()
            self._metrics['started_at'] = self._start_time
            logger.info(f"Component {self.name} started")

            # Execute post-start hooks
            for hook in self._lifecycle_hooks['after_start']:
                await self._execute_hook(hook)

        except Exception as e:
            logger.error(f"Failed to start component {self.name}: {e}")
            logger.debug(traceback.format_exc())
            self._metrics['last_error'] = {
                'time': time.time(),
                'message': str(e),
                'traceback': traceback.format_exc()
            }
            self._component_metrics.record_error()
            raise

    async def stop(self) -> None:
        """Stop component"""
        if not self._running:
            logger.warning(f"Component {self.name} is already stopped")
            return

        # Execute pre-stop hooks
        for hook in self._lifecycle_hooks['before_stop']:
            await self._execute_hook(hook)

        try:
            await self._stop_impl()
            self._running = False
            self._metrics['uptime'] += time.time() - self._start_time
            logger.info(f"Component {self.name} stopped")

            # Execute post-stop hooks
            for hook in self._lifecycle_hooks['after_stop']:
                await self._execute_hook(hook)

        except Exception as e:
            logger.error(f"Failed to stop component {self.name}: {e}")
            logger.debug(traceback.format_exc())
            self._metrics['last_error'] = {
                'time': time.time(),
                'message': str(e),
                'traceback': traceback.format_exc()
            }
            self._component_metrics.record_error()
            raise

    async def _execute_hook(self,
                            hook: Callable[["BaseComponent"], Union[None, Coroutine[Any, Any, None]]]) -> None:
        """Execute lifecycle hook"""
        try:
            if asyncio.iscoroutinefunction(hook):
                await hook(self)
            else:
                hook(self)
        except Exception as e:
            logger.error(f"Failed to execute hook {hook.__name__}: {e}")
            logger.debug(traceback.format_exc())

    async def _start_impl(self) -> None:
        """Component start implementation (can be overridden by subclass)"""
        pass

    async def _stop_impl(self) -> None:
        """Component stop implementation (can be overridden by subclass)"""
        pass

    async def handle_message(self, topic: str, data: Any) -> None:
        """
        Handle received message (can be overridden by subclass)

        Args:
            topic: Message topic
            data: Message data
        """
        logger.debug(
            f"Base handle_message called with topic: {topic}, data type: {type(data)}")
        pass

    def add_transformer(self, transformer: Any) -> None:
        """
        Add data transformer

        Args:
            transformer: Transformer object, must implement transform method
        """
        self._transformers.append(transformer)
        self._shared_services.register_transformer(
            f"{self.name}_{len(self._transformers)}",
            transformer
        )

    # 修复: 将泛型函数的TypeVar改为object类型
    async def _transform_data(self, data: Any) -> Any:
        """
        Apply all transformers to process data

        Args:
            data: Data to transform

        Returns:
            Transformed data
        """
        start_time = time.time()
        try:
            result: Any = data
            for transformer in self._transformers:
                if asyncio.iscoroutinefunction(transformer.transform):
                    result = await transformer.transform(result)
                else:
                    result = transformer.transform(result)

            # Record performance metrics
            processing_time = time.time() - start_time
            self._component_metrics.record_message(processing_time)
            return result

        except Exception as e:
            logger.error(f"Failed to transform data: {e}")
            self._component_metrics.record_error()
            self._metrics['last_error'] = {
                'time': time.time(),
                'message': str(e),
                'traceback': traceback.format_exc()
            }
            raise

    async def check_health(self) -> Dict[str, Any]:
        """
        Check component health status

        Returns:
            Component health status information
        """
        now = time.time()
        if now - self._last_health_check < self._health_check_interval:
            # Return cached health status
            return self._get_health_status()

        try:
            # Perform actual health check
            await self._health_check_impl()
            self._last_health_check = now
            return self._get_health_status()

        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return self._get_health_status(False, str(e))

    async def _health_check_impl(self) -> None:
        """Health check implementation (can be overridden by subclass)"""
        pass

    def _get_health_status(self, healthy: bool = True, error: Optional[str] = None) -> Dict[str, Any]:
        """Get health status information"""
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
        """Get component metrics"""
        current_metrics = self._metrics.copy()

        # Update current runtime
        if self._running:
            current_metrics['uptime'] = time.time() - self._start_time

        # Add component metrics
        current_metrics['component_metrics'] = {
            'message_count': self._component_metrics.message_count,
            'error_count': self._component_metrics.error_count,
            'avg_processing_time': self._component_metrics.avg_processing_time
        }

        return current_metrics

    def is_running(self) -> bool:
        """Check if component is running"""
        return self._running

    @property
    def shared_services(self) -> SharedServices:
        """Get shared services instance"""
        return self._shared_services

    # 修复: 添加setter方法允许外部更新shared_services
    @shared_services.setter
    def shared_services(self, services: SharedServices) -> None:
        """Set shared services instance"""
        self._shared_services = services


class ComponentManager:
    """Component manager, responsible for tracking and managing all component lifecycles"""

    def __init__(self, shared_services: Optional[SharedServices] = None):
        """
        Initialize component manager

        Args:
            shared_services: Shared services instance
        """
        self.components: Dict[str, BaseComponent] = {}
        self.shared_services = shared_services or SharedServices()
        self._startup_order: List[str] = []
        self._shutdown_order: List[str] = []
        self._dependencies: Dict[str, List[str]] = {}

    def register_component(self, component: BaseComponent,
                           dependencies: Optional[List[str]] = None) -> None:
        """
        Register component

        Args:
            component: Component to register
            dependencies: List of component dependencies (component names)
        """
        if component.name in self.components:
            raise ValueError(f"Component {component.name} already registered")

        self.components[component.name] = component

        # 修复: 使用setter方法设置shared_services而不是直接访问受保护的属性
        component.shared_services = self.shared_services

        # Record dependencies
        deps = dependencies or []
        self._dependencies[component.name] = deps

        # Recalculate startup and shutdown order
        self._calculate_execution_order()

        logger.info(f"Registered component: {component.name}")

    def _calculate_execution_order(self) -> None:
        """Calculate component startup and shutdown order, considering dependencies"""
        # Topological sort
        visited: Set[str] = set()
        temp_visited: Set[str] = set()
        order: List[str] = []

        def visit(name: str) -> None:
            if name in temp_visited:
                raise ValueError(
                    f"Component dependency cycle detected: {name}")

            if name in visited:
                return

            temp_visited.add(name)

            # Visit dependencies
            for dep in self._dependencies.get(name, []):
                if dep not in self.components:
                    logger.warning(
                        f"Component {name} depends on unregistered component {dep}")
                    continue
                visit(dep)

            temp_visited.remove(name)
            visited.add(name)
            order.append(name)

        # Perform topological sort on all components
        for name in self.components:
            if name not in visited:
                visit(name)

        self._startup_order = order
        self._shutdown_order = list(reversed(order))

    async def start_all(self) -> None:
        """Start all components in dependency order"""
        logger.info("Starting all components...")

        for name in self._startup_order:
            component = self.components[name]
            logger.info(f"Starting component: {name}")
            try:
                await component.start()
                logger.info(f"Component {name} started successfully")
            except Exception as e:
                logger.error(f"Component {name} failed to start: {e}")
                # In production, might need to decide whether to continue or abort
                raise

        logger.info("All components started")

    async def stop_all(self) -> None:
        """Stop all components in reverse dependency order"""
        logger.info("Stopping all components...")

        for name in self._shutdown_order:
            component = self.components[name]
            logger.info(f"Stopping component: {name}")
            try:
                await component.stop()
                logger.info(f"Component {name} stopped")
            except Exception as e:
                logger.error(f"Failed to stop component {name}: {e}")
                # Continue stopping other components

        logger.info("All components stopped")

    def get_component(self, name: str) -> Optional[BaseComponent]:
        """
        Get component by name

        Args:
            name: Component name

        Returns:
            Component instance, or None if not found
        """
        return self.components.get(name)

    def get_components_by_type(self, component_type: str) -> List[BaseComponent]:
        """
        Get all components of specified type

        Args:
            component_type: Component type name

        Returns:
            List of components matching the type
        """
        return [
            component for component in self.components.values()
            if component.__class__.__name__ == component_type
        ]

    async def check_health(self) -> Dict[str, Dict[str, Any]]:
        """
        Check health status of all components

        Returns:
            Mapping of component names to health status
        """
        health_status: Dict[str, Dict[str, Any]] = {}

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
        Get performance metrics for all components

        Returns:
            Mapping of component names to metrics
        """
        return {
            name: component.metrics
            for name, component in self.components.items()
        }
