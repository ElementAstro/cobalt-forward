from typing import Any, Callable, Dict, List, Optional, Union
import asyncio
from enum import Enum, auto
from loguru import logger
import time
from dataclasses import dataclass, field
import traceback
from weakref import WeakSet
from functools import wraps
import uuid


class EventPriority(Enum):
    """事件处理的优先级"""
    LOW = 0
    NORMAL = 1
    HIGH = 2
    URGENT = 3


@dataclass
class Event:
    """事件类，包含事件名称、数据和元数据"""
    name: str
    data: Any = None
    priority: EventPriority = EventPriority.NORMAL
    timestamp: float = field(default_factory=lambda: time.time())
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __lt__(self, other):
        """支持事件的优先级比较，用于优先队列"""
        if not isinstance(other, Event):
            return NotImplemented
        # 首先按优先级，然后按时间戳（先进先出）
        return (self.priority.value > other.priority.value or
                (self.priority == other.priority and self.timestamp < self.timestamp))


class EventDispatchMode(Enum):
    """事件分发模式"""
    SEQUENTIAL = auto()  # 顺序处理所有处理器
    PARALLEL = auto()    # 并行处理所有处理器
    FIRST_MATCH = auto()  # 处理第一个成功的处理器


class EventBus:
    """高性能事件总线，支持异步事件处理和优先级队列"""

    def __init__(self, max_workers: int = 10, queue_size: int = 1000):
        """
        初始化事件总线

        Args:
            max_workers: 最大并发工作线程数
            queue_size: 事件队列大小限制
        """
        self._subscribers: Dict[str, Dict[EventPriority, List[Callable]]] = {}
        self._wildcard_subscribers: Dict[EventPriority, List[Callable]] = {
            priority: [] for priority in EventPriority
        }
        self._event_queue = asyncio.PriorityQueue(maxsize=queue_size)
        self._running = False
        self._workers: List[asyncio.Task] = []
        self._max_workers = max_workers
        self._metrics = {
            'published_events': 0,
            'processed_events': 0,
            'failed_events': 0,
            'event_types': {},
            'processing_times': {},
            'worker_utilization': 0.0,
        }
        self._worker_semaphore = asyncio.Semaphore(max_workers)
        self._active_handlers: WeakSet = WeakSet()
        self._default_dispatch_mode = EventDispatchMode.PARALLEL
        self._dispatch_modes: Dict[str, EventDispatchMode] = {}
        self._event_hooks: Dict[str, List[Callable]] = {
            'before_publish': [],
            'after_publish': [],
            'before_process': [],
            'after_process': [],
            'on_error': []
        }

    async def start(self):
        """启动事件总线的事件处理"""
        if self._running:
            return

        self._running = True
        self._workers = [
            asyncio.create_task(self._worker_process())
            for _ in range(self._max_workers)
        ]
        logger.info(f"EventBus started with {self._max_workers} workers")

    async def stop(self):
        """停止事件总线，等待所有事件处理完成"""
        if not self._running:
            return

        self._running = False

        # 等待所有工作线程完成
        if self._workers:
            await asyncio.gather(*self._workers, return_exceptions=True)
            self._workers = []

        # 等待所有活动处理器完成
        if self._active_handlers:
            logger.info(f"等待 {len(self._active_handlers)} 个活动处理器完成...")
            while self._active_handlers:
                await asyncio.sleep(0.1)

        logger.info("EventBus stopped")

    def subscribe(self, event_name: str, callback: Callable,
                  priority: EventPriority = EventPriority.NORMAL):
        """
        订阅指定名称的事件

        Args:
            event_name: 事件名称，可以使用'*'作为通配符
            callback: 事件处理函数，可以是同步或异步函数
            priority: 事件处理的优先级
        """
        if event_name == '*':
            self._wildcard_subscribers[priority].append(callback)
        else:
            if event_name not in self._subscribers:
                self._subscribers[event_name] = {p: [] for p in EventPriority}
            self._subscribers[event_name][priority].append(callback)
        logger.debug(
            f"Subscribed to event {event_name} with priority {priority}")

    def unsubscribe(self, event_name: str, callback: Callable):
        """
        取消订阅事件

        Args:
            event_name: 事件名称
            callback: 要取消的事件处理函数
        """
        if event_name == '*':
            for priority in EventPriority:
                if callback in self._wildcard_subscribers[priority]:
                    self._wildcard_subscribers[priority].remove(callback)
        elif event_name in self._subscribers:
            for priority in EventPriority:
                if callback in self._subscribers[event_name][priority]:
                    self._subscribers[event_name][priority].remove(callback)
            # 如果没有订阅者，清理该事件名称的字典
            if all(not self._subscribers[event_name][p] for p in EventPriority):
                del self._subscribers[event_name]
        logger.debug(f"Unsubscribed from event {event_name}")

    def set_dispatch_mode(self, event_name: str, mode: EventDispatchMode):
        """
        为特定事件设置分发模式

        Args:
            event_name: 事件名称
            mode: 分发模式
        """
        self._dispatch_modes[event_name] = mode

    def set_default_dispatch_mode(self, mode: EventDispatchMode):
        """
        设置默认的事件分发模式

        Args:
            mode: 分发模式
        """
        self._default_dispatch_mode = mode

    def add_hook(self, hook_type: str, callback: Callable):
        """
        添加事件钩子函数

        Args:
            hook_type: 钩子类型 ('before_publish', 'after_publish', 'before_process', 'after_process', 'on_error')
            callback: 钩子函数
        """
        if hook_type in self._event_hooks:
            self._event_hooks[hook_type].append(callback)
        else:
            raise ValueError(f"未知的钩子类型: {hook_type}")

    async def publish(self, event: Union[Event, str], data: Any = None,
                      priority: EventPriority = None):
        """
        发布事件到事件总线

        Args:
            event: 事件对象或事件名称字符串
            data: 如果event是字符串，则为事件数据
            priority: 如果event是字符串，则为事件优先级

        Returns:
            事件ID
        """
        # 如果传入的是事件名称，创建一个新的事件对象
        if isinstance(event, str):
            event = Event(
                name=event,
                data=data,
                priority=priority or EventPriority.NORMAL
            )

        # 执行发布前钩子
        for hook in self._event_hooks['before_publish']:
            try:
                if asyncio.iscoroutinefunction(hook):
                    await hook(event)
                else:
                    hook(event)
            except Exception as e:
                logger.error(f"发布前钩子执行失败: {e}")

        try:
            # 发布事件到队列
            await self._event_queue.put((event.priority.value, event))

            # 更新指标
            self._metrics['published_events'] += 1
            if event.name not in self._metrics['event_types']:
                self._metrics['event_types'][event.name] = 0
            self._metrics['event_types'][event.name] += 1

            # 执行发布后钩子
            for hook in self._event_hooks['after_publish']:
                try:
                    if asyncio.iscoroutinefunction(hook):
                        await hook(event)
                    else:
                        hook(event)
                except Exception as e:
                    logger.error(f"发布后钩子执行失败: {e}")

            logger.trace(f"Published event {event.name} with id {event.id}")
            return event.id

        except asyncio.QueueFull:
            logger.error(f"事件队列已满，无法发布事件 {event.name}")
            self._metrics['failed_events'] += 1
            # 执行错误钩子
            for hook in self._event_hooks['on_error']:
                try:
                    if asyncio.iscoroutinefunction(hook):
                        await hook(event, Exception("事件队列已满"))
                    else:
                        hook(event, Exception("事件队列已满"))
                except Exception as e:
                    logger.error(f"错误钩子执行失败: {e}")
            return None

    async def _worker_process(self):
        """事件处理工作线程"""
        while self._running:
            try:
                # 获取下一个事件
                _, event = await self._event_queue.get()

                # 处理事件
                await self._process_event(event)
            except Exception as e:
                logger.error(f"事件处理工作线程出错: {e}")

    async def _process_event(self, event: Event):
        """处理单个事件并分发到订阅者"""
        start_time = time.time()

        try:
            # 执行处理前钩子
            for hook in self._event_hooks['before_process']:
                try:
                    if asyncio.iscoroutinefunction(hook):
                        await hook(event)
                    else:
                        hook(event)
                except Exception as e:
                    logger.error(f"处理前钩子执行失败: {e}")

            # 获取此事件的分发模式
            dispatch_mode = self._dispatch_modes.get(
                event.name, self._default_dispatch_mode)

            # 获取所有匹配的订阅者（按优先级排序）
            handlers = []

            # 添加特定事件的处理器
            if event.name in self._subscribers:
                for priority in sorted(EventPriority, key=lambda p: p.value, reverse=True):
                    handlers.extend(self._subscribers[event.name][priority])

            # 添加通配符事件处理器
            for priority in sorted(EventPriority, key=lambda p: p.value, reverse=True):
                handlers.extend(self._wildcard_subscribers[priority])

            if not handlers:
                return

            # 根据分发模式处理事件
            if dispatch_mode == EventDispatchMode.PARALLEL:
                # 并行处理所有处理器
                async with self._worker_semaphore:
                    tasks = []
                    for handler in handlers:
                        task = self._execute_handler(handler, event)
                        tasks.append(task)

                    if tasks:
                        await asyncio.gather(*tasks, return_exceptions=True)

            elif dispatch_mode == EventDispatchMode.SEQUENTIAL:
                # 顺序处理所有处理器
                for handler in handlers:
                    await self._execute_handler(handler, event)

            elif dispatch_mode == EventDispatchMode.FIRST_MATCH:
                # 处理第一个成功的处理器
                for handler in handlers:
                    try:
                        result = await self._execute_handler(handler, event)
                        if result:  # 如果处理器返回True，表示已处理
                            break
                    except Exception as e:
                        logger.error(f"事件处理器执行失败: {e}")

            # 更新指标
            self._metrics['processed_events'] += 1
            processing_time = time.time() - start_time

            if event.name not in self._metrics['processing_times']:
                self._metrics['processing_times'][event.name] = []

            self._metrics['processing_times'][event.name].append(
                processing_time)
            # 保持最近100次处理时间
            if len(self._metrics['processing_times'][event.name]) > 100:
                self._metrics['processing_times'][event.name].pop(0)

            # 执行处理后钩子
            for hook in self._event_hooks['after_process']:
                try:
                    if asyncio.iscoroutinefunction(hook):
                        await hook(event)
                    else:
                        hook(event)
                except Exception as e:
                    logger.error(f"处理后钩子执行失败: {e}")

        except Exception as e:
            self._metrics['failed_events'] += 1
            logger.error(f"事件处理失败: {event.name}, {e}")
            logger.debug(traceback.format_exc())

            # 执行错误钩子
            for hook in self._event_hooks['on_error']:
                try:
                    if asyncio.iscoroutinefunction(hook):
                        await hook(event, e)
                    else:
                        hook(event, e)
                except Exception as hook_error:
                    logger.error(f"错误钩子执行失败: {hook_error}")
        finally:
            # 标记任务完成
            self._event_queue.task_done()

    async def _execute_handler(self, handler: Callable, event: Event) -> Any:
        """执行单个事件处理器"""
        self._active_handlers.add(handler)
        try:
            if asyncio.iscoroutinefunction(handler):
                return await handler(event)
            else:
                return handler(event)
        except Exception as e:
            logger.error(f"事件处理器执行出错: {e}")
            # 在调试模式下打印完整堆栈
            logger.debug(traceback.format_exc())
            raise
        finally:
            # 移除活动处理器引用
            if handler in self._active_handlers:
                self._active_handlers.remove(handler)

    async def wait_until_empty(self, timeout: Optional[float] = None) -> bool:
        """
        等待事件队列处理完毕

        Args:
            timeout: 超时时间（秒），None表示无限等待

        Returns:
            是否成功清空队列
        """
        if not self._running:
            return False

        try:
            await asyncio.wait_for(self._event_queue.join(), timeout)
            return True
        except asyncio.TimeoutError:
            return False

    def get_queue_size(self) -> int:
        """获取当前事件队列大小"""
        return self._event_queue.qsize()

    def get_metrics(self) -> Dict[str, Any]:
        """获取事件总线指标"""
        metrics = self._metrics.copy()

        # 计算平均处理时间
        avg_times = {}
        for event_name, times in self._metrics['processing_times'].items():
            if times:
                avg_times[event_name] = sum(times) / len(times)
        metrics['avg_processing_times'] = avg_times

        # 计算工作线程利用率
        workers_used = self._max_workers - self._worker_semaphore._value
        metrics['worker_utilization'] = workers_used / \
            self._max_workers if self._max_workers > 0 else 0

        # 添加订阅者统计
        metrics['subscribers'] = {
            'total': sum(
                sum(len(self._subscribers[event][priority])
                    for priority in EventPriority)
                for event in self._subscribers
            ) + sum(len(self._wildcard_subscribers[priority]) for priority in EventPriority),
            'events': {
                event: sum(len(handlers)
                           for handlers in self._subscribers[event].values())
                for event in self._subscribers
            },
            'wildcards': sum(len(self._wildcard_subscribers[priority]) for priority in EventPriority)
        }

        return metrics

    async def register_plugin_handlers(self, plugin_manager):
        """注册插件系统的事件处理器"""
        logger.info("注册插件系统的事件处理器")
        
        # 注册插件生命周期事件
        self.subscribe("plugin.loaded", self._on_plugin_loaded)
        self.subscribe("plugin.unloaded", self._on_plugin_unloaded)
        self.subscribe("plugin.error", self._on_plugin_error, EventPriority.HIGH)
        
        # 存储插件管理器引用
        self._plugin_manager = plugin_manager
        
    async def _on_plugin_loaded(self, event: Event):
        """处理插件加载事件"""
        plugin_name = event.data.get("name")
        logger.info(f"插件已加载: {plugin_name}")
        
    async def _on_plugin_unloaded(self, event: Event):
        """处理插件卸载事件"""
        plugin_name = event.data.get("name")
        logger.info(f"插件已卸载: {plugin_name}")
        
    async def _on_plugin_error(self, event: Event):
        """处理插件错误事件"""
        plugin_name = event.data.get("name")
        error = event.data.get("error")
        logger.error(f"插件错误 [{plugin_name}]: {error}")

    async def forward_to_message_bus(self, message_bus, event: Event):
        """将事件转发到消息总线"""
        try:
            # 将事件转换为消息格式
            topic = f"event.{event.name}"
            await message_bus.publish(topic, event.data)
            logger.debug(f"事件 {event.name} 转发到消息总线: {topic}")
        except Exception as e:
            logger.error(f"转发事件到消息总线失败: {e}")
            
    async def register_system_events(self):
        """注册系统核心事件"""
        # 这些是系统核心事件，应该由集成管理器或其他核心组件发布
        core_events = [
            "system.startup",
            "system.shutdown", 
            "system.error",
            "config.updated",
            "network.connected",
            "network.disconnected",
            "security.auth.success",
            "security.auth.failure"
        ]
        
        # 记录这些核心事件
        for event_name in core_events:
            if event_name not in self._subscribers:
                self._subscribers[event_name] = {p: [] for p in EventPriority}
                logger.debug(f"注册系统核心事件: {event_name}")
