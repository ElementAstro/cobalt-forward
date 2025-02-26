import asyncio
import time
import logging
import traceback
from typing import Dict, List, Any, Callable, Set, Tuple, Optional, Union
import json
from dataclasses import dataclass, field, asdict
import threading
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass
class Event:
    """事件类，用于插件系统中的事件传递"""
    name: str
    data: Any = None
    source: str = "system"
    timestamp: float = field(default_factory=time.time)
    id: str = field(default_factory=lambda: f"{time.time():.6f}")
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Event':
        """从字典创建事件"""
        return cls(**data)


class EventBus:
    """事件总线，提供事件分发和处理功能"""
    
    def __init__(self, max_queue_size: int = 1000, max_history: int = 100):
        self._handlers: Dict[str, List[Tuple[str, Callable]]] = {}
        self._subscribers: Dict[str, Set[str]] = {}
        self._event_queue = asyncio.Queue(maxsize=max_queue_size)
        self._event_history: List[Event] = []
        self._max_history = max_history
        
        self._lock = threading.RLock()
        self._processing = False
        self._processor_task: Optional[asyncio.Task] = None
        
        # 性能统计
        self._stats = {
            "events_processed": 0,
            "events_dropped": 0,
            "processing_errors": 0,
            "avg_processing_time": 0,
            "total_processing_time": 0,
            "handler_stats": {}
        }
    
    def subscribe(self, event_name: str, handler: Callable, subscriber_id: str):
        """订阅事件"""
        with self._lock:
            if event_name not in self._handlers:
                self._handlers[event_name] = []
                
            # 记录订阅者
            if event_name not in self._subscribers:
                self._subscribers[event_name] = set()
            self._subscribers[event_name].add(subscriber_id)
            
            # 添加处理器
            handler_tuple = (subscriber_id, handler)
            if handler_tuple not in self._handlers[event_name]:
                self._handlers[event_name].append(handler_tuple)
                
                # 初始化处理器统计
                if subscriber_id not in self._stats["handler_stats"]:
                    self._stats["handler_stats"][subscriber_id] = {
                        "total_calls": 0,
                        "errors": 0,
                        "avg_time": 0,
                        "total_time": 0
                    }
    
    def unsubscribe(self, event_name: str, subscriber_id: str):
        """取消订阅"""
        with self._lock:
            if event_name in self._handlers:
                self._handlers[event_name] = [
                    (sid, h) for sid, h in self._handlers[event_name] if sid != subscriber_id
                ]
                
            # 更新订阅者记录
            if event_name in self._subscribers:
                self._subscribers[event_name].discard(subscriber_id)
                if not self._subscribers[event_name]:
                    del self._subscribers[event_name]
    
    def unsubscribe_all(self, subscriber_id: str):
        """取消所有订阅"""
        with self._lock:
            for event_name in list(self._handlers.keys()):
                self.unsubscribe(event_name, subscriber_id)
    
    async def emit(self, event: Union[Event, str], data: Any = None, source: str = "system") -> bool:
        """发布事件"""
        try:
            if isinstance(event, str):
                event = Event(name=event, data=data, source=source)
            
            # 添加到事件历史
            with self._lock:
                self._event_history.append(event)
                if len(self._event_history) > self._max_history:
                    self._event_history = self._event_history[-self._max_history:]
            
            # 如果队列已满，丢弃事件
            try:
                await asyncio.wait_for(
                    self._event_queue.put(event),
                    timeout=0.5
                )
                return True
            except asyncio.TimeoutError:
                logger.warning(f"事件队列已满，丢弃事件: {event.name}")
                with self._lock:
                    self._stats["events_dropped"] += 1
                return False
                
        except Exception as e:
            logger.error(f"发布事件失败: {e}")
            return False
    
    async def emit_batch(self, events: List[Union[Event, Tuple[str, Any, str]]]) -> int:
        """批量发布事件"""
        success_count = 0
        for event in events:
            if isinstance(event, tuple):
                event_name, data, source = event
                if await self.emit(event_name, data, source):
                    success_count += 1
            else:
                if await self.emit(event):
                    success_count += 1
        return success_count
    
    async def start_processing(self):
        """开始处理事件队列"""
        if self._processing:
            return
            
        self._processing = True
        self._processor_task = asyncio.create_task(self._process_events())
        logger.info("事件总线处理器已启动")
    
    async def stop_processing(self):
        """停止处理事件队列"""
        if self._processor_task:
            self._processing = False
            self._processor_task.cancel()
            try:
                await self._processor_task
            except asyncio.CancelledError:
                pass
            self._processor_task = None
            logger.info("事件总线处理器已停止")
    
    async def _process_events(self):
        """处理事件队列的任务"""
        while self._processing:
            try:
                event = await self._event_queue.get()
                start_time = time.time()
                
                await self._process_single_event(event)
                
                processing_time = time.time() - start_time
                with self._lock:
                    self._stats["events_processed"] += 1
                    self._stats["total_processing_time"] += processing_time
                    self._stats["avg_processing_time"] = (
                        self._stats["total_processing_time"] / 
                        self._stats["events_processed"]
                    )
                
                self._event_queue.task_done()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"事件处理错误: {e}\n{traceback.format_exc()}")
                with self._lock:
                    self._stats["processing_errors"] += 1
    
    async def _process_single_event(self, event: Event):
        """处理单个事件"""
        if event.name in self._handlers:
            handlers = list(self._handlers[event.name])
            
            # 并发执行所有处理器
            tasks = []
            for subscriber_id, handler in handlers:
                tasks.append(self._execute_handler(event, handler, subscriber_id))
            
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _execute_handler(self, event: Event, handler: Callable, subscriber_id: str):
        """执行单个事件处理器"""
        start_time = time.time()
        try:
            if asyncio.iscoroutinefunction(handler):
                await handler(event)
            else:
                handler(event)
                
            # 更新统计信息
            execution_time = time.time() - start_time
            with self._lock:
                stats = self._stats["handler_stats"].get(subscriber_id, {
                    "total_calls": 0,
                    "errors": 0,
                    "avg_time": 0,
                    "total_time": 0
                })
                
                stats["total_calls"] += 1
                stats["total_time"] += execution_time
                stats["avg_time"] = stats["total_time"] / stats["total_calls"]
                self._stats["handler_stats"][subscriber_id] = stats
                
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"事件处理器错误 (事件: {event.name}, 处理器: {subscriber_id}): {e}")
            
            # 更新错误统计
            with self._lock:
                stats = self._stats["handler_stats"].get(subscriber_id, {
                    "total_calls": 0,
                    "errors": 0,
                    "avg_time": 0,
                    "total_time": 0
                })
                
                stats["total_calls"] += 1
                stats["errors"] += 1
                stats["total_time"] += execution_time
                stats["avg_time"] = stats["total_time"] / stats["total_calls"]
                self._stats["handler_stats"][subscriber_id] = stats
    
    def get_stats(self) -> Dict[str, Any]:
        """获取事件总线统计信息"""
        with self._lock:
            return {
                "events_processed": self._stats["events_processed"],
                "events_dropped": self._stats["events_dropped"],
                "processing_errors": self._stats["processing_errors"],
                "avg_processing_time": self._stats["avg_processing_time"],
                "subscribers_count": len(self._stats["handler_stats"]),
                "event_types_count": len(self._handlers),
                "queue_size": self._event_queue.qsize(),
                "queue_maxsize": self._event_queue.maxsize,
                "history_size": len(self._event_history)
            }
    
    def get_subscriber_stats(self, subscriber_id: str = None) -> Dict[str, Any]:
        """获取订阅者统计信息"""
        with self._lock:
            if subscriber_id:
                return self._stats["handler_stats"].get(subscriber_id, {})
            else:
                return self._stats["handler_stats"]
    
    def get_event_history(self, limit: int = None, event_name: str = None) -> List[Dict[str, Any]]:
        """获取事件历史"""
        with self._lock:
            if event_name:
                filtered_history = [event.to_dict() for event in self._event_history if event.name == event_name]
            else:
                filtered_history = [event.to_dict() for event in self._event_history]
                
            if limit is not None and limit > 0:
                return filtered_history[-limit:]
            return filtered_history
    
    def get_subscribers(self, event_name: str = None) -> Dict[str, List[str]]:
        """获取订阅者列表"""
        with self._lock:
            if event_name:
                return {event_name: list(self._subscribers.get(event_name, set()))}
            return {event_name: list(subscribers) for event_name, subscribers in self._subscribers.items()}
    
    def clear_history(self):
        """清除事件历史"""
        with self._lock:
            self._event_history = []
    
    def reset_stats(self):
        """重置统计信息"""
        with self._lock:
            self._stats = {
                "events_processed": 0,
                "events_dropped": 0,
                "processing_errors": 0,
                "avg_processing_time": 0,
                "total_processing_time": 0,
                "handler_stats": {}
            }
            
    async def add_delayed_event(self, event: Union[Event, str], delay: float, data: Any = None, source: str = "system") -> bool:
        """添加延迟事件，在指定时间后触发"""
        async def delayed_emit():
            await asyncio.sleep(delay)
            return await self.emit(event, data, source)
            
        # 创建并启动任务
        task = asyncio.create_task(delayed_emit())
        return True
        
    async def emit_periodic(self, event_name: str, interval: float, data_generator: Callable = None, 
                     source: str = "system", max_count: int = None) -> asyncio.Task:
        """发出周期性事件，可选择使用数据生成器函数"""
        async def periodic_emitter():
            count = 0
            while max_count is None or count < max_count:
                try:
                    if data_generator:
                        if asyncio.iscoroutinefunction(data_generator):
                            data = await data_generator()
                        else:
                            data = data_generator()
                    else:
                        data = {"timestamp": time.time(), "count": count}
                    
                    await self.emit(event_name, data, source)
                    count += 1
                    await asyncio.sleep(interval)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"周期性事件发射器错误: {e}")
                    await asyncio.sleep(interval)  # 即使出错也继续
        
        # 创建并启动任务
        task = asyncio.create_task(periodic_emitter())
        return task