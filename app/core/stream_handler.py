import asyncio
import json
import logging
import time
import uuid
from typing import Any, Dict, List, Optional, Set, Callable, AsyncGenerator, Coroutine
from enum import Enum, auto
from dataclasses import dataclass, field

from app.core.base import BaseComponent
from app.utils.error_handler import error_boundary

logger = logging.getLogger(__name__)


class StreamType(Enum):
    """流类型枚举"""
    TEXT = auto()           # 文本流
    BINARY = auto()         # 二进制流
    JSON = auto()           # JSON流
    EVENT = auto()          # 事件流
    MIXED = auto()          # 混合类型流


class StreamDirection(Enum):
    """流方向枚举"""
    INPUT = auto()          # 输入流
    OUTPUT = auto()         # 输出流
    BIDIRECTIONAL = auto()  # 双向流


@dataclass
class StreamEvent:
    """流事件基类"""
    stream_id: str
    event_type: str
    timestamp: float = field(default_factory=time.time)
    data: Any = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class StreamInfo:
    """流信息"""
    stream_id: str
    stream_type: StreamType
    direction: StreamDirection
    name: str
    created_at: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)
    is_active: bool = True
    total_bytes: int = 0
    total_messages: int = 0
    last_activity: float = field(default_factory=time.time)
    error_count: int = 0


class DataFragmentType(Enum):
    """数据片段类型枚举"""
    START = auto()          # 开始标记
    DATA = auto()           # 数据片段
    END = auto()            # 结束标记
    ERROR = auto()          # 错误
    HEARTBEAT = auto()      # 心跳


@dataclass
class DataFragment:
    """数据片段"""
    fragment_id: str
    stream_id: str
    fragment_type: DataFragmentType
    data: Any = None
    timestamp: float = field(default_factory=time.time)
    sequence: int = 0
    is_last: bool = False
    error: Optional[str] = None


class StreamHandler(BaseComponent):
    """
    流处理器，用于处理各种类型的数据流
    
    功能：
    1. 支持创建和管理多种类型的数据流
    2. 提供流式数据处理和传输能力
    3. 支持流数据分块和重组
    4. 实现流量控制和背压
    5. 支持实时流处理中间件
    """
    
    def __init__(self, max_buffer_size: int = 1024 * 1024,
                 chunk_size: int = 8192,
                 heartbeat_interval: int = 30,
                 name: str = "stream_handler"):
        """
        初始化流处理器
        
        Args:
            max_buffer_size: 每个流的最大缓冲区大小（字节）
            chunk_size: 默认数据分块大小（字节）
            heartbeat_interval: 心跳间隔（秒）
            name: 组件名称
        """
        super().__init__(name=name)
        self._max_buffer_size = max_buffer_size
        self._chunk_size = chunk_size
        self._heartbeat_interval = heartbeat_interval
        self._streams: Dict[str, StreamInfo] = {}
        self._stream_queues: Dict[str, asyncio.Queue[DataFragment]] = {}
        self._event_listeners: Dict[str, List[Callable[..., Any]]] = {}
        self._active_streams: Set[str] = set()
        self._stream_tasks: Dict[str, asyncio.Task[None]] = {}
        self._heartbeat_task: Optional[asyncio.Task[None]] = None
        self._middleware: List[Callable[..., Any]] = []
        self._transformers: Dict[str, List[Callable[..., Any]]] = {}
        self._message_bus: Optional[Any] = None # Consider a MessageBus protocol/interface
        self._closed = False
    
    async def _start_impl(self) -> None:
        """启动流处理器"""
        self._closed = False
        self._heartbeat_task = asyncio.create_task(self._heartbeat_monitor())
        logger.info(f"流处理器启动，心跳间隔: {self._heartbeat_interval}秒")
    
    async def _stop_impl(self) -> None:
        """停止流处理器"""
        self._closed = True
        
        # 取消心跳监控任务
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            self._heartbeat_task = None # Clear the task
        
        # 关闭所有活跃流
        for stream_id in list(self._active_streams):
            await self.close_stream(stream_id)
        
        # 取消所有流处理任务
        tasks_to_await = [task for task in self._stream_tasks.values() if task and not task.done()]
        for task in tasks_to_await:
            task.cancel()
        
        if tasks_to_await:
            await asyncio.gather(*tasks_to_await, return_exceptions=True)
        self._stream_tasks.clear()
        
        logger.info("流处理器已停止")
    
    async def _heartbeat_monitor(self) -> None:
        """心跳监控任务"""
        while not self._closed:
            try:
                now = time.time()
                
                # 检查每个活跃流并发送心跳
                for stream_id in list(self._active_streams): # Iterate over a copy
                    if stream_id not in self._streams:
                        if stream_id in self._active_streams: # Check again before removing
                             self._active_streams.remove(stream_id)
                        continue
                    
                    stream_info = self._streams[stream_id]
                    
                    # 如果流已经很久没有活动，发送心跳
                    if now - stream_info.last_activity > self._heartbeat_interval:
                        # 创建心跳数据片段
                        heartbeat = DataFragment(
                            fragment_id=str(uuid.uuid4()),
                            stream_id=stream_id,
                            fragment_type=DataFragmentType.HEARTBEAT,
                            timestamp=now,
                            sequence=-1 # Or a specific sequence for heartbeats
                        )
                        
                        # 将心跳放入流队列
                        if stream_id in self._stream_queues:
                            try:
                                self._stream_queues[stream_id].put_nowait(heartbeat)
                                logger.debug(f"发送心跳到流: {stream_id}")
                            except asyncio.QueueFull:
                                logger.warning(f"流队列已满，无法发送心跳: {stream_id}")
                        
                        # 更新流活动时间（即使队列满了，也更新时间）
                        stream_info.last_activity = now
                
                # 等待下一个检查周期
                await asyncio.sleep(self._heartbeat_interval)
                
            except asyncio.CancelledError:
                logger.info("心跳监控任务被取消")
                break
            except Exception as e:
                logger.error(f"心跳监控任务出错: {e}", exc_info=True)
                await asyncio.sleep(5)  # 发生错误时短暂休眠
    
    async def create_stream(self, stream_type: StreamType, direction: StreamDirection,
                          name: str, metadata: Optional[Dict[str, Any]] = None) -> str:
        """
        创建新的数据流
        
        Args:
            stream_type: 流类型
            direction: 流方向
            name: 流名称
            metadata: 元数据
            
        Returns:
            流ID
        """
        # 生成流ID
        stream_id = str(uuid.uuid4())
        
        # 创建流信息
        stream_info = StreamInfo(
            stream_id=stream_id,
            stream_type=stream_type,
            direction=direction,
            name=name,
            metadata=metadata or {}
        )
        
        # 创建流队列
        self._stream_queues[stream_id] = asyncio.Queue(maxsize=100)
        
        # 存储流信息
        self._streams[stream_id] = stream_info
        self._active_streams.add(stream_id)
        
        # 触发流创建事件
        await self._trigger_event("stream_created", stream_id, stream_info=stream_info)
        
        logger.info(f"创建流: {stream_id}, 类型: {stream_type.name}, 方向: {direction.name}, 名称: {name}")
        
        return stream_id
    
    async def write_to_stream(self, stream_id: str, data: Any,
                            is_last: bool = False) -> bool:
        """
        写入数据到流
        
        Args:
            stream_id: 流ID
            data: 要写入的数据
            is_last: 是否是最后一个数据片段
            
        Returns:
            是否成功写入
        """
        if stream_id not in self._streams or stream_id not in self._stream_queues:
            logger.warning(f"写入流失败：流不存在: {stream_id}")
            return False
        
        stream_info = self._streams[stream_id]
        
        if not stream_info.is_active:
            logger.warning(f"写入流失败：流已关闭: {stream_id}")
            return False
        
        if stream_info.direction == StreamDirection.INPUT:
            logger.warning(f"写入流失败：流是输入流，不能写入: {stream_id}")
            return False
        
        current_data = data
        try:
            # 应用流变换器
            if stream_id in self._transformers:
                for transformer in self._transformers[stream_id]:
                    try:
                        if asyncio.iscoroutinefunction(transformer):
                            current_data = await transformer(current_data)
                        else:
                            current_data = transformer(current_data)
                    except Exception as e:
                        logger.error(f"流变换器 {transformer.__name__ if hasattr(transformer, '__name__') else transformer} 失败 for stream {stream_id}: {e}", exc_info=True)
                        # Decide if this error should stop the write or be propagated
            
            # 创建数据片段
            fragment = DataFragment(
                fragment_id=str(uuid.uuid4()),
                stream_id=stream_id,
                fragment_type=DataFragmentType.DATA,
                data=current_data,
                sequence=stream_info.total_messages,
                is_last=is_last
            )
            
            # 将数据片段放入队列
            await self._stream_queues[stream_id].put(fragment)
            
            # 更新流信息
            stream_info.total_messages += 1
            stream_info.last_activity = time.time()
            
            # 计算数据大小
            data_size = 0
            if isinstance(current_data, (str, bytes)):
                data_size = len(current_data)
            elif isinstance(current_data, dict) or isinstance(current_data, list):
                try:
                    data_size = len(json.dumps(current_data))
                except TypeError:
                    data_size = 0 # Or some other default for non-serializable data
            
            stream_info.total_bytes += data_size
            
            # 如果是最后一个片段，发送结束事件
            if is_last:
                end_fragment = DataFragment(
                    fragment_id=str(uuid.uuid4()),
                    stream_id=stream_id,
                    fragment_type=DataFragmentType.END,
                    sequence=stream_info.total_messages # Sequence for END can be same as last DATA or +1
                )
                await self._stream_queues[stream_id].put(end_fragment)
                stream_info.is_active = False # Mark as inactive, actual removal from active_streams in close_stream
                
                # 触发流结束事件
                await self._trigger_event("stream_ended", stream_id, stream_info=stream_info)
                
                logger.info(f"流结束: {stream_id}, 总消息数: {stream_info.total_messages}, 总字节数: {stream_info.total_bytes}")
            
            return True
            
        except Exception as e:
            logger.error(f"写入流 {stream_id} 失败: {e}", exc_info=True)
            stream_info.error_count += 1
            
            # 触发流错误事件
            await self._trigger_event("stream_error", stream_id, stream_info=stream_info, error=str(e))
            
            return False
    
    async def write_error_to_stream(self, stream_id: str, error: str) -> bool:
        """
        写入错误到流
        
        Args:
            stream_id: 流ID
            error: 错误信息
            
        Returns:
            是否成功写入
        """
        if stream_id not in self._streams or stream_id not in self._stream_queues:
            logger.warning(f"写入错误失败：流不存在: {stream_id}")
            return False
        
        stream_info = self._streams[stream_id]
        
        if not stream_info.is_active: # Allow writing error even if marked inactive by is_last
            logger.warning(f"写入错误警告：流已标记为非活动: {stream_id}")
            # Continue to allow error propagation
        
        try:
            # 创建错误片段
            error_fragment = DataFragment(
                fragment_id=str(uuid.uuid4()),
                stream_id=stream_id,
                fragment_type=DataFragmentType.ERROR,
                error=error,
                sequence=stream_info.total_messages # Or a specific sequence for errors
            )
            
            # 将错误片段放入队列
            await self._stream_queues[stream_id].put(error_fragment)
            
            # 更新流信息
            stream_info.total_messages += 1 # Count error as a message
            stream_info.last_activity = time.time()
            stream_info.error_count += 1
            
            # 触发流错误事件
            await self._trigger_event("stream_error", stream_id, stream_info=stream_info, error=error)
            
            logger.warning(f"流错误写入: {stream_id}, 错误: {error}")
            
            return True
            
        except Exception as e:
            logger.error(f"写入错误到流 {stream_id} 失败: {e}", exc_info=True)
            return False
    
    async def read_from_stream(self, stream_id: str) -> AsyncGenerator[Any, None]:
        """
        从流中读取数据
        
        Args:
            stream_id: 流ID
            
        Returns:
            数据生成器
        """
        if stream_id not in self._streams or stream_id not in self._stream_queues:
            logger.warning(f"读取流失败：流不存在: {stream_id}")
            return # Empty async generator
        
        stream_info = self._streams[stream_id]
        queue = self._stream_queues[stream_id]
        
        if stream_info.direction == StreamDirection.OUTPUT:
            logger.warning(f"读取流失败：流是输出流，不能读取: {stream_id}")
            return # Empty async generator
        
        try:
            # Loop while stream is active or queue has items (even if marked inactive after END)
            while stream_info.is_active or not queue.empty():
                try:
                    # From队列获取下一个数据片段
                    fragment: DataFragment = await asyncio.wait_for(queue.get(), timeout=1.0) # Add timeout to prevent indefinite blocking if stream logic is flawed
                except asyncio.TimeoutError:
                    if not stream_info.is_active and queue.empty(): # Check again if stream ended during timeout
                        break
                    continue # Continue waiting if stream is still supposed to be active

                # 更新流活动时间
                stream_info.last_activity = time.time()
                
                # 处理不同类型的片段
                if fragment.fragment_type == DataFragmentType.DATA:
                    # 产生实际数据
                    yield fragment.data
                    
                    # 如果是最后一个片段，标记流结束 (actual closing handled by END or close_stream)
                    if fragment.is_last:
                        stream_info.is_active = False # Consumer sees end of data
                
                elif fragment.fragment_type == DataFragmentType.ERROR:
                    # 产生错误信息
                    yield {"error": fragment.error, "stream_id": stream_id} # Include stream_id for context
                    # Optionally, could mark stream_info.is_active = False here depending on error handling strategy
                
                elif fragment.fragment_type == DataFragmentType.END:
                    # 流结束
                    stream_info.is_active = False
                    queue.task_done() # Mark END fragment as processed
                    break # Exit loop on END fragment
                
                elif fragment.fragment_type == DataFragmentType.HEARTBEAT:
                    # 心跳，不需要传递给消费者
                    pass
                
                # 标记任务完成
                queue.task_done()
                
        except asyncio.CancelledError:
            # 读取被取消
            logger.info(f"读取流 {stream_id} 被取消")
            
        except Exception as e:
            # 读取出错
            logger.error(f"读取流 {stream_id} 出错: {e}", exc_info=True)
            stream_info.error_count += 1
            
            # 触发流错误事件
            await self._trigger_event("stream_error", stream_id, stream_info=stream_info, error=str(e))
            
            # 产生错误
            yield {"error": str(e), "stream_id": stream_id} # Include stream_id for context
        finally:
            # Ensure stream is marked inactive if loop exits unexpectedly while active
            if stream_info.is_active:
                 logger.warning(f"Stream {stream_id} reader exited while stream still marked active.")
                 # stream_info.is_active = False # Consider implications
    
    async def close_stream(self, stream_id: str) -> bool:
        """
        关闭流
        
        Args:
            stream_id: 流ID
            
        Returns:
            是否成功关闭
        """
        if stream_id not in self._streams:
            logger.warning(f"关闭流失败：流不存在: {stream_id}")
            return False
        
        stream_info = self._streams[stream_id]
        
        # Idempotency: if already marked inactive by an END fragment or previous close.
        # However, we still want to ensure tasks are cancelled and events are triggered.
        # if not stream_info.is_active and stream_id not in self._active_streams:
        # logger.info(f"流已经关闭: {stream_id}")
        # return True

        logger.info(f"Attempting to close stream: {stream_id}, current active status: {stream_info.is_active}")

        try:
            # 更新流状态
            stream_info.is_active = False
            
            # 如果有待处理的流任务，取消它
            if stream_id in self._stream_tasks:
                task = self._stream_tasks[stream_id]
                if task and not task.done():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        logger.debug(f"Stream task for {stream_id} cancelled.")
                    except Exception as e_task:
                        logger.error(f"Error awaiting cancelled stream task {stream_id}: {e_task}", exc_info=True)
                del self._stream_tasks[stream_id] # Remove after handling
            
            # 从活跃流集合中移除
            if stream_id in self._active_streams:
                self._active_streams.remove(stream_id)
            
            # Optionally, clear the queue or add a final "closed" marker if consumers might still be reading
            # For now, relying on is_active and END/ERROR markers in read_from_stream

            # 触发流关闭事件
            await self._trigger_event("stream_closed", stream_id, stream_info=stream_info)
            
            logger.info(f"流关闭完成: {stream_id}")
            
            return True
            
        except Exception as e:
            logger.error(f"关闭流 {stream_id} 失败: {e}", exc_info=True)
            # Ensure it's marked inactive and removed from active_streams even on error
            stream_info.is_active = False
            if stream_id in self._active_streams:
                self._active_streams.remove(stream_id)
            return False
    
    def add_transformer(self, stream_id: str, transformer: Callable[..., Any]) -> None:
        """
        添加流数据转换器
        
        Args:
            stream_id: 流ID
            transformer: 转换器函数
        """
        if stream_id not in self._transformers:
            self._transformers[stream_id] = []
        
        self._transformers[stream_id].append(transformer)
        logger.debug(f"添加转换器到流: {stream_id}")
    
    def add_middleware(self, middleware: Callable[..., Any]) -> None:
        """
        添加流处理中间件
        
        Args:
            middleware: 中间件函数
        """
        self._middleware.append(middleware)
        logger.debug(f"添加流处理中间件: {middleware.__name__ if hasattr(middleware, '__name__') else middleware}")
    
    def add_event_listener(self, event_type: str, listener: Callable[..., Any]) -> None:
        """
        添加流事件监听器
        
        Args:
            event_type: 事件类型 ('stream_created', 'stream_ended', 'stream_error', 'stream_closed')
            listener: 监听器函数
        """
        if event_type not in self._event_listeners:
            self._event_listeners[event_type] = []
        
        self._event_listeners[event_type].append(listener)
        logger.debug(f"添加流事件监听器: {event_type} - {listener.__name__ if hasattr(listener, '__name__') else listener}")
    
    async def _trigger_event(self, event_type: str, stream_id: str, 
                           stream_info: Optional[StreamInfo] = None, **kwargs: Any) -> None:
        """触发流事件"""
        # 调用注册的监听器
        for listener in self._event_listeners.get(event_type, []):
            try:
                # Prepare args for listener, ensuring stream_info is passed correctly
                listener_args = {"stream_id": stream_id, "stream_info": stream_info, **kwargs}
                
                if asyncio.iscoroutinefunction(listener):
                    await listener(**listener_args)
                else:
                    listener(**listener_args)
            except Exception as e:
                logger.error(f"执行流事件监听器 {listener.__name__ if hasattr(listener, '__name__') else listener} for event {event_type} on stream {stream_id} 失败: {e}", exc_info=True)
        
        # 发布到消息总线
        if self._message_bus:
            try:
                event_data: Dict[str, Any] = {
                    "stream_id": stream_id,
                    "event_type": event_type, # Add event_type to data
                    "timestamp": time.time()
                }
                
                if stream_info:
                    event_data.update({
                        "name": stream_info.name,
                        "type": stream_info.stream_type.name,
                        "direction": stream_info.direction.name,
                        "is_active": stream_info.is_active,
                        "total_messages": stream_info.total_messages,
                        "total_bytes": stream_info.total_bytes,
                        "metadata": stream_info.metadata # Include metadata
                    })
                
                event_data.update(kwargs) # Add any other kwargs
                
                # Ensure _message_bus has a publish method (runtime check if not using a protocol)
                if hasattr(self._message_bus, 'publish') and callable(self._message_bus.publish):
                    await self._message_bus.publish(f"stream.{event_type}", event_data)
                else:
                    logger.error(f"消息总线对象 for stream {stream_id} 没有 'publish' 方法或不可调用.")
            except Exception as e:
                logger.error(f"发布流事件 {event_type} for stream {stream_id} 到消息总线失败: {e}", exc_info=True)
    
    async def get_stream_info(self, stream_id: str) -> Optional[StreamInfo]:
        """
        获取流信息
        
        Args:
            stream_id: 流ID
            
        Returns:
            流信息，不存在则返回None
        """
        return self._streams.get(stream_id)
    
    async def list_streams(self, active_only: bool = False) -> List[StreamInfo]:
        """
        列出所有流
        
        Args:
            active_only: 是否只列出活跃的流
            
        Returns:
            流信息列表
        """
        if active_only:
            return [stream_info for stream_id, stream_info in self._streams.items() if stream_id in self._active_streams and stream_info.is_active]
        return list(self._streams.values())
    
    @error_boundary() # Assuming error_boundary is correctly defined and typed elsewhere
    async def process_stream(self, stream_id: str, processor: Callable[..., Any]) -> None:
        """
        处理流数据
        
        Args:
            stream_id: 流ID
            processor: 处理器函数，接收流数据并返回处理结果
        """
        if stream_id not in self._streams:
            logger.warning(f"处理流失败：流不存在: {stream_id}")
            return
        
        stream_info = self._streams[stream_id]
        
        if not stream_info.is_active and self._stream_queues.get(stream_id, asyncio.Queue()).empty(): # Check if truly inactive
            logger.warning(f"处理流警告：流已关闭且队列为空: {stream_id}")
            return # Don't start processor if stream is already done
        
        if stream_id in self._stream_tasks and not self._stream_tasks[stream_id].done():
            logger.warning(f"处理流警告：流 {stream_id} 已有处理任务在运行.")
            return


        logger.info(f"Starting processor for stream {stream_id}")
        try:
            # 创建处理任务
            task = asyncio.create_task(self._run_processor(stream_id, processor))
            self._stream_tasks[stream_id] = task
            
            # 等待任务完成
            await task
            
        except asyncio.CancelledError:
            logger.info(f"流处理任务被取消: {stream_id}")
            if stream_info: stream_info.is_active = False # Ensure marked inactive
            
        except Exception as e:
            logger.error(f"处理流 {stream_id} 失败: {e}", exc_info=True)
            if stream_info:
                stream_info.error_count += 1
                stream_info.is_active = False # Mark inactive on error
            
            # 触发流错误事件
            await self._trigger_event("stream_error", stream_id, stream_info=stream_info, error=str(e))
            
        finally:
            # 从任务字典中移除
            if stream_id in self._stream_tasks:
                del self._stream_tasks[stream_id]
            logger.info(f"Processor for stream {stream_id} finished.")
    
    async def _run_processor(self, stream_id: str, processor: Callable[..., Any]) -> None:
        """运行流处理器"""
        processed_data_count = 0
        stream_info = self._streams.get(stream_id)
        if not stream_info:
            logger.error(f"Cannot run processor: StreamInfo not found for {stream_id}")
            return

        try:
            async for data_item in self.read_from_stream(stream_id):
                current_data_item: Any = data_item
                # 应用中间件
                for mw in self._middleware:
                    try:
                        mw_name = mw.__name__ if hasattr(mw, '__name__') else str(mw)
                        if asyncio.iscoroutinefunction(mw):
                            current_data_item = await mw(current_data_item, stream_id=stream_id)
                        else:
                            current_data_item = mw(current_data_item, stream_id=stream_id)
                    except Exception as e_mw:
                        logger.error(f"流中间件 {mw_name} 处理失败 for stream {stream_id}: {e_mw}", exc_info=True)
                        # Decide: skip item, error stream, or continue
                
                # 应用处理器
                try:
                    processor_name = processor.__name__ if hasattr(processor, '__name__') else str(processor)
                    result: Any
                    if asyncio.iscoroutinefunction(processor):
                        result = await processor(current_data_item)
                    else:
                        result = processor(current_data_item)
                    processed_data_count +=1
                    
                    # 如果处理器返回了结果，写入到输出流
                    if result is not None:
                        # 查找或创建与输入流相关联的输出流
                        output_stream_id = stream_info.metadata.get("output_stream_id")
                        
                        if isinstance(output_stream_id, str) and output_stream_id in self._streams:
                            await self.write_to_stream(output_stream_id, result)
                        elif output_stream_id:
                             logger.warning(f"Output stream ID {output_stream_id} for input {stream_id} not found or invalid.")
                            
                except Exception as e_proc:
                    logger.error(f"流处理器 {processor_name} 执行失败 for stream {stream_id}: {e_proc}", exc_info=True)
                    await self.write_error_to_stream(stream_id, f"Processor error: {e_proc}")
                    # Decide if processor error should stop the whole stream processing
                    # stream_info.is_active = False # Optionally stop stream on processor error
                    # break # Exit processing loop
            logger.info(f"Processor for stream {stream_id} completed processing {processed_data_count} items.")
        except Exception as e_outer:
            logger.error(f"Outer loop of _run_processor for stream {stream_id} failed: {e_outer}", exc_info=True)
            await self.write_error_to_stream(stream_id, f"Stream processing loop error: {e_outer}")
        finally:
            # Ensure stream is marked as ended if not already, especially if processor loop finishes
            if stream_info and stream_info.is_active:
                logger.info(f"Processor for {stream_id} finished, but stream was still active. Marking as inactive.")
                # This might indicate that read_from_stream didn't see an END or was cancelled.
                # Consider sending an END fragment if appropriate, or just marking inactive.
                # await self.write_to_stream(stream_id, None, is_last=True) # If an explicit end is needed
                stream_info.is_active = False
                await self._trigger_event("stream_ended", stream_id, stream_info=stream_info, reason="Processor finished")


    async def create_pipe(self, input_name: str, output_name: str,
                        stream_type: StreamType = StreamType.TEXT,
                        metadata: Optional[Dict[str, Any]] = None) -> Dict[str, str]:
        """
        创建双向管道（输入流和输出流）
        
        Args:
            input_name: 输入流名称
            output_name: 输出流名称
            stream_type: 流类型
            metadata: 元数据
            
        Returns:
            含有输入和输出流ID的字典
        """
        # 创建元数据副本
        current_metadata = metadata or {}
        
        # 创建输入流
        input_stream_id = await self.create_stream(
            stream_type=stream_type,
            direction=StreamDirection.INPUT,
            name=input_name,
            metadata=current_metadata.copy() # Pass a copy for each stream
        )
        
        # 创建输出流
        output_stream_id = await self.create_stream(
            stream_type=stream_type,
            direction=StreamDirection.OUTPUT,
            name=output_name,
            metadata=current_metadata.copy() # Pass a copy for each stream
        )
        
        # 关联两个流
        if input_stream_id in self._streams:
            self._streams[input_stream_id].metadata["output_stream_id"] = output_stream_id
        if output_stream_id in self._streams:
            self._streams[output_stream_id].metadata["input_stream_id"] = input_stream_id
        
        return {
            "input_stream_id": input_stream_id,
            "output_stream_id": output_stream_id
        }
    
    async def copy_stream(self, source_id: str, target_id: str) -> None:
        """
        复制一个流的数据到另一个流
        
        Args:
            source_id: 源流ID
            target_id: 目标流ID
        """
        if source_id not in self._streams:
            logger.warning(f"复制流失败：源流不存在: {source_id}")
            return
            
        if target_id not in self._streams:
            logger.warning(f"复制流失败：目标流不存在: {target_id}")
            return
            
        source_info = self._streams[source_id]
        target_info = self._streams[target_id]
        
        # Allow copying from an inactive (but potentially queued) stream
        # if not source_info.is_active:
        # logger.warning(f"复制流警告：源流已关闭: {source_id}")
        # return # Or allow copying remaining data
            
        if not target_info.is_active:
            logger.warning(f"复制流失败：目标流已关闭: {target_id}")
            return
            
        copy_task_name = f"copy_{source_id}_to_{target_id}"
        if copy_task_name in self._stream_tasks and not self._stream_tasks[copy_task_name].done():
            logger.warning(f"Copy task {copy_task_name} is already running.")
            return

        # 创建复制任务
        async def _copy_task_impl():
            logger.info(f"Copy task started: from {source_id} to {target_id}")
            items_copied = 0
            try:
                async for data in self.read_from_stream(source_id):
                    # 写入数据到目标流
                    # Check if data is an error dict from read_from_stream
                    if isinstance(data, dict) and "error" in data and data.get("stream_id") == source_id:
                        await self.write_error_to_stream(target_id, str(data["error"]))
                    else:
                        await self.write_to_stream(target_id, data)
                    items_copied +=1
                # After loop, if source stream ended, ensure target stream also gets an END if not already sent by write_to_stream
                # This is tricky because read_from_stream yields data, not fragments.
                # The is_last logic in write_to_stream should handle ending the target stream.
                logger.info(f"Copy task finished: {items_copied} items copied from {source_id} to {target_id}")

            except Exception as e_copy:
                logger.error(f"Error during copy from {source_id} to {target_id}: {e_copy}", exc_info=True)
                await self.write_error_to_stream(target_id, f"Copy error: {e_copy}")
            finally:
                if copy_task_name in self._stream_tasks:
                    del self._stream_tasks[copy_task_name]
                # Ensure target stream is properly closed if source stream ended.
                # This might require more sophisticated end-of-stream signaling.
                # For now, assume write_to_stream with is_last=True or an explicit close handles it.
                if source_info and not source_info.is_active and target_info and target_info.is_active:
                    logger.info(f"Source stream {source_id} ended, ensuring target stream {target_id} is also ended.")
                    # Send an explicit END if not already handled by is_last in write_to_stream
                    # This requires knowing if the last write was indeed the last.
                    # A simpler approach might be to just close it if the source is done.
                    await self.close_stream(target_id)


        task = asyncio.create_task(_copy_task_impl())
        self._stream_tasks[copy_task_name] = task
        
        logger.info(f"开始从流 {source_id} 复制到流 {target_id}")
    
    async def set_message_bus(self, message_bus: Any) -> None: # Consider a MessageBus protocol/interface
        """设置消息总线引用"""
        self._message_bus = message_bus
        logger.info("消息总线已与流处理器集成")
    
    async def handle_message(self, topic: str, data: Any) -> None:
        """处理接收到的消息"""
        if not isinstance(data, dict):
            logger.warning(f"接收到的消息数据不是字典类型 for topic {topic}: {type(data)}")
            return

        if topic.startswith("stream.command."):
            command = topic.split(".")[-1]
            response_topic_base = f"stream.response.{command}"
            
            try:
                if command == "create":
                    if all(k in data for k in ["name", "type", "direction"]):
                        try:
                            stream_type_str = str(data["type"]).upper()
                            stream_type = StreamType[stream_type_str]
                            direction_str = str(data["direction"]).upper()
                            direction = StreamDirection[direction_str]
                            
                            name = str(data["name"])
                            raw_metadata = data.get("metadata")
                            metadata: Optional[Dict[str, Any]] = None
                            if isinstance(raw_metadata, dict):
                                metadata = {str(k): v for k, v in raw_metadata.items()} # Ensure keys are strings
                            
                            stream_id = await self.create_stream(
                                stream_type=stream_type,
                                direction=direction,
                                name=name,
                                metadata=metadata
                            )
                            if self._message_bus:
                                await self._message_bus.publish(response_topic_base, {"stream_id": stream_id, "success": True})
                        except (KeyError, ValueError) as e: # Enum conversion error
                            logger.error(f"创建流失败: 无效的类型或方向. Type: {data.get('type')}, Direction: {data.get('direction')}. Error: {e}")
                            if self._message_bus:
                                await self._message_bus.publish(response_topic_base, {"success": False, "error": f"Invalid stream type or direction: {e}"})
                    else:
                        logger.warning(f"创建流命令缺少必要参数: {data}")
                        if self._message_bus:
                            await self._message_bus.publish(response_topic_base, {"success": False, "error": "Missing parameters for create"})
                
                elif command == "write":
                    if all(k in data for k in ["stream_id", "data"]):
                        stream_id = str(data["stream_id"])
                        stream_data = data["data"] # Type is Any
                        is_last = bool(data.get("is_last", False))
                        
                        success = await self.write_to_stream(stream_id, stream_data, is_last)
                        if self._message_bus:
                            await self._message_bus.publish(response_topic_base, {"stream_id": stream_id, "success": success})
                    else:
                        logger.warning(f"写入流命令缺少必要参数: {data}")
                        if self._message_bus:
                            await self._message_bus.publish(response_topic_base, {"stream_id": data.get("stream_id"), "success": False, "error": "Missing parameters for write"})
                
                elif command == "close":
                    if "stream_id" in data:
                        stream_id = str(data["stream_id"])
                        success = await self.close_stream(stream_id)
                        if self._message_bus:
                            await self._message_bus.publish(response_topic_base, {"stream_id": stream_id, "success": success})
                    else:
                        logger.warning(f"关闭流命令缺少 stream_id: {data}")
                        if self._message_bus:
                            await self._message_bus.publish(response_topic_base, {"success": False, "error": "Missing stream_id for close"})
                            
                elif command == "create_pipe":
                    if all(k in data for k in ["input_name", "output_name"]):
                        try:
                            stream_type_str = str(data.get("type", "TEXT")).upper()
                            stream_type = StreamType[stream_type_str]
                            
                            input_name = str(data["input_name"])
                            output_name = str(data["output_name"])
                            raw_metadata = data.get("metadata")
                            metadata: Optional[Dict[str, Any]] = None
                            if isinstance(raw_metadata, dict):
                                metadata = {str(k): v for k, v in raw_metadata.items()}

                            result = await self.create_pipe(
                                input_name=input_name,
                                output_name=output_name,
                                stream_type=stream_type,
                                metadata=metadata
                            )
                            if self._message_bus:
                                await self._message_bus.publish(response_topic_base, {"success": True, **result})
                        except (KeyError, ValueError) as e: # Enum conversion error
                            logger.error(f"创建管道失败: 无效的类型. Type: {data.get('type')}. Error: {e}")
                            if self._message_bus:
                                await self._message_bus.publish(response_topic_base, {"success": False, "error": f"Invalid stream type for pipe: {e}"})
                    else:
                        logger.warning(f"创建管道命令缺少必要参数: {data}")
                        if self._message_bus:
                             await self._message_bus.publish(response_topic_base, {"success": False, "error": "Missing parameters for create_pipe"})
                else:
                    logger.warning(f"未知流命令: {command} for topic {topic}")
                    if self._message_bus:
                        await self._message_bus.publish(f"stream.response.unknown_command", {"command": command, "success": False, "error": "Unknown command"})

            except Exception as e_cmd:
                logger.error(f"处理命令 {command} 失败: {e_cmd}", exc_info=True)
                if self._message_bus:
                    await self._message_bus.publish(response_topic_base, {"success": False, "error": str(e_cmd), "command": command})
    
    def get_metrics(self) -> Dict[str, Any]:
        """获取流处理器指标"""
        # metrics = super().get_metrics() # Assuming BaseComponent has get_metrics
        metrics: Dict[str, Any] = {} # If super().get_metrics() is not available or not suitable.
        
        # 计算流统计信息
        total_streams = len(self._streams)
        # active_streams_count = len(self._active_streams) # self._active_streams might not perfectly reflect StreamInfo.is_active
        active_streams_count = sum(1 for s_id in self._active_streams if s_id in self._streams and self._streams[s_id].is_active)

        total_bytes = sum(s.total_bytes for s in self._streams.values())
        total_messages = sum(s.total_messages for s in self._streams.values())
        total_errors = sum(s.error_count for s in self._streams.values())
        
        # 按类型统计流数量
        streams_by_type: Dict[str, int] = {st.name: 0 for st in StreamType}
        for s_info in self._streams.values():
            streams_by_type[s_info.stream_type.name] += 1
        
        # 按方向统计流数量
        streams_by_direction: Dict[str, int] = {sd.name: 0 for sd in StreamDirection}
        for s_info in self._streams.values():
            streams_by_direction[s_info.direction.name] += 1
        
        metrics.update({
            'total_streams': total_streams,
            'active_streams': active_streams_count,
            'total_bytes_processed': total_bytes, # Renamed for clarity
            'total_messages_processed': total_messages, # Renamed for clarity
            'total_stream_errors': total_errors, # Renamed for clarity
            'streams_by_type': streams_by_type,
            'streams_by_direction': streams_by_direction,
            'active_processing_tasks': len(self._stream_tasks),
            'event_listener_groups': len(self._event_listeners),
            'middleware_count': len(self._middleware),
            'transformers_groups_count': len(self._transformers) # Number of streams with transformers
        })
        
        return metrics
