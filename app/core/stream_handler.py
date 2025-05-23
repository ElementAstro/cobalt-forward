import asyncio
import json
import logging
import time
import uuid
from typing import Any, Dict, List, Optional, Set, Callable, AsyncGenerator, Union
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
        self._stream_queues: Dict[str, asyncio.Queue] = {}
        self._event_listeners: Dict[str, List[Callable]] = {}
        self._active_streams: Set[str] = set()
        self._stream_tasks: Dict[str, asyncio.Task] = {}
        self._heartbeat_task = None
        self._middleware: List[Callable] = []
        self._transformers: Dict[str, List[Callable]] = {}
        self._message_bus = None
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
        
        # 关闭所有活跃流
        for stream_id in list(self._active_streams):
            await self.close_stream(stream_id)
        
        # 取消所有流处理任务
        for task in self._stream_tasks.values():
            task.cancel()
        
        if self._stream_tasks:
            await asyncio.gather(*self._stream_tasks.values(), return_exceptions=True)
        
        logger.info("流处理器已停止")
    
    async def _heartbeat_monitor(self) -> None:
        """心跳监控任务"""
        while not self._closed:
            try:
                now = time.time()
                
                # 检查每个活跃流并发送心跳
                for stream_id in list(self._active_streams):
                    if stream_id not in self._streams:
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
                            sequence=-1
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
                break
            except Exception as e:
                logger.error(f"心跳监控任务出错: {e}")
                await asyncio.sleep(5)  # 发生错误时短暂休眠
    
    async def create_stream(self, stream_type: StreamType, direction: StreamDirection,
                          name: str, metadata: Dict[str, Any] = None) -> str:
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
        await self._trigger_event("stream_created", stream_id, stream_info)
        
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
            logger.warning(f"流不存在: {stream_id}")
            return False
        
        stream_info = self._streams[stream_id]
        
        if not stream_info.is_active:
            logger.warning(f"流已关闭: {stream_id}")
            return False
        
        if stream_info.direction == StreamDirection.INPUT:
            logger.warning(f"流是输入流，不能写入: {stream_id}")
            return False
        
        try:
            # 应用流变换器
            if stream_id in self._transformers:
                for transformer in self._transformers[stream_id]:
                    try:
                        data = await transformer(data)
                    except Exception as e:
                        logger.error(f"流变换器失败: {e}")
            
            # 创建数据片段
            fragment = DataFragment(
                fragment_id=str(uuid.uuid4()),
                stream_id=stream_id,
                fragment_type=DataFragmentType.DATA,
                data=data,
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
            if isinstance(data, (str, bytes)):
                data_size = len(data)
            elif isinstance(data, dict):
                data_size = len(json.dumps(data))
            
            stream_info.total_bytes += data_size
            
            # 如果是最后一个片段，发送结束事件
            if is_last:
                end_fragment = DataFragment(
                    fragment_id=str(uuid.uuid4()),
                    stream_id=stream_id,
                    fragment_type=DataFragmentType.END,
                    sequence=stream_info.total_messages
                )
                await self._stream_queues[stream_id].put(end_fragment)
                stream_info.is_active = False
                
                # 触发流结束事件
                await self._trigger_event("stream_ended", stream_id, stream_info)
                
                logger.info(f"流结束: {stream_id}, 总消息数: {stream_info.total_messages}, 总字节数: {stream_info.total_bytes}")
            
            return True
            
        except Exception as e:
            logger.error(f"写入流 {stream_id} 失败: {e}")
            stream_info.error_count += 1
            
            # 触发流错误事件
            await self._trigger_event("stream_error", stream_id, error=str(e))
            
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
            logger.warning(f"流不存在: {stream_id}")
            return False
        
        stream_info = self._streams[stream_id]
        
        if not stream_info.is_active:
            logger.warning(f"流已关闭: {stream_id}")
            return False
        
        try:
            # 创建错误片段
            error_fragment = DataFragment(
                fragment_id=str(uuid.uuid4()),
                stream_id=stream_id,
                fragment_type=DataFragmentType.ERROR,
                error=error,
                sequence=stream_info.total_messages
            )
            
            # 将错误片段放入队列
            await self._stream_queues[stream_id].put(error_fragment)
            
            # 更新流信息
            stream_info.total_messages += 1
            stream_info.last_activity = time.time()
            stream_info.error_count += 1
            
            # 触发流错误事件
            await self._trigger_event("stream_error", stream_id, error=error)
            
            logger.warning(f"流错误: {stream_id}, 错误: {error}")
            
            return True
            
        except Exception as e:
            logger.error(f"写入错误到流 {stream_id} 失败: {e}")
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
            logger.warning(f"流不存在: {stream_id}")
            return
        
        stream_info = self._streams[stream_id]
        queue = self._stream_queues[stream_id]
        
        if stream_info.direction == StreamDirection.OUTPUT:
            logger.warning(f"流是输出流，不能读取: {stream_id}")
            return
        
        try:
            while stream_info.is_active or not queue.empty():
                # 从队列获取下一个数据片段
                fragment = await queue.get()
                
                # 更新流活动时间
                stream_info.last_activity = time.time()
                
                # 处理不同类型的片段
                if fragment.fragment_type == DataFragmentType.DATA:
                    # 产生实际数据
                    yield fragment.data
                    
                    # 如果是最后一个片段，结束流
                    if fragment.is_last:
                        stream_info.is_active = False
                
                elif fragment.fragment_type == DataFragmentType.ERROR:
                    # 产生错误信息
                    yield {"error": fragment.error}
                    
                elif fragment.fragment_type == DataFragmentType.END:
                    # 流结束
                    stream_info.is_active = False
                    break
                
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
            logger.error(f"读取流 {stream_id} 出错: {e}")
            stream_info.error_count += 1
            
            # 触发流错误事件
            await self._trigger_event("stream_error", stream_id, error=str(e))
            
            # 产生错误
            yield {"error": str(e)}
    
    async def close_stream(self, stream_id: str) -> bool:
        """
        关闭流
        
        Args:
            stream_id: 流ID
            
        Returns:
            是否成功关闭
        """
        if stream_id not in self._streams:
            logger.warning(f"流不存在: {stream_id}")
            return False
        
        stream_info = self._streams[stream_id]
        
        if not stream_info.is_active:
            logger.warning(f"流已经关闭: {stream_id}")
            return False
        
        try:
            # 更新流状态
            stream_info.is_active = False
            
            # 如果有待处理的流任务，取消它
            if stream_id in self._stream_tasks and not self._stream_tasks[stream_id].done():
                self._stream_tasks[stream_id].cancel()
                try:
                    await self._stream_tasks[stream_id]
                except asyncio.CancelledError:
                    pass
            
            # 从活跃流集合中移除
            if stream_id in self._active_streams:
                self._active_streams.remove(stream_id)
            
            # 触发流关闭事件
            await self._trigger_event("stream_closed", stream_id, stream_info)
            
            logger.info(f"流关闭: {stream_id}")
            
            return True
            
        except Exception as e:
            logger.error(f"关闭流 {stream_id} 失败: {e}")
            return False
    
    def add_transformer(self, stream_id: str, transformer: Callable) -> None:
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
    
    def add_middleware(self, middleware: Callable) -> None:
        """
        添加流处理中间件
        
        Args:
            middleware: 中间件函数
        """
        self._middleware.append(middleware)
        logger.debug(f"添加流处理中间件")
    
    def add_event_listener(self, event_type: str, listener: Callable) -> None:
        """
        添加流事件监听器
        
        Args:
            event_type: 事件类型 ('stream_created', 'stream_ended', 'stream_error', 'stream_closed')
            listener: 监听器函数
        """
        if event_type not in self._event_listeners:
            self._event_listeners[event_type] = []
        
        self._event_listeners[event_type].append(listener)
        logger.debug(f"添加流事件监听器: {event_type}")
    
    async def _trigger_event(self, event_type: str, stream_id: str, 
                           stream_info: StreamInfo = None, **kwargs) -> None:
        """触发流事件"""
        # 调用注册的监听器
        for listener in self._event_listeners.get(event_type, []):
            try:
                if asyncio.iscoroutinefunction(listener):
                    await listener(stream_id, stream_info=stream_info, **kwargs)
                else:
                    listener(stream_id, stream_info=stream_info, **kwargs)
            except Exception as e:
                logger.error(f"执行流事件监听器失败: {e}")
        
        # 发布到消息总线
        if self._message_bus:
            try:
                event_data = {
                    "stream_id": stream_id,
                    "timestamp": time.time()
                }
                
                if stream_info:
                    event_data.update({
                        "name": stream_info.name,
                        "type": stream_info.stream_type.name,
                        "direction": stream_info.direction.name,
                        "is_active": stream_info.is_active,
                        "total_messages": stream_info.total_messages,
                        "total_bytes": stream_info.total_bytes
                    })
                
                event_data.update(kwargs)
                
                await self._message_bus.publish(f"stream.{event_type}", event_data)
            except Exception as e:
                logger.error(f"发布流事件到消息总线失败: {e}")
    
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
            return [stream for stream in self._streams.values() if stream.is_active]
        return list(self._streams.values())
    
    @error_boundary()
    async def process_stream(self, stream_id: str, processor: Callable) -> None:
        """
        处理流数据
        
        Args:
            stream_id: 流ID
            processor: 处理器函数，接收流数据并返回处理结果
        """
        if stream_id not in self._streams:
            logger.warning(f"流不存在: {stream_id}")
            return
        
        stream_info = self._streams[stream_id]
        
        if not stream_info.is_active:
            logger.warning(f"流已关闭: {stream_id}")
            return
        
        try:
            # 创建处理任务
            task = asyncio.create_task(self._run_processor(stream_id, processor))
            self._stream_tasks[stream_id] = task
            
            # 等待任务完成
            await task
            
        except asyncio.CancelledError:
            logger.info(f"流处理任务被取消: {stream_id}")
            
        except Exception as e:
            logger.error(f"处理流 {stream_id} 失败: {e}")
            stream_info.error_count += 1
            
            # 触发流错误事件
            await self._trigger_event("stream_error", stream_id, error=str(e))
            
        finally:
            # 从任务字典中移除
            if stream_id in self._stream_tasks:
                del self._stream_tasks[stream_id]
    
    async def _run_processor(self, stream_id: str, processor: Callable) -> None:
        """运行流处理器"""
        async for data in self.read_from_stream(stream_id):
            # 应用中间件
            for middleware in self._middleware:
                try:
                    if asyncio.iscoroutinefunction(middleware):
                        data = await middleware(data, stream_id=stream_id)
                    else:
                        data = middleware(data, stream_id=stream_id)
                except Exception as e:
                    logger.error(f"流中间件处理失败: {e}")
            
            # 应用处理器
            try:
                if asyncio.iscoroutinefunction(processor):
                    result = await processor(data)
                else:
                    result = processor(data)
                
                # 如果处理器返回了结果，写入到输出流
                if result is not None:
                    # 查找或创建与输入流相关联的输出流
                    output_stream_id = self._streams[stream_id].metadata.get("output_stream_id")
                    
                    if output_stream_id and output_stream_id in self._streams:
                        await self.write_to_stream(output_stream_id, result)
                        
            except Exception as e:
                logger.error(f"流处理器执行失败: {e}")
                await self.write_error_to_stream(stream_id, str(e))
    
    async def create_pipe(self, input_name: str, output_name: str,
                        stream_type: StreamType = StreamType.TEXT,
                        metadata: Dict[str, Any] = None) -> Dict[str, str]:
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
        metadata = metadata.copy() if metadata else {}
        
        # 创建输入流
        input_stream_id = await self.create_stream(
            stream_type=stream_type,
            direction=StreamDirection.INPUT,
            name=input_name,
            metadata=metadata
        )
        
        # 创建输出流
        output_stream_id = await self.create_stream(
            stream_type=stream_type,
            direction=StreamDirection.OUTPUT,
            name=output_name,
            metadata=metadata
        )
        
        # 关联两个流
        self._streams[input_stream_id].metadata["output_stream_id"] = output_stream_id
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
            logger.warning(f"源流不存在: {source_id}")
            return
            
        if target_id not in self._streams:
            logger.warning(f"目标流不存在: {target_id}")
            return
            
        source_info = self._streams[source_id]
        target_info = self._streams[target_id]
        
        if not source_info.is_active:
            logger.warning(f"源流已关闭: {source_id}")
            return
            
        if not target_info.is_active:
            logger.warning(f"目标流已关闭: {target_id}")
            return
            
        # 创建复制任务
        async def copy_task():
            async for data in self.read_from_stream(source_id):
                # 写入数据到目标流
                await self.write_to_stream(target_id, data)
                
                # 如果源流包含错误，同样写入目标流
                if isinstance(data, dict) and "error" in data:
                    await self.write_error_to_stream(target_id, data["error"])
        
        task = asyncio.create_task(copy_task())
        self._stream_tasks[f"copy_{source_id}_to_{target_id}"] = task
        
        logger.info(f"开始从流 {source_id} 复制到流 {target_id}")
    
    async def set_message_bus(self, message_bus) -> None:
        """设置消息总线引用"""
        self._message_bus = message_bus
        logger.info("消息总线已与流处理器集成")
    
    async def handle_message(self, topic: str, data: Any) -> None:
        """处理接收到的消息"""
        if topic.startswith("stream.command."):
            command = topic.split(".")[-1]
            
            if command == "create":
                # 创建流
                if isinstance(data, dict) and "name" in data and "type" in data and "direction" in data:
                    try:
                        stream_type = StreamType[data["type"]]
                        direction = StreamDirection[data["direction"]]
                        
                        stream_id = await self.create_stream(
                            stream_type=stream_type,
                            direction=direction,
                            name=data["name"],
                            metadata=data.get("metadata")
                        )
                        
                        # 发送响应
                        if self._message_bus:
                            await self._message_bus.publish("stream.response.created", {
                                "stream_id": stream_id,
                                "success": True
                            })
                    except (KeyError, ValueError) as e:
                        logger.error(f"创建流失败: {e}")
                        
                        # 发送错误响应
                        if self._message_bus:
                            await self._message_bus.publish("stream.response.created", {
                                "success": False,
                                "error": str(e)
                            })
            
            elif command == "write":
                # 写入数据到流
                if isinstance(data, dict) and "stream_id" in data and "data" in data:
                    stream_id = data["stream_id"]
                    stream_data = data["data"]
                    is_last = data.get("is_last", False)
                    
                    success = await self.write_to_stream(stream_id, stream_data, is_last)
                    
                    # 发送响应
                    if self._message_bus:
                        await self._message_bus.publish("stream.response.write", {
                            "stream_id": stream_id,
                            "success": success
                        })
            
            elif command == "close":
                # 关闭流
                if isinstance(data, dict) and "stream_id" in data:
                    stream_id = data["stream_id"]
                    
                    success = await self.close_stream(stream_id)
                    
                    # 发送响应
                    if self._message_bus:
                        await self._message_bus.publish("stream.response.close", {
                            "stream_id": stream_id,
                            "success": success
                        })
                        
            elif command == "create_pipe":
                # 创建双向管道
                if isinstance(data, dict) and "input_name" in data and "output_name" in data:
                    stream_type_str = data.get("type", "TEXT")
                    try:
                        stream_type = StreamType[stream_type_str]
                        
                        result = await self.create_pipe(
                            input_name=data["input_name"],
                            output_name=data["output_name"],
                            stream_type=stream_type,
                            metadata=data.get("metadata")
                        )
                        
                        # 发送响应
                        if self._message_bus:
                            await self._message_bus.publish("stream.response.create_pipe", {
                                "success": True,
                                **result
                            })
                    except (KeyError, ValueError) as e:
                        logger.error(f"创建管道失败: {e}")
                        
                        # 发送错误响应
                        if self._message_bus:
                            await self._message_bus.publish("stream.response.create_pipe", {
                                "success": False,
                                "error": str(e)
                            })
    
    def get_metrics(self) -> Dict[str, Any]:
        """获取流处理器指标"""
        metrics = super().metrics
        
        # 计算流统计信息
        total_streams = len(self._streams)
        active_streams = len(self._active_streams)
        total_bytes = sum(s.total_bytes for s in self._streams.values())
        total_messages = sum(s.total_messages for s in self._streams.values())
        total_errors = sum(s.error_count for s in self._streams.values())
        
        # 按类型统计流数量
        streams_by_type = {}
        for stream_type in StreamType:
            streams_by_type[stream_type.name] = len([
                s for s in self._streams.values() if s.stream_type == stream_type
            ])
        
        # 按方向统计流数量
        streams_by_direction = {}
        for direction in StreamDirection:
            streams_by_direction[direction.name] = len([
                s for s in self._streams.values() if s.direction == direction
            ])
        
        metrics.update({
            'total_streams': total_streams,
            'active_streams': active_streams,
            'total_bytes': total_bytes,
            'total_messages': total_messages,
            'total_errors': total_errors,
            'streams_by_type': streams_by_type,
            'streams_by_direction': streams_by_direction,
            'middleware_count': len(self._middleware),
            'transformers_count': len(self._transformers)
        })
        
        return metrics
