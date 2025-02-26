from abc import abstractmethod
from types import coroutine
from typing import Any, Dict, Type, Optional, List, Callable, Protocol, Generic, TypeVar
from dataclasses import dataclass, field
from enum import Enum, auto
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import asyncio
import uuid
import time
import json
import logging
from datetime import datetime, timedelta
import traceback

from app.core.base import BaseComponent
from app.core.shared_services import SharedServices, ServiceType
from app.utils.error_handler import error_boundary

logger = logging.getLogger(__name__)

# 命令相关的类型定义
T = TypeVar('T')
R = TypeVar('R')


class CommandType(Enum):
    """命令类型枚举"""
    USER = auto()           # 用户发起的命令
    SYSTEM = auto()         # 系统自动发起的命令
    SCHEDULED = auto()      # 定时调度的命令
    BATCH = auto()          # 批处理命令
    PLUGIN = auto()         # 插件发起的命令
    REMOTE = auto()         # 远程调用命令


class CommandPriority(Enum):
    """命令优先级枚举"""
    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3


class CommandStatus(Enum):
    """命令状态枚举"""
    PENDING = auto()        # 等待执行
    EXECUTING = auto()      # 执行中
    COMPLETED = auto()      # 执行完成
    FAILED = auto()         # 执行失败
    CANCELED = auto()       # 已取消
    TIMEOUT = auto()        # 超时
    RETRY = auto()          # 重试中


@dataclass
class Command(Generic[T]):
    """命令基类"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    command_type: CommandType = CommandType.USER
    name: str = ""
    data: Optional[T] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    priority: CommandPriority = CommandPriority.NORMAL
    created_at: float = field(default_factory=time.time)
    timeout: float = 30.0  # 默认30秒超时
    retry_count: int = 0
    max_retries: int = 0
    source: str = "user"
    target: str = "system"
    correlation_id: Optional[str] = None

    def __lt__(self, other):
        """支持基于优先级的比较，用于优先队列"""
        if not isinstance(other, Command):
            return NotImplemented
        # 高优先级在队列前面，同等优先级则先进先出
        return (self.priority.value > other.priority.value or
                (self.priority == other.priority and self.created_at < other.created_at))


@dataclass
class CommandResult(Generic[R]):
    """命令执行结果"""
    command_id: str
    status: CommandStatus
    result: Optional[R] = None
    error: Optional[str] = None
    execution_time: float = 0.0  # 执行耗时
    completed_at: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)


class CommandHandler(Protocol, Generic[T, R]):
    """命令处理器协议，定义命令处理的接口"""

    @abstractmethod
    async def handle(self, command: Command[T]) -> CommandResult[R]:
        """处理命令"""
        pass


class UserCommand(Command[Dict[str, Any]]):
    """用户命令，携带用户信息"""
    def __init__(self, name: str, data: Dict[str, Any], user_id: str, **kwargs):
        super().__init__(name=name, data=data, command_type=CommandType.USER, **kwargs)
        self.metadata["user_id"] = user_id
        self.metadata["session_id"] = kwargs.get("session_id", str(uuid.uuid4()))


class ScheduledCommand(Command[Dict[str, Any]]):
    """定时调度命令"""
    def __init__(self, name: str, data: Dict[str, Any], schedule_id: str, **kwargs):
        super().__init__(name=name, data=data, command_type=CommandType.SCHEDULED, **kwargs)
        self.metadata["schedule_id"] = schedule_id
        self.metadata["execution_count"] = kwargs.get("execution_count", 0)


class BatchCommand(Command[List[Dict[str, Any]]]):
    """批处理命令，包含多个子命令"""
    def __init__(self, name: str, commands: List[Dict[str, Any]], **kwargs):
        super().__init__(name=name, data=commands, command_type=CommandType.BATCH, **kwargs)
        self.metadata["command_count"] = len(commands)
        self.metadata["batch_id"] = kwargs.get("batch_id", str(uuid.uuid4()))


class CommandTransmitter(BaseComponent):
    """
    命令发送器，负责将命令发送到消息总线
    
    用于客户端向服务器发送命令请求
    """
    
    def __init__(self, message_bus = None, name: str = "command_transmitter"):
        """
        初始化命令发送器
        
        Args:
            message_bus: 消息总线
            name: 组件名称
        """
        super().__init__(name=name)
        self._message_bus = message_bus
        self._pending_responses: Dict[str, asyncio.Future] = {}
        self._response_timeout = 30.0  # 默认响应超时时间
        self._default_retry = 3  # 默认重试次数
    
    async def _start_impl(self) -> None:
        """启动命令发送器"""
        if self._message_bus:
            # 订阅命令响应主题
            self._response_queue = await self._message_bus.subscribe("command.response")
            # 启动响应处理任务
            self._response_task = asyncio.create_task(self._process_responses())
    
    async def _stop_impl(self) -> None:
        """停止命令发送器"""
        if hasattr(self, '_response_task') and self._response_task:
            self._response_task.cancel()
            try:
                await self._response_task
            except asyncio.CancelledError:
                pass
        
        # 取消所有未完成的命令响应
        for cmd_id, future in self._pending_responses.items():
            if not future.done():
                future.set_exception(Exception("Command transmitter stopped"))
    
    async def _process_responses(self) -> None:
        """处理命令响应消息"""
        while self._running:
            try:
                message = await self._response_queue.get()
                result = message.data
                
                # 查找匹配的命令ID
                cmd_id = result.get("command_id")
                if cmd_id and cmd_id in self._pending_responses:
                    future = self._pending_responses[cmd_id]
                    if not future.done():
                        future.set_result(result)
                    
                    # 清理已完成的命令
                    del self._pending_responses[cmd_id]
            
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"处理命令响应时出错: {e}")
    
    @error_boundary()
    async def send(self, command: Command) -> CommandResult:
        """
        发送命令并等待响应
        
        Args:
            command: 要发送的命令
            
        Returns:
            命令执行结果
            
        Raises:
            asyncio.TimeoutError: 如果命令响应超时
            Exception: 其他发送错误
        """
        if not self._message_bus:
            raise Exception("消息总线未初始化")
        
        # 创建响应Future
        future = asyncio.get_event_loop().create_future()
        self._pending_responses[command.id] = future
        
        # 处理命令超时
        timeout = command.timeout or self._response_timeout
        
        # 发送命令
        start_time = time.time()
        try:
            # 发送到命令请求主题
            topic = f"command.request.{command.name}"
            
            # 准备发送数据
            message_data = {
                "id": command.id,
                "name": command.name,
                "type": command.command_type.name,
                "data": command.data,
                "metadata": command.metadata,
                "priority": command.priority.name,
                "source": command.source,
                "target": command.target,
                "correlation_id": command.correlation_id,
                "created_at": command.created_at
            }
            
            # 发送命令
            await self._message_bus.publish(topic, message_data)
            
            # 等待响应，设置超时
            result_data = await asyncio.wait_for(future, timeout)
            
            # 构建命令结果
            execution_time = time.time() - start_time
            result = CommandResult(
                command_id=command.id,
                status=CommandStatus[result_data.get("status", "COMPLETED")],
                result=result_data.get("result"),
                error=result_data.get("error"),
                execution_time=execution_time,
                completed_at=time.time(),
                metadata=result_data.get("metadata", {})
            )
            
            # 记录命令执行指标
            self._component_metrics.record_message(execution_time)
            
            return result
            
        except asyncio.TimeoutError:
            # 清理超时命令的Future
            if command.id in self._pending_responses:
                del self._pending_responses[command.id]
            
            # 返回超时结果
            self._component_metrics.record_error()
            return CommandResult(
                command_id=command.id,
                status=CommandStatus.TIMEOUT,
                error="Command timed out",
                execution_time=time.time() - start_time,
                completed_at=time.time(),
                metadata={"timeout": timeout}
            )
            
        except Exception as e:
            # 清理错误命令的Future
            if command.id in self._pending_responses:
                del self._pending_responses[command.id]
            
            # 记录错误
            self._component_metrics.record_error()
            logger.error(f"发送命令 {command.name} 失败: {e}")
            
            # 返回失败结果
            return CommandResult(
                command_id=command.id,
                status=CommandStatus.FAILED,
                error=str(e),
                execution_time=time.time() - start_time,
                completed_at=time.time(),
                metadata={"exception": traceback.format_exc()}
            )
    
    async def send_batch(self, commands: List[Command], wait_all: bool = True) -> List[CommandResult]:
        """
        批量发送多个命令
        
        Args:
            commands: 要发送的命令列表
            wait_all: 是否等待所有命令完成
            
        Returns:
            命令结果列表
        """
        if wait_all:
            # 等待所有命令完成
            results = []
            for cmd in commands:
                result = await self.send(cmd)
                results.append(result)
            return results
        else:
            # 并行发送所有命令
            tasks = [self.send(cmd) for cmd in commands]
            return await asyncio.gather(*tasks, return_exceptions=True)
    
    async def create_and_send(self, name: str, data: Any, command_type: CommandType = CommandType.USER, 
                              **kwargs) -> CommandResult:
        """
        创建并发送命令的便捷方法
        
        Args:
            name: 命令名称
            data: 命令数据
            command_type: 命令类型
            **kwargs: 其他命令参数
            
        Returns:
            命令执行结果
        """
        cmd = Command(
            name=name,
            data=data,
            command_type=command_type,
            **kwargs
        )
        return await self.send(cmd)
    
    def register_handler(self, command_name: str, handler: CommandHandler) -> None:
        """
        注册命令处理器
        
        即使命令发送器主要用于发送命令，也可以在本地注册一些基本命令处理器
        
        Args:
            command_name: 命令名称
            handler: 命令处理器
        """
        # 此方法主要用于将本地处理器注册到命令服务，不在此实现详细逻辑
        logger.info(f"注册本地命令处理器: {command_name}")


class UserCommandHandler(BaseComponent):
    """用户命令处理器基类，用于处理用户交互命令"""
    
    def __init__(self, name: Optional[str] = None):
        """初始化用户命令处理器"""
        super().__init__(name=name)
        # 命令授权等级
        self.required_permission = None
        # 命令处理超时
        self.timeout = 30.0
    
    @abstractmethod
    async def handle_command(self, command: UserCommand) -> CommandResult:
        """
        处理用户命令，需要子类实现
        
        Args:
            command: 用户命令
            
        Returns:
            命令执行结果
        """
        pass


class CommandDispatcher(BaseComponent):
    """
    命令分发器，负责接收和分发命令到适当的处理器
    
    用于服务端接收和处理客户端发送的命令
    """
    
    def __init__(self, name: str = "command_dispatcher", max_workers: int = 10):
        """
        初始化命令分发器
        
        Args:
            name: 组件名称
            max_workers: 最大工作线程数
        """
        super().__init__(name=name)
        self._handlers: Dict[str, Dict[CommandType, List[CommandHandler]]] = defaultdict(
            lambda: {ct: [] for ct in CommandType}
        )
        self._message_bus = None
        self._event_bus = None
        self._running = False
        self._command_queue = asyncio.PriorityQueue()
        self._result_cache: Dict[str, CommandResult] = {}
        self._thread_pool = ThreadPoolExecutor(max_workers=max_workers)
        self._worker_semaphore = asyncio.Semaphore(max_workers)
        self._worker_tasks: List[asyncio.Task] = []
        self._active_commands: Dict[str, Command] = {}
        self._active_command_futures: Dict[str, asyncio.Future] = {}
        self._command_subscription = None
        self._metrics.update({
            'commands_processed': 0,
            'commands_failed': 0,
            'avg_processing_time': 0,
            'active_commands': 0,
            'queue_size': 0
        })
    
    async def _start_impl(self) -> None:
        """启动命令分发器"""
        self._running = True
        
        # 如果有消息总线，订阅命令请求主题
        if self._message_bus:
            self._command_subscription = await self._message_bus.subscribe("command.request.#")
            # 启动命令接收任务
            asyncio.create_task(self._receive_commands())
        
        # 启动工作线程
        self._worker_tasks = [
            asyncio.create_task(self._worker_process())
            for _ in range(min(10, asyncio.get_event_loop()._default_executor._max_workers))
        ]
        
        logger.info(f"命令分发器启动完成，工作线程数: {len(self._worker_tasks)}")
    
    async def _stop_impl(self) -> None:
        """停止命令分发器"""
        self._running = False
        
        # 取消所有活动命令
        for cmd_id, future in self._active_command_futures.items():
            if not future.done():
                future.set_exception(Exception("CommandDispatcher stopped"))
        
        # 取消所有工作线程
        for task in self._worker_tasks:
            task.cancel()
        
        # 等待所有工作线程结束
        if self._worker_tasks:
            await asyncio.gather(*self._worker_tasks, return_exceptions=True)
            self._worker_tasks = []
        
        logger.info("命令分发器已停止")
    
    async def set_message_bus(self, message_bus) -> None:
        """设置消息总线引用"""
        self._message_bus = message_bus
        # 如果分发器已经运行，立即订阅命令主题
        if self._running:
            self._command_subscription = await self._message_bus.subscribe("command.request.#")
            asyncio.create_task(self._receive_commands())
        logger.info("消息总线已与命令分发器集成")
    
    async def set_event_bus(self, event_bus) -> None:
        """设置事件总线引用"""
        self._event_bus = event_bus
        logger.info("事件总线已与命令分发器集成")
    
    async def _receive_commands(self) -> None:
        """从消息总线接收命令请求"""
        while self._running:
            try:
                message = await self._command_subscription.get()
                topic = message.topic
                data = message.data
                
                # 解析命令名称
                cmd_name = topic.split('.')[-1] if '.' in topic else topic
                
                # 构建命令对象
                try:
                    cmd_type = CommandType[data.get("type", "USER")]
                    cmd_priority = CommandPriority[data.get("priority", "NORMAL")]
                    
                    command = Command(
                        id=data.get("id", str(uuid.uuid4())),
                        command_type=cmd_type,
                        name=cmd_name,
                        data=data.get("data"),
                        metadata=data.get("metadata", {}),
                        priority=cmd_priority,
                        created_at=data.get("created_at", time.time()),
                        source=data.get("source", "remote"),
                        target=data.get("target", "system"),
                        correlation_id=data.get("correlation_id")
                    )
                    
                    # 发送命令到队列
                    await self._command_queue.put((command.priority.value, command))
                    
                    # 更新指标
                    self._metrics['queue_size'] = self._command_queue.qsize()
                    logger.debug(f"接收到命令: {cmd_name}, ID: {command.id}")
                    
                except Exception as e:
                    logger.error(f"解析命令数据失败: {e}, 数据: {data}")
                    # 发送错误响应
                    if self._message_bus:
                        error_response = {
                            "command_id": data.get("id", "unknown"),
                            "status": "FAILED",
                            "error": f"Invalid command format: {str(e)}",
                            "completed_at": time.time()
                        }
                        await self._message_bus.publish("command.response", error_response)
            
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"接收命令时出错: {e}")
    
    async def _worker_process(self) -> None:
        """工作线程处理命令队列"""
        while self._running:
            try:
                # 获取下一个命令
                _, command = await self._command_queue.get()
                
                # 使用工作线程信号量限制并发
                async with self._worker_semaphore:
                    try:
                        # 记录活动命令
                        self._active_commands[command.id] = command
                        self._metrics['active_commands'] = len(self._active_commands)
                        
                        # 处理命令
                        result = await self._dispatch_command(command)
                        
                        # 缓存结果
                        self._result_cache[command.id] = result
                        
                        # 如果是远程命令，发送响应
                        if command.source != "local" and self._message_bus:
                            response = {
                                "command_id": result.command_id,
                                "status": result.status.name,
                                "result": result.result,
                                "error": result.error,
                                "execution_time": result.execution_time,
                                "completed_at": result.completed_at,
                                "metadata": result.metadata
                            }
                            await self._message_bus.publish("command.response", response)
                        
                        # 处理命令结果的Future
                        if command.id in self._active_command_futures:
                            future = self._active_command_futures[command.id]
                            if not future.done():
                                future.set_result(result)
                            del self._active_command_futures[command.id]
                        
                    except Exception as e:
                        logger.error(f"处理命令 {command.name} 失败: {e}")
                        
                        # 构建错误结果
                        error_result = CommandResult(
                            command_id=command.id,
                            status=CommandStatus.FAILED,
                            error=str(e),
                            completed_at=time.time(),
                            metadata={"exception": traceback.format_exc()}
                        )
                        
                        # 缓存错误结果
                        self._result_cache[command.id] = error_result
                        
                        # 发送错误响应
                        if command.source != "local" and self._message_bus:
                            error_response = {
                                "command_id": command.id,
                                "status": "FAILED",
                                "error": str(e),
                                "completed_at": time.time(),
                                "metadata": {"exception": traceback.format_exc()}
                            }
                            await self._message_bus.publish("command.response", error_response)
                        
                        # 处理命令结果的Future
                        if command.id in self._active_command_futures:
                            future = self._active_command_futures[command.id]
                            if not future.done():
                                future.set_exception(e)
                            del self._active_command_futures[command.id]
                    
                    finally:
                        # 移除活动命令
                        if command.id in self._active_commands:
                            del self._active_commands[command.id]
                            self._metrics['active_commands'] = len(self._active_commands)
                        
                        # 标记任务完成
                        self._command_queue.task_done()
                        
                        # 更新队列大小指标
                        self._metrics['queue_size'] = self._command_queue.qsize()
            
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"命令工作线程出错: {e}")
    
    async def _dispatch_command(self, command: Command) -> CommandResult:
        """
        根据命令类型和名称分发命令到适当的处理器
        
        Args:
            command: 要处理的命令
            
        Returns:
            命令执行结果
        """
        start_time = time.time()
        
        try:
            # 查找匹配的处理器
            handlers = self._handlers.get(command.name, {}).get(command.command_type, [])
            
            # 如果没有找到处理器，尝试通用处理器
            if not handlers and command.command_type in self._handlers.get("*", {}):
                handlers = self._handlers.get("*", {}).get(command.command_type, [])
            
            # 如果仍然没有找到处理器，尝试默认处理器
            if not handlers:
                handlers = self._handlers.get("*", {}).get(CommandType.USER, [])
            
            # 如果没有找到任何处理器，返回失败
            if not handlers:
                raise Exception(f"No handler found for command: {command.name} of type {command.command_type.name}")
            
            # 选择第一个处理器处理命令
            handler = handlers[0]
            
            # 使用超时机制调用处理器
            try:
                result = await asyncio.wait_for(
                    handler.handle(command),
                    timeout=command.timeout
                )
            except asyncio.TimeoutError:
                result = CommandResult(
                    command_id=command.id,
                    status=CommandStatus.TIMEOUT,
                    error=f"Command execution timed out after {command.timeout} seconds",
                    completed_at=time.time(),
                    execution_time=time.time() - start_time
                )
            
            # 发布命令执行完成事件
            if self._event_bus:
                await self._event_bus.publish(
                    f"command.executed.{command.name}",
                    {
                        "command_id": command.id,
                        "name": command.name,
                        "success": result.status == CommandStatus.COMPLETED,
                        "execution_time": result.execution_time
                    }
                )
            
            # 更新指标
            self._metrics['commands_processed'] += 1
            if result.status != CommandStatus.COMPLETED:
                self._metrics['commands_failed'] += 1
            
            # 更新平均处理时间
            execution_time = time.time() - start_time
            if 'processing_times' not in self._metrics:
                self._metrics['processing_times'] = []
            
            self._metrics['processing_times'].append(execution_time)
            if len(self._metrics['processing_times']) > 100:  # 保持最近100个命令的处理时间
                self._metrics['processing_times'].pop(0)
            
            # 计算新的平均处理时间
            if self._metrics['processing_times']:
                self._metrics['avg_processing_time'] = sum(self._metrics['processing_times']) / len(self._metrics['processing_times'])
            
            return result
            
        except Exception as e:
            # 更新错误指标
            self._metrics['commands_failed'] += 1
            self._component_metrics.record_error()
            
            # 记录错误详情
            logger.error(f"处理命令 {command.name} 时出错: {e}")
            logger.debug(traceback.format_exc())
            
            # 构建错误结果
            error_result = CommandResult(
                command_id=command.id,
                status=CommandStatus.FAILED,
                error=str(e),
                completed_at=time.time(),
                execution_time=time.time() - start_time,
                metadata={"exception": traceback.format_exc()}
            )
            
            # 发布命令执行失败事件
            if self._event_bus:
                await self._event_bus.publish(
                    "command.failed",
                    {
                        "command_id": command.id,
                        "name": command.name,
                        "error": str(e),
                        "execution_time": error_result.execution_time
                    }
                )
            
            return error_result
    
    async def dispatch(self, command_name: str, data: Any, **kwargs) -> CommandResult:
        """
        分发命令，可直接从组件内部调用
        
        Args:
            command_name: 命令名称
            data: 命令数据
            **kwargs: 其他命令参数
            
        Returns:
            命令执行结果
        """
        # 创建命令
        command = Command(
            name=command_name,
            data=data,
            source="local",  # 标记为本地调用
            **kwargs
        )
        
        # 创建Future以等待结果
        future = asyncio.get_event_loop().create_future()
        self._active_command_futures[command.id] = future
        
        # 将命令加入队列
        await self._command_queue.put((command.priority.value, command))
        
        try:
            # 等待命令执行结果
            return await asyncio.wait_for(future, command.timeout)
        except asyncio.TimeoutError:
            # 命令执行超时
            if command.id in self._active_command_futures:
                del self._active_command_futures[command.id]
                
            return CommandResult(
                command_id=command.id,
                status=CommandStatus.TIMEOUT,
                error=f"Command dispatch timed out after {command.timeout} seconds",
                completed_at=time.time()
            )
    
    def register_handler(self, command_name: str, handler: CommandHandler,
                         command_type: CommandType = CommandType.USER) -> None:
        """
        注册命令处理器
        
        Args:
            command_name: 命令名称，可以是具体名称或通配符"*"
            handler: 命令处理器
            command_type: 命令类型
        """
        self._handlers[command_name][command_type].append(handler)
        logger.debug(f"注册命令处理器: {command_name}, 类型: {command_type.name}")
    
    def unregister_handler(self, command_name: str, handler: CommandHandler,
                          command_type: CommandType = CommandType.USER) -> bool:
        """
        取消注册命令处理器
        
        Args:
            command_name: 命令名称
            handler: 命令处理器
            command_type: 命令类型
            
        Returns:
            是否成功取消注册
        """
        if (command_name in self._handlers and 
            command_type in self._handlers[command_name] and
            handler in self._handlers[command_name][command_type]):
            
            self._handlers[command_name][command_type].remove(handler)
            
            # 如果没有更多处理器，清
