from typing import Any, Dict, List, Optional, Type, Generic, TypeVar
from dataclasses import dataclass
import asyncio
import json
from collections import defaultdict
import uuid
from enum import Enum
from abc import ABC, abstractmethod

from loguru import logger

from app.core.base import BaseComponent
from app.utils.error_handler import CircuitBreaker
from app.utils.performance import RateLimiter, async_performance_monitor
from app.client.tcp.client import ClientConfig, TCPClient
from app.utils.error_handler import ExceptionHandler, RetryConfig, error_boundary
from app.core.exceptions import (
    MessageBusException, MessageDeliveryException,
    MessageProcessingException, ConnectionException
)

_error_handler = ExceptionHandler()

T = TypeVar('T')


class MessageType(Enum):
    """Message types for internal communication"""
    REQUEST = "REQUEST"
    RESPONSE = "RESPONSE"
    EVENT = "EVENT"
    ERROR = "ERROR"


@dataclass
class Message(Generic[T]):
    """Message structure for bus communication"""
    id: str
    type: MessageType
    topic: str
    data: T
    correlation_id: Optional[str] = None
    metadata: Dict[str, Any] = None


class Serializer(ABC):
    """Abstract base class for message serializers"""
    @abstractmethod
    def serialize(self, message: Message) -> bytes:
        pass

    @abstractmethod
    def deserialize(self, data: bytes) -> Message:
        pass


class JSONSerializer(Serializer):
    """JSON implementation of message serializer"""

    def serialize(self, message: Message) -> bytes:
        return json.dumps({
            'id': message.id,
            'type': message.type.value,
            'topic': message.topic,
            'data': message.data,
            'correlation_id': message.correlation_id,
            'metadata': message.metadata
        }).encode('utf-8')

    def deserialize(self, data: bytes) -> Message:
        msg_dict = json.loads(data.decode('utf-8'))
        return Message(
            id=msg_dict['id'],
            type=MessageType(msg_dict['type']),
            topic=msg_dict['topic'],
            data=msg_dict['data'],
            correlation_id=msg_dict['correlation_id'],
            metadata=msg_dict['metadata']
        )


class MessageBus:
    """
    High-performance asynchronous message bus.
    This class provides an asynchronous infrastructure for publishing, requesting, and subscribing
    to messages over a TCP connection. It includes features such as error handling with retries,
    rate limiting, circuit breaking, priority queue processing, and metrics collection.
    Attributes:
        _tcp_client (TCPClient): The TCP client used for sending messages.
        _serializer (Serializer): The serializer used to serialize messages (default JSONSerializer).
        _subscribers (Dict[str, List[asyncio.Queue]]): Mapping of topics to lists of subscriber queues.
        _response_futures (Dict[str, asyncio.Future]): Futures for pending responses keyed by correlation IDs.
        _running (bool): Flag indicating whether the message bus is currently running.
        _processing_task (Optional[asyncio.Task]): The async task responsible for processing messages.
        _message_count (int): Count of processed messages.
        _processing_times (List[float]): Recorded processing times for messages.
        _message_queue (asyncio.Queue): Queue for incoming messages with a size limit.
        _rate_limiter (RateLimiter): Rate limiter enforcing a limit on messages per second.
        _circuit_breaker (CircuitBreaker): Circuit breaker to prevent executing failing operations.
        _priority_queue (asyncio.PriorityQueue): Priority queue for ordering message processing.
        _error_handler (ExceptionHandler): Centralized error handling mechanism.
        _message_retry_config (RetryConfig): Configuration for retry logic on message delivery failures.
    Methods:
        __init__(tcp_client, serializer=JSONSerializer()):
            Initializes the MessageBus instance, sets up queues, rate limiter, circuit breaker,
            and error handlers, and registers TCP client callbacks.
        _setup_error_handlers():
            Registers custom asynchronous error handlers for MessageDeliveryException
            and ConnectionException, providing logic for retries and reconnection.
        async _handle_message_failure(exc: MessageDeliveryException):
            Handles cases where message delivery fails permanently, potentially persisting
            the failed message (e.g., to a dead-letter queue or database).
        async start():
            Starts the message bus by initiating the asynchronous task to process messages.
        async stop():
            Stops the message bus by terminating the message processing task gracefully.
        async publish(topic: str, data: Any, priority: int = 0) -> None:
            Publishes a message on a specified topic. Prior to sending, it enforces rate limiting,
            wraps the send operation with a circuit breaker for resilience, and encapsulates errors
            using custom exception handling.
        def _create_message(topic: str, data: Any) -> Message:
            Constructs a new Message instance with a unique ID, specified topic and data, and
            attaches metadata such as a timestamp.
        async request(topic: str, data: Any, timeout: float = 30.0) -> Any:
            Sends a request-type message and awaits a corresponding response. It creates a future
            linked to a unique correlation ID, sends the message over the TCP client, and waits for
            a response within the specified timeout period.
        async subscribe(topic: str) -> asyncio.Queue:
            Subscribes to a given topic by creating and returning a new asyncio queue where messages
            are delivered for that topic.
        async unsubscribe(topic: str, queue: asyncio.Queue) -> None:
            Unsubscribes a previously registered queue from the specified topic, removing it from the
            subscription list.
        async _process_messages():
            Continuously processes incoming messages from the priority queue. Each message is handled
            using the _handle_message method and wrapped in an error boundary to catch and report exceptions.
        async _handle_message(message: Message):
            Processes an individual message. If the message is a response with a matching correlation
            ID, it resolves the corresponding future; otherwise, it dispatches the message to all subscriber
            queues. It also tracks processing time and updates related metrics.
        def _update_metrics(processing_time: float):
            Updates the metrics of the message bus by incrementing the message count and appending the
            latest processing time, while maintaining a bounded record for performance analysis.
        def _handle_tcp_message(data: bytes):
            Callback for processing incoming TCP messages. (Implement additional processing as needed.)
        def _handle_tcp_error(error: Exception):
            Callback for handling errors reported by the TCP client. (Implement custom error handling logic.)
            Returns a dictionary containing key performance and state metrics of the message bus, such as:
                - message_count: Total number of processed messages.
                - average_processing_time: Average time taken to process messages.
                - active_subscribers: Total number of currently active subscriber queues.
                - pending_responses: Number of pending response futures.
    """
    """High-performance asynchronous message bus"""

    def __init__(self, tcp_client: 'TCPClient', serializer: Serializer = JSONSerializer()):
        self._tcp_client = tcp_client
        self._serializer = serializer
        self._subscribers: Dict[str, List[asyncio.Queue]] = defaultdict(list)
        self._response_futures: Dict[str, asyncio.Future] = {}
        self._running = False
        self._processing_task: Optional[asyncio.Task] = None

        # Message handling metrics
        self._message_count = 0
        self._processing_times: List[float] = []

        # Register TCP client callbacks
        self._tcp_client.on('message', self._handle_tcp_message)
        self._tcp_client.on('error', self._handle_tcp_error)

        self._message_queue = asyncio.Queue(maxsize=1000)  # 限制队列大小
        self._rate_limiter = RateLimiter(rate_limit=500)  # 限制每秒500条消息
        self._circuit_breaker = CircuitBreaker()
        self._priority_queue = asyncio.PriorityQueue(maxsize=1000)
        self._error_handler = ExceptionHandler()
        self._setup_error_handlers()
        self._message_retry_config = RetryConfig(
            max_retries=3,
            delay=1.0,
            backoff_factor=2.0,
            exceptions=(MessageDeliveryException,)
        )
        self._command_dispatcher = None
        self._protocol_converter = None
        self._ssh_forwarder = None
        self._components = {}
        self._transformers = []
        self._component_routes = {}
        self._message_pool = asyncio.Queue(maxsize=10000)
        self._worker_count = 4
        self._workers = []
        # 添加与事件总线的集成点
        self._event_bus = None
        self._plugin_manager = None
        self._integration_manager = None

    async def set_command_dispatcher(self, dispatcher):
        """设置命令分发器"""
        self._command_dispatcher = dispatcher
        logger.info("命令分发器已集成到消息总线")

    async def set_protocol_converter(self, converter):
        """设置协议转换器"""
        self._protocol_converter = converter
        logger.info("协议转换器已集成到消息总线")

    async def set_ssh_forwarder(self, forwarder):
        """设置SSH转发器"""
        self._ssh_forwarder = forwarder
        logger.info("SSH转发器已集成到消息总线")

    async def register_component(self, name: str, component: Any):
        """注册组件到消息总线"""
        self._components[name] = component
        if hasattr(component, 'set_message_bus'):
            await component.set_message_bus(self)
        logger.info(f"组件 {name} 已注册到消息总线")

    async def register_transformer(self, transformer):
        """注册消息转换器"""
        self._transformers.append(transformer)
        logger.info(f"消息转换器 {transformer.__class__.__name__} 已注册")

    async def set_event_bus(self, event_bus):
        """设置事件总线引用"""
        self._event_bus = event_bus
        logger.info("事件总线已集成到消息总线")

    async def set_plugin_manager(self, plugin_manager):
        """设置插件管理器引用"""
        self._plugin_manager = plugin_manager
        logger.info("插件管理器已集成到消息总线")
        
    async def set_integration_manager(self, integration_manager):
        """设置集成管理器引用"""
        self._integration_manager = integration_manager
        logger.info("集成管理器已集成到消息总线")

    def _setup_error_handlers(self):
        """设置错误处理器"""
        async def handle_delivery_error(exc: MessageDeliveryException):
            logger.error(
                f"Message delivery failed: {exc.message}, retries: {exc.retry_count}")
            if exc.retry_count >= self._message_retry_config.max_retries:
                await self._handle_message_failure(exc)
            return {"status": "failed", "error": str(exc)}

        async def handle_connection_error(exc: ConnectionException):
            logger.critical(f"Connection error: {exc.message}")
            await self._tcp_client.reconnect()
            return {"status": "reconnecting", "error": str(exc)}

        self._error_handler.register(
            MessageDeliveryException, handle_delivery_error)
        self._error_handler.register(
            ConnectionException, handle_connection_error)

    async def _handle_message_failure(self, exc: MessageDeliveryException):
        """处理消息最终失败的情况"""
        # 实现消息持久化或死信队列逻辑
        logger.error(f"Message permanently failed: {exc}")
        # 可以将消息保存到死信队列或数据库中
        pass

    async def start(self):
        """启动消息总线和工作线程"""
        self._running = True
        self._workers = [
            asyncio.create_task(self._process_message_pool())
            for _ in range(self._worker_count)
        ]

    async def stop(self):
        """Stop the message bus"""
        self._running = False
        if self._processing_task:
            await self._processing_task
            self._processing_task = None

    async def _process_message_pool(self):
        """工作线程处理消息池"""
        while self._running:
            try:
                message = await self._message_pool.get()
                await self._handle_message(message)
                self._message_pool.task_done()
            except Exception as e:
                logger.error(f"Message processing error: {e}")

    async def register_component(self, prefix: str, component: BaseComponent):
        """注册组件和路由前缀"""
        self._component_routes[prefix] = component
        await component.set_message_bus(self)

    @error_boundary(error_handler=_error_handler)
    @async_performance_monitor()
    async def publish(self, topic: str, data: Any, priority: int = 0) -> None:
        """优化的消息发布"""
        # 应用消息转换器
        for transformer in self._transformers:
            data = await transformer.transform(data)

        # 转发到插件系统
        if self._plugin_manager:
            try:
                # 检查是否有插件注册了对此主题的处理
                plugin_handled = await self._plugin_manager.handle_topic(topic, data)
                if plugin_handled:
                    # 如果插件处理了消息，记录但继续处理
                    logger.debug(f"消息主题 {topic} 由插件系统处理")
            except Exception as e:
                logger.error(f"插件处理消息失败: {e}")
        
        # 转发到事件总线系统
        if self._event_bus and topic.startswith("event."):
            try:
                # 从event.xxx格式转换为xxx事件名
                event_name = topic[6:]
                await self._event_bus.publish(event_name, data)
                logger.debug(f"消息主题 {topic} 转发到事件总线: {event_name}")
            except Exception as e:
                logger.error(f"转发到事件总线失败: {e}")

        # 按主题前缀路由到对应组件
        prefix = topic.split('.')[0]
        if prefix in self._components:
            await self._components[prefix].handle_message(topic, data)
            return

        if self._protocol_converter and isinstance(data, dict):
            data = await self._protocol_converter.to_wire_format(data)
        
        if topic.startswith('cmd.'):
            if self._command_dispatcher:
                await self._command_dispatcher.dispatch(topic[4:], data)
                return

        if topic.startswith('ssh.'):
            if self._ssh_forwarder:
                await self._ssh_forwarder.handle_message(topic[4:], data)
                return

        if not await self._rate_limiter.acquire():
            raise MessageDeliveryException("Rate limit exceeded")

        try:
            message = self._create_message(topic, data)
            await self._circuit_breaker.execute(
                self._tcp_client.send,
                self._serializer.serialize(message)
            )
        except Exception as e:
            raise MessageDeliveryException(
                f"Failed to publish message: {str(e)}"
            ) from e

    def _create_message(self, topic: str, data: Any) -> Message:
        return Message(
            id=str(uuid.uuid4()),
            type=MessageType.EVENT,
            topic=topic,
            data=data,
            metadata={'timestamp': asyncio.get_event_loop().time()}
        )

    async def request(self, topic: str, data: Any, timeout: float = 30.0) -> Any:
        """Send a request and wait for response"""
        correlation_id = str(uuid.uuid4())
        message = Message(
            id=str(uuid.uuid4()),
            type=MessageType.REQUEST,
            topic=topic,
            data=data,
            correlation_id=correlation_id,
            metadata={'timestamp': asyncio.get_event_loop().time()}
        )

        # Create future for response
        future = asyncio.get_event_loop().create_future()
        self._response_futures[correlation_id] = future

        try:
            await self._tcp_client.send(self._serializer.serialize(message))
            return await asyncio.wait_for(future, timeout)
        finally:
            self._response_futures.pop(correlation_id, None)

    async def subscribe(self, topic: str) -> asyncio.Queue:
        """Subscribe to a topic"""
        queue = asyncio.Queue()
        self._subscribers[topic].append(queue)
        return queue

    async def unsubscribe(self, topic: str, queue: asyncio.Queue) -> None:
        """Unsubscribe from a topic"""
        if topic in self._subscribers and queue in self._subscribers[topic]:
            self._subscribers[topic].remove(queue)
            if not self._subscribers[topic]:
                del self._subscribers[topic]

    @error_boundary(error_handler=_error_handler)
    async def _process_messages(self):
        """Process incoming messages with error handling"""
        while self._running:
            try:
                priority, message = await self._priority_queue.get()
                await self._handle_message(message)
                self._priority_queue.task_done()
            except Exception as e:
                raise MessageProcessingException(
                    f"Error processing message: {str(e)}"
                ) from e

    async def _handle_message(self, message: Message):
        """Enhanced message handling with transformers"""
        try:
            # 应用消息转换器
            for transformer in self._transformers:
                message.data = await transformer.transform(message.data)

            start_time = asyncio.get_event_loop().time()

            # 处理响应消息
            if message.type == MessageType.RESPONSE and message.correlation_id:
                if message.correlation_id in self._response_futures:
                    self._response_futures[message.correlation_id].set_result(
                        message.data)
                    return

            # 分发到订阅者
            if message.topic in self._subscribers:
                await asyncio.gather(*[
                    queue.put(message) for queue in self._subscribers[message.topic]
                ])

            # 如果是事件消息，转发到事件总线
            if message.topic.startswith("event.") and self._event_bus:
                event_name = message.topic[6:]
                await self._event_bus.publish(event_name, message.data)

            # 更新指标
            processing_time = asyncio.get_event_loop().time() - start_time
            self._update_metrics(processing_time)

        except Exception as e:
            # 更新错误统计
            await self._error_handler.handle(
                MessageProcessingException(
                    f"Message handling failed: {str(e)}")
            )

    def _update_metrics(self, processing_time: float):
        """更新性能指标"""
        self._message_count += 1
        self._processing_times.append(processing_time)
        # 保持最近1000条消息的处理时间记录
        if len(self._processing_times) > 1000:
            self._processing_times.pop(0)

    def _handle_tcp_message(self, data: bytes):
        """Handle TCP client messages"""
        # Additional processing if needed
        pass

    def _handle_tcp_error(self, error: Exception):
        """Handle TCP client errors"""
        # Error handling logic
        pass

    @property
    def metrics(self) -> Dict[str, Any]:
        """Get message bus metrics"""
        metrics = {
            'message_count': self._message_count,
            'average_processing_time': sum(self._processing_times) / len(self._processing_times) if self._processing_times else 0,
            'active_subscribers': sum(len(subs) for subs in self._subscribers.values()),
            'pending_responses': len(self._response_futures),
            'transformers_count': len(self._transformers),
            'components_count': len(self._components)
        }
        
        # 添加组件统计
        if self._components:
            metrics['components'] = list(self._components.keys())
            
        return metrics

# Extended TCP Client with message bus support


class MessageBusEnabledTCPClient(TCPClient):
    """TCP Client with integrated message bus support"""

    def __init__(self, config: ClientConfig):
        super().__init__(config)
        self.message_bus: Optional[MessageBus] = None

    async def connect(self) -> None:
        """Connect and initialize message bus"""
        await super().connect()
        self.message_bus = MessageBus(self)
        await self.message_bus.start()

    async def disconnect(self) -> None:
        """Disconnect and stop message bus"""
        if self.message_bus:
            await self.message_bus.stop()
        await super().disconnect()
