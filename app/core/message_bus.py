from typing import Any, Dict, List, Optional, Generic, TypeVar, cast, Protocol
from dataclasses import dataclass, field
import asyncio
from asyncio import Queue, Future, Task
import json
from collections import defaultdict
import uuid
import time
from enum import Enum
from abc import ABC, abstractmethod

from loguru import logger

from app.core.base import BaseComponent, MessageBus as BaseMessageBus
from app.utils.error_handler import CircuitBreaker
from app.utils.performance import RateLimiter, async_performance_monitor
from app.client.tcp.client import ClientConfig, TCPClient
from app.utils.error_handler import ExceptionHandler, RetryConfig, error_boundary
from app.core.exceptions import (
    MessageDeliveryException,
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
    """Message class containing topic, data and metadata"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    topic: str = ""
    data: Optional[T] = None
    message_type: MessageType = MessageType.EVENT
    created_at: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)
    correlation_id: Optional[str] = None
    priority: int = 0
    ttl: int = 3600  # Message Time to Live (seconds)

    def __lt__(self, other: Any) -> bool:
        """Support comparison based on priority, for priority queues"""
        if not isinstance(other, Message):
            return NotImplemented
        return (self.priority > other.priority or
                (self.priority == other.priority and self.created_at < other.created_at))


class Serializer(ABC):
    """Abstract base class for message serializers"""
    @abstractmethod
    def serialize(self, message: Message[Any]) -> bytes:
        pass

    @abstractmethod
    def deserialize(self, data: bytes) -> Message[Any]:
        pass


class JSONSerializer(Serializer):
    """JSON implementation of message serializer"""

    def serialize(self, message: Message[Any]) -> bytes:
        return json.dumps({
            'id': message.id,
            'type': message.message_type.value,
            'topic': message.topic,
            'data': message.data,
            'correlation_id': message.correlation_id,
            'metadata': message.metadata
        }).encode('utf-8')

    def deserialize(self, data: bytes) -> Message[Any]:
        msg_dict = json.loads(data.decode('utf-8'))
        return Message(
            id=msg_dict['id'],
            message_type=MessageType(msg_dict['type']),
            topic=msg_dict['topic'],
            data=msg_dict['data'],
            correlation_id=msg_dict['correlation_id'],
            metadata=msg_dict['metadata']
        )


class EventBusProtocol(Protocol):
    """Event bus protocol for type checking"""

    async def publish(self, topic: str, data: Any) -> None:
        """Publish event to a topic"""
        ...


class PluginManagerProtocol(Protocol):
    """Plugin manager protocol for type checking"""

    async def handle_topic(self, topic: str, data: Any) -> bool:
        """Handle topic and return whether it was processed"""
        ...


class IntegrationManagerProtocol(Protocol):
    """Integration manager protocol for type checking"""
    pass


class ProtocolConverterProtocol(Protocol):
    """Protocol converter protocol for type checking"""

    async def to_wire_format(self, data: Dict[str, Any]) -> Any:
        """Convert data to wire format"""
        ...


class SSHForwarderProtocol(Protocol):
    """SSH forwarder protocol for type checking"""

    async def handle_message(self, topic: str, data: Any) -> None:
        """Handle SSH message"""
        ...


class TransformerProtocol(Protocol):
    """Transformer protocol for type checking"""

    async def transform(self, data: Any) -> Any:
        """Transform data"""
        ...


class CommandDispatcherProtocol(Protocol):
    """Command dispatcher protocol for type checking"""

    async def dispatch(self, command_name: str, data: Any) -> Any:
        """Dispatch command"""
        ...


class TCPClientCallback(Protocol):
    """Protocol for TCP client callbacks"""

    def __call__(self, *args: Any) -> None:
        """Callback function"""
        ...


class MessageBus(BaseMessageBus):
    """High-performance asynchronous message bus"""

    def __init__(self, tcp_client: TCPClient, serializer: Serializer = JSONSerializer()):
        self._tcp_client = tcp_client
        self._serializer = serializer
        self._subscribers: Dict[str,
                                List[Queue[Message[Any]]]] = defaultdict(list)
        self._response_futures: Dict[str, Future[Any]] = {}
        self._running = False
        self._processing_task: Optional[Task[None]] = None

        # Message handling metrics
        self._message_count = 0
        self._processing_times: List[float] = []

        # Register TCP client callbacks
        self._tcp_client.on('message', self._handle_tcp_message)
        self._tcp_client.on('error', self._handle_tcp_error)

        self._message_queue: Queue[Message[Any]] = asyncio.Queue(
            maxsize=1000)  # Limit queue size
        # Limit to 500 messages per second
        self._rate_limiter = RateLimiter(rate_limit=500)
        self._circuit_breaker = CircuitBreaker()
        self._priority_queue: asyncio.PriorityQueue[tuple[int, Message[Any]]] = asyncio.PriorityQueue(
            maxsize=1000)
        self._error_handler = ExceptionHandler()
        self._setup_error_handlers()
        self._message_retry_config = RetryConfig(
            max_retries=3,
            delay=1.0,
            backoff_factor=2.0,
            exceptions=(MessageDeliveryException,)
        )
        self._command_dispatcher: Optional[CommandDispatcherProtocol] = None
        self._protocol_converter: Optional[ProtocolConverterProtocol] = None
        self._ssh_forwarder: Optional[SSHForwarderProtocol] = None
        self._components: Dict[str, Any] = {}
        self._transformers: List[TransformerProtocol] = []
        self._component_routes: Dict[str, BaseComponent] = {}
        self._message_pool: Queue[Message[Any]] = asyncio.Queue(maxsize=10000)
        self._worker_count = 4
        self._workers: List[Task[None]] = []
        # Add integration points with event bus
        self._event_bus: Optional[EventBusProtocol] = None
        self._plugin_manager: Optional[PluginManagerProtocol] = None
        self._integration_manager: Optional[IntegrationManagerProtocol] = None

    async def set_command_dispatcher(self, dispatcher: CommandDispatcherProtocol) -> None:
        """Set command dispatcher"""
        self._command_dispatcher = dispatcher
        logger.info("Command dispatcher integrated with message bus")

    async def set_protocol_converter(self, converter: ProtocolConverterProtocol) -> None:
        """Set protocol converter"""
        self._protocol_converter = converter
        logger.info("Protocol converter integrated with message bus")

    async def set_ssh_forwarder(self, forwarder: SSHForwarderProtocol) -> None:
        """Set SSH forwarder"""
        self._ssh_forwarder = forwarder
        logger.info("SSH forwarder integrated with message bus")

    async def register_component_by_name(self, name: str, component: Any) -> None:
        """Register component to message bus by name"""
        self._components[name] = component
        if hasattr(component, 'set_message_bus'):
            await component.set_message_bus(self)
        logger.info(f"Component {name} registered to message bus")

    async def register_transformer(self, transformer: TransformerProtocol) -> None:
        """Register message transformer"""
        self._transformers.append(transformer)
        logger.info(
            f"Message transformer {transformer.__class__.__name__} registered")

    async def set_event_bus(self, event_bus: EventBusProtocol) -> None:
        """Set event bus reference"""
        self._event_bus = event_bus
        logger.info("Event bus integrated with message bus")

    async def set_plugin_manager(self, plugin_manager: PluginManagerProtocol) -> None:
        """Set plugin manager reference"""
        self._plugin_manager = plugin_manager
        logger.info("Plugin manager integrated with message bus")

    async def set_integration_manager(self, integration_manager: IntegrationManagerProtocol) -> None:
        """Set integration manager reference"""
        self._integration_manager = integration_manager
        logger.info("Integration manager integrated with message bus")

    def _setup_error_handlers(self) -> None:
        """Set up error handlers"""
        async def handle_delivery_error(exc: MessageDeliveryException) -> Dict[str, str]:
            logger.error(
                f"Message delivery failed: {exc.message}, retries: {exc.retry_count}")
            if exc.retry_count >= self._message_retry_config.max_retries:
                await self._handle_message_failure(exc)
            return {"status": "failed", "error": str(exc)}

        async def handle_connection_error(exc: ConnectionException) -> Dict[str, str]:
            logger.critical(f"Connection error: {exc.message}")
            await self._tcp_client.reconnect()
            return {"status": "reconnecting", "error": str(exc)}

        self._error_handler.register(
            MessageDeliveryException, handle_delivery_error)
        self._error_handler.register(
            ConnectionException, handle_connection_error)

    async def _handle_message_failure(self, exc: MessageDeliveryException) -> None:
        """Handle permanently failed messages"""
        # Implement message persistence or dead letter queue logic
        logger.error(f"Message permanently failed: {exc}")
        # Message could be saved to a dead letter queue or database

    async def start(self) -> None:
        """Start message bus and worker threads"""
        self._running = True
        self._workers = [
            asyncio.create_task(self._process_message_pool())
            for _ in range(self._worker_count)
        ]

    async def stop(self) -> None:
        """Stop the message bus"""
        self._running = False
        if self._processing_task:
            await self._processing_task
            self._processing_task = None

    async def _process_message_pool(self) -> None:
        """Worker thread to process message pool"""
        while self._running:
            try:
                message = await self._message_pool.get()
                await self._handle_message(message)
                self._message_pool.task_done()
            except Exception as e:
                logger.error(f"Message processing error: {e}")

    async def register_component(self, prefix: str, component: BaseComponent) -> None:
        """Register component and route prefix"""
        self._component_routes[prefix] = component
        # Pass self as MessageBus to component
        await component.set_message_bus(self)

    @error_boundary(error_handler=_error_handler)
    @async_performance_monitor()
    async def publish(self, topic: str, data: Any, priority: int = 0) -> None:
        """Optimized message publishing"""
        # Apply message transformers
        transformed_data = data
        for transformer in self._transformers:
            transformed_data = await transformer.transform(transformed_data)

        # Forward to plugin system
        if self._plugin_manager:
            try:
                # Check if a plugin has registered to handle this topic
                plugin_handled = await self._plugin_manager.handle_topic(topic, transformed_data)
                if plugin_handled:
                    # If plugin handled the message, log but continue processing
                    logger.debug(
                        f"Message topic {topic} handled by plugin system")
            except Exception as e:
                logger.error(f"Plugin message handling failed: {e}")

        # Forward to event bus system
        if self._event_bus and topic.startswith("event."):
            try:
                # Convert from event.xxx format to xxx event name
                event_name = topic[6:]
                await self._event_bus.publish(event_name, transformed_data)
                logger.debug(
                    f"Message topic {topic} forwarded to event bus: {event_name}")
            except Exception as e:
                logger.error(f"Forward to event bus failed: {e}")

        # Route to component by topic prefix
        prefix = topic.split('.')[0]
        if prefix in self._components:
            await self._components[prefix].handle_message(topic, transformed_data)
            return

        if self._protocol_converter and isinstance(transformed_data, dict):
            transformed_data = await self._protocol_converter.to_wire_format(transformed_data)

        if topic.startswith('cmd.'):
            if self._command_dispatcher:
                await self._command_dispatcher.dispatch(topic[4:], transformed_data)
                return

        if topic.startswith('ssh.'):
            if self._ssh_forwarder:
                await self._ssh_forwarder.handle_message(topic[4:], transformed_data)
                return

        if not await self._rate_limiter.acquire():
            raise MessageDeliveryException("Rate limit exceeded")

        try:
            message = self._create_message(topic, transformed_data)
            await self._circuit_breaker.execute(
                self._tcp_client.send,
                self._serializer.serialize(message)
            )
        except Exception as e:
            raise MessageDeliveryException(
                f"Failed to publish message: {str(e)}"
            ) from e

    def _create_message(self, topic: str, data: Any) -> Message[Any]:
        return Message[Any](
            id=str(uuid.uuid4()),
            topic=topic,
            data=data,
            message_type=MessageType.EVENT,
            metadata={'timestamp': asyncio.get_event_loop().time()}
        )

    async def request(self, topic: str, data: Any, timeout: float = 30.0) -> Any:
        """Send a request and wait for response"""
        correlation_id = str(uuid.uuid4())
        message = Message[Any](
            id=str(uuid.uuid4()),
            topic=topic,
            data=data,
            message_type=MessageType.REQUEST,
            correlation_id=correlation_id,
            metadata={'timestamp': asyncio.get_event_loop().time()}
        )

        # Create future for response
        future: Future[Any] = asyncio.get_event_loop().create_future()
        self._response_futures[correlation_id] = future

        try:
            await self._tcp_client.send(self._serializer.serialize(message))
            return await asyncio.wait_for(future, timeout)
        finally:
            self._response_futures.pop(correlation_id, None)

    async def subscribe(self, topic: str) -> Queue[Message[Any]]:
        """Subscribe to a topic"""
        queue: Queue[Message[Any]] = asyncio.Queue()
        self._subscribers[topic].append(queue)
        return queue

    async def unsubscribe(self, topic: str, queue: Queue[Message[Any]]) -> None:
        """Unsubscribe from a topic"""
        if topic in self._subscribers and queue in self._subscribers[topic]:
            self._subscribers[topic].remove(queue)
            if not self._subscribers[topic]:
                del self._subscribers[topic]

    @error_boundary(error_handler=_error_handler)
    async def _process_messages(self) -> None:
        """Process incoming messages with error handling"""
        while self._running:
            try:
                _, message = await self._priority_queue.get()
                await self._handle_message(message)
                self._priority_queue.task_done()
            except Exception as e:
                raise MessageProcessingException(
                    f"Error processing message: {str(e)}"
                ) from e

    async def _handle_message(self, message: Message[Any]) -> None:
        """Enhanced message handling with transformers"""
        try:
            # Apply message transformers
            transformed_data = message.data
            for transformer in self._transformers:
                if message.data is not None:
                    transformed_data = await transformer.transform(message.data)
                    message.data = transformed_data

            start_time = asyncio.get_event_loop().time()

            # Handle response messages
            if message.message_type == MessageType.RESPONSE and message.correlation_id:
                if message.correlation_id in self._response_futures:
                    self._response_futures[message.correlation_id].set_result(
                        message.data)
                    return

            # Distribute to subscribers
            if message.topic in self._subscribers:
                await asyncio.gather(*[
                    queue.put(message) for queue in self._subscribers[message.topic]
                ])

            # If it's an event message, forward to event bus
            if message.topic.startswith("event.") and self._event_bus:
                event_name = message.topic[6:]
                await self._event_bus.publish(event_name, message.data)

            # Update metrics
            processing_time = asyncio.get_event_loop().time() - start_time
            self._update_metrics(processing_time)

        except Exception as e:
            # Update error statistics
            await self._error_handler.handle(
                MessageProcessingException(
                    f"Message handling failed: {str(e)}")
            )

    def _update_metrics(self, processing_time: float) -> None:
        """Update performance metrics"""
        self._message_count += 1
        self._processing_times.append(processing_time)
        # Keep processing times for the last 1000 messages
        if len(self._processing_times) > 1000:
            self._processing_times.pop(0)

    def _handle_tcp_message(self, data: bytes) -> None:
        """Handle TCP client messages"""
        # Additional processing if needed

    def _handle_tcp_error(self, error: Exception) -> None:
        """Handle TCP client errors"""
        # Error handling logic

    @property
    def metrics(self) -> Dict[str, Any]:
        """Get message bus metrics"""
        metrics: Dict[str, Any] = {
            'message_count': self._message_count,
            'average_processing_time': sum(self._processing_times) / len(self._processing_times) if self._processing_times else 0,
            'active_subscribers': sum(len(subs) for subs in self._subscribers.values()),
            'pending_responses': len(self._response_futures),
            'transformers_count': len(self._transformers),
            'components_count': len(self._components)
        }

        # Add component statistics
        if self._components:
            metrics['components'] = list(self._components.keys())

        return metrics


# Extended TCP Client with message bus support
class MessageBusEnabledTCPClient(TCPClient):
    """TCP Client with integrated message bus support"""

    def __init__(self, config: ClientConfig):
        super().__init__(config)
        self.message_bus: Optional[MessageBus] = None

    async def connect(self) -> bool:
        """Connect and initialize message bus"""
        success = await super().connect()
        if success:
            self.message_bus = MessageBus(self)
            await self.message_bus.start()
        return success

    async def disconnect(self) -> None:
        """Disconnect and stop message bus"""
        if self.message_bus:
            await self.message_bus.stop()
        await super().disconnect()
