from typing import Any, Dict, List, Optional, Type, Generic, TypeVar
from dataclasses import dataclass
import asyncio
import json
from collections import defaultdict
import uuid
from enum import Enum
from abc import ABC, abstractmethod

from loguru import logger

from app.utils.error_handler import CircuitBreaker
from app.utils.performance import RateLimiter, async_performance_monitor
from app.client.tcp.client import ClientConfig, TCPClient

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
        self._error_handler = self._setup_error_handler()

    def _setup_error_handler(self):
        async def handler(error: Exception):
            logger.error(f"MessageBus Error: {error}")
            # 添加错误恢复逻辑

        return handler

    async def start(self):
        """Start the message bus"""
        self._running = True
        self._processing_task = asyncio.create_task(self._process_messages())

    async def stop(self):
        """Stop the message bus"""
        self._running = False
        if self._processing_task:
            await self._processing_task
            self._processing_task = None

    @async_performance_monitor()
    async def publish(self, topic: str, data: Any, priority: int = 0) -> None:
        """Enhanced publish with rate limiting and circuit breaker"""
        if not await self._rate_limiter.acquire():
            await self._handle_rate_limit_exceeded()
            return

        message = self._create_message(topic, data)
        await self._priority_queue.put((priority, message))

    async def _handle_rate_limit_exceeded(self):
        # 处理速率限制超出情况
        logger.warning("Message rate limit exceeded")
        # 实现背压机制

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

    async def _process_messages(self):
        """Process incoming messages"""
        while self._running:
            try:
                data = await self._tcp_client.receive()
                if data:
                    start_time = asyncio.get_event_loop().time()
                    message = self._serializer.deserialize(data)

                    # Handle response messages
                    if message.type == MessageType.RESPONSE and message.correlation_id:
                        if message.correlation_id in self._response_futures:
                            self._response_futures[message.correlation_id].set_result(
                                message.data)

                    # Distribute to subscribers
                    if message.topic in self._subscribers:
                        tasks = [
                            queue.put(message)
                            for queue in self._subscribers[message.topic]
                        ]
                        await asyncio.gather(*tasks)

                    # Update metrics
                    processing_time = asyncio.get_event_loop().time() - start_time
                    self._processing_times.append(processing_time)

            except Exception as e:
                print(f"Error processing message: {e}")

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
        return {
            'message_count': self._message_count,
            'average_processing_time': sum(self._processing_times) / len(self._processing_times) if self._processing_times else 0,
            'active_subscribers': sum(len(subs) for subs in self._subscribers.values()),
            'pending_responses': len(self._response_futures)
        }

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
