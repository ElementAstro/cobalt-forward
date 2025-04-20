from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List
import time
import json
from .constants import MQTTQoS
import asyncio
import uuid


@dataclass
class RetryConfig:
    """Retry configuration for message delivery"""
    max_retries: int = 3
    retry_interval: float = 1.0
    retry_backoff: float = 1.5


@dataclass
class PerformanceMetrics:
    """Performance metrics for monitoring"""
    publish_count: int = 0
    publish_failures: int = 0
    message_size_total: int = 0
    message_processing_time: float = 0
    retry_count: int = 0
    last_reset: float = field(default_factory=time.time)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    @property
    def average_message_size(self) -> float:
        """Calculate average message size in bytes"""
        return self.message_size_total / max(1, self.publish_count)

    @property
    def success_rate(self) -> float:
        """Calculate message delivery success rate"""
        return 1 - (self.publish_failures / max(1, self.publish_count))

    async def update_async(self, success: bool, message_size: int, process_time: float):
        """Update metrics asynchronously"""
        async with self._lock:
            self.publish_count += 1
            if not success:
                self.publish_failures += 1
            self.message_size_total += message_size
            self.message_processing_time += process_time
            self.retry_count += (0 if success else 1)

    def reset(self):
        """Reset all metrics"""
        self.__init__()


@dataclass
class MQTTConfig:
    """MQTT client configuration"""
    broker: str
    port: int
    client_id: str
    username: Optional[str] = None
    password: Optional[str] = None
    keepalive: int = 60
    ssl_config: Optional[Dict] = None
    clean_session: bool = True
    reconnect_delay: int = 5
    max_reconnect_attempts: int = -1  # -1 means infinite retries
    retry_config: RetryConfig = field(default_factory=RetryConfig)
    message_cache_size: int = 1000
    batch_size: int = 100
    heartbeat_interval: int = 30
    circuit_breaker_threshold: int = 5
    circuit_breaker_reset_timeout: float = 60.0

    def validate(self) -> bool:
        """Validate configuration parameters"""
        if not self.broker or not self.client_id:
            return False
        if self.port < 0 or self.port > 65535:
            return False
        return True


@dataclass
class MQTTMessage:
    """MQTT message with metadata"""
    topic: str
    payload: Any
    qos: MQTTQoS = MQTTQoS.AT_MOST_ONCE
    retain: bool = False
    timestamp: float = field(default_factory=time.time)
    message_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    retry_count: int = 0
    
    def to_json(self) -> str:
        """Convert message to JSON string"""
        return json.dumps({
            'topic': self.topic,
            'payload': self.payload,
            'qos': self.qos.value,
            'retain': self.retain,
            'timestamp': self.timestamp,
            'message_id': self.message_id
        })

    @staticmethod
    def from_json(json_str: str) -> 'MQTTMessage':
        """Create message from JSON string"""
        data = json.loads(json_str)
        return MQTTMessage(
            topic=data['topic'],
            payload=data['payload'],
            qos=MQTTQoS(data['qos']),
            retain=data.get('retain', False),
            timestamp=data.get('timestamp', time.time()),
            message_id=data.get('message_id', str(uuid.uuid4()))
        )


@dataclass
class MQTTSubscription:
    """MQTT subscription information"""
    topic: str
    qos: MQTTQoS = MQTTQoS.AT_MOST_ONCE
    callback: Optional[callable] = None
    created_at: float = field(default_factory=time.time)


@dataclass
class MQTTBatchMessage:
    """MQTT batch message container"""
    messages: List[MQTTMessage]
    created_at: float = field(default_factory=time.time)
    batch_id: str = field(default_factory=lambda: str(uuid.uuid4()))

    def __len__(self) -> int:
        return len(self.messages)

    def __iter__(self):
        return iter(self.messages)
