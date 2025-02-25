from dataclasses import dataclass, field
from typing import Optional, Dict, Any
import time
import json
from .constants import MQTTQoS
import asyncio


@dataclass
class RetryConfig:
    """消息重试配置"""
    max_retries: int = 3
    retry_interval: float = 1.0
    retry_backoff: float = 1.5


@dataclass
class PerformanceMetrics:
    """优化的性能指标类"""
    publish_count: int = 0
    publish_failures: int = 0
    message_size_total: int = 0
    message_processing_time: float = 0
    retry_count: int = 0
    last_reset: float = field(default_factory=time.time)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    @property
    def average_message_size(self) -> float:
        return self.message_size_total / max(1, self.publish_count)

    @property
    def success_rate(self) -> float:
        return 1 - (self.publish_failures / max(1, self.publish_count))

    async def update_async(self, success: bool, message_size: int, process_time: float):
        async with self._lock:
            self.publish_count += 1
            if not success:
                self.publish_failures += 1
            self.message_size_total += message_size
            self.message_processing_time += process_time
            self.retry_count += (0 if success else 1)

    def reset(self):
        """重置指标"""
        self.__init__()


@dataclass
class MQTTConfig:
    """MQTT配置类"""
    broker: str
    port: int
    client_id: str
    username: Optional[str] = None
    password: Optional[str] = None
    keepalive: int = 60
    ssl_config: Optional[Dict] = None
    clean_session: bool = True
    reconnect_delay: int = 5
    max_reconnect_attempts: int = -1
    retry_config: RetryConfig = RetryConfig()
    message_cache_size: int = 1000
    batch_size: int = 100
    heartbeat_interval: int = 30

    def validate(self) -> bool:
        """验证配置有效性"""
        if not self.broker or not self.client_id:
            return False
        if self.port < 0 or self.port > 65535:
            return False
        return True


@dataclass(frozen=True)
class MQTTMessage:
    """不可变的MQTT消息类"""
    topic: str
    payload: Any
    qos: MQTTQoS = MQTTQoS.AT_MOST_ONCE
    retain: bool = False
    timestamp: float = field(default_factory=time.time)
    message_id: Optional[str] = None

    def to_json(self) -> str:
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
        data = json.loads(json_str)
        msg = MQTTMessage(
            topic=data['topic'],
            payload=data['payload'],
            qos=MQTTQoS(data['qos']),
            retain=data.get('retain', False)
        )
        msg.timestamp = data.get('timestamp', time.time())
        msg.message_id = data.get('message_id')
        return msg
