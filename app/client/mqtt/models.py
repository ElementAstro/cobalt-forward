from dataclasses import dataclass
from typing import Optional, Dict, Any
import time
import json
from .constants import MQTTQoS

@dataclass
class RetryConfig:
    """消息重试配置"""
    max_retries: int = 3
    retry_interval: float = 1.0
    retry_backoff: float = 1.5

class PerformanceMetrics:
    """性能指标类"""
    def __init__(self):
        self.publish_count = 0
        self.publish_failures = 0
        self.message_size_total = 0
        self.message_processing_time = 0
        self.retry_count = 0
        self.last_reset = time.time()

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

class MQTTMessage:
    """MQTT消息封装类"""
    def __init__(self, 
                 topic: str, 
                 payload: Any, 
                 qos: MQTTQoS = MQTTQoS.AT_MOST_ONCE,
                 retain: bool = False):
        self.topic = topic
        self.payload = payload
        self.qos = qos
        self.retain = retain
        self.timestamp = time.time()
        self.message_id = None

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
