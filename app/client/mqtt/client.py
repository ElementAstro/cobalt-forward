import paho.mqtt.client as mqtt
from loguru import logger
import threading
import queue
from typing import Dict, List, Optional, Callable
from .models import MQTTConfig, MQTTMessage, PerformanceMetrics
from .constants import MQTTQoS, MQTT_CONNECTION_CODES
from .exceptions import ConnectionError, PublishError, SubscriptionError
from .utils import parse_payload
import time
import cachetools
from concurrent.futures import ThreadPoolExecutor
import asyncio

class MQTTCallback:
    """回调处理类"""
    def __init__(self, callback: Callable, topics: List[str]):
        self.callback = callback
        self.topics = topics
        self.created_at = time.time()

class MQTTClient:
    """增强的MQTT客户端"""
    def __init__(self, config: MQTTConfig):
        if not config.validate():
            raise ValueError("Invalid MQTT configuration")
            
        self.config = config
        self.client = mqtt.Client(client_id=config.client_id, 
                                clean_session=config.clean_session)
        self.connected = False
        self.callbacks: Dict[str, List[MQTTCallback]] = {}
        self.reconnect_count = 0
        self.message_queue = queue.Queue()
        self.stopping = False
        self.last_message_id = 0
        self.message_callbacks: Dict[int, Callable] = {}
        self.metrics = PerformanceMetrics()
        self.message_cache = cachetools.TTLCache(
            maxsize=config.message_cache_size,
            ttl=300
        )
        self.retry_queue = queue.PriorityQueue()
        self.batch_queue = queue.Queue(maxsize=config.batch_size)
        self.thread_pool = ThreadPoolExecutor(max_workers=4)

        # 设置基本回调
        self._setup_callbacks()
        
        # 设置认证和SSL
        self._configure_auth_and_ssl()

        # 启动消息处理线程
        self._start_message_processor()

        # 启动心跳检测
        self._start_heartbeat()
        # 启动重试处理器
        self._start_retry_processor()
        # 启动批处理器
        self._start_batch_processor()

    def _setup_callbacks(self):
        """设置回调函数"""
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message
        self.client.on_publish = self._on_publish
        self.client.on_subscribe = self._on_subscribe
        self.client.on_unsubscribe = self._on_unsubscribe

    def _configure_auth_and_ssl(self):
        """配置认证和SSL"""
        if self.config.username and self.config.password:
            self.client.username_pw_set(self.config.username, self.config.password)
        
        if self.config.ssl_config:
            self.client.tls_set(**self.config.ssl_config)

    def _start_message_processor(self):
        """启动消息处理线程"""
        self.message_processor = threading.Thread(target=self._process_message_queue)
        self.message_processor.daemon = True
        self.message_processor.start()

    def _start_heartbeat(self):
        """启动心跳检测"""
        def heartbeat_check():
            while not self.stopping:
                if self.connected:
                    self.client.ping()
                time.sleep(self.config.heartbeat_interval)

        self.heartbeat_thread = threading.Thread(target=heartbeat_check)
        self.heartbeat_thread.daemon = True
        self.heartbeat_thread.start()

    def _start_retry_processor(self):
        """启动重试处理器"""
        def retry_processor():
            while not self.stopping:
                try:
                    priority, message = self.retry_queue.get(timeout=1)
                    if message.retry_count < self.config.retry_config.max_retries:
                        message.retry_count += 1
                        backoff = self.config.retry_config.retry_interval * (
                            self.config.retry_config.retry_backoff ** message.retry_count
                        )
                        time.sleep(backoff)
                        self._publish_message(message)
                    else:
                        logger.error(f"Message {message.message_id} failed after max retries")
                except queue.Empty:
                    continue

        self.retry_thread = threading.Thread(target=retry_processor)
        self.retry_thread.daemon = True
        self.retry_thread.start()

    def _start_batch_processor(self):
        """启动批处理器"""
        def batch_processor():
            while not self.stopping:
                batch = []
                try:
                    while len(batch) < self.config.batch_size:
                        message = self.batch_queue.get(timeout=0.1)
                        batch.append(message)
                except queue.Empty:
                    pass
                
                if batch:
                    self._process_batch(batch)

        self.batch_thread = threading.Thread(target=batch_processor)
        self.batch_thread.daemon = True
        self.batch_thread.start()

    def _process_batch(self, batch: List[MQTTMessage]):
        """批量处理消息"""
        for message in batch:
            self.thread_pool.submit(self._process_single_message, message)

    async def publish_async(self, message: MQTTMessage) -> bool:
        """异步发布消息"""
        return await asyncio.get_event_loop().run_in_executor(
            self.thread_pool, 
            self.publish, 
            message
        )

    def _update_metrics(self, success: bool, message_size: int, processing_time: float):
        """更新性能指标"""
        self.metrics.publish_count += 1
        if not success:
            self.metrics.publish_failures += 1
        self.metrics.message_size_total += message_size
        self.metrics.message_processing_time += processing_time

    def get_metrics(self) -> Dict:
        """获取性能指标"""
        current_time = time.time()
        elapsed_time = current_time - self.metrics.last_reset
        
        return {
            'publish_rate': self.metrics.publish_count / elapsed_time,
            'failure_rate': self.metrics.publish_failures / max(1, self.metrics.publish_count),
            'average_message_size': self.metrics.message_size_total / max(1, self.metrics.publish_count),
            'average_processing_time': self.metrics.message_processing_time / max(1, self.metrics.publish_count),
            'retry_rate': self.metrics.retry_count / max(1, self.metrics.publish_count)
        }

    def _publish_message(self, message: MQTTMessage) -> bool:
        """实际发布消息的内部方法"""
        start_time = time.time()
        
        try:
            message_size = len(str(message.payload).encode('utf-8'))
            result = self.client.publish(
                message.topic,
                message.payload,
                qos=message.qos.value,
                retain=message.retain
            )
            
            success = result.rc == mqtt.MQTT_ERR_SUCCESS
            self._update_metrics(success, message_size, time.time() - start_time)
            
            if not success:
                self.retry_queue.put((time.time(), message))
            else:
                self.message_cache[message.message_id] = message
            
            return success
            
        except Exception as e:
            logger.error(f"Error publishing message: {str(e)}")
            self.retry_queue.put((time.time(), message))
            return False

    # ... (其他现有方法保持不变) ...

    def publish_with_callback(self, message: MQTTMessage, callback: Callable = None) -> int:
        """发布消息并设置回调"""
        self.last_message_id += 1
        message.message_id = self.last_message_id
        
        if callback:
            self.message_callbacks[message.message_id] = callback
            
        success = self.publish(message)
        if not success:
            del self.message_callbacks[message.message_id]
            raise PublishError(f"Failed to publish message {message.message_id}")
            
        return message.message_id

    def subscribe_pattern(self, pattern: str, callback: Callable, qos: MQTTQoS = MQTTQoS.AT_MOST_ONCE):
        """使用模式匹配订阅主题"""
        if not pattern:
            raise SubscriptionError("Invalid topic pattern")
            
        if pattern not in self.callbacks:
            self.callbacks[pattern] = []
            
        self.callbacks[pattern].append(MQTTCallback(callback, [pattern]))
        result = self.client.subscribe(pattern, qos.value)
        
        if result[0] != mqtt.MQTT_ERR_SUCCESS:
            raise SubscriptionError(f"Failed to subscribe to pattern {pattern}")
            
        return True
