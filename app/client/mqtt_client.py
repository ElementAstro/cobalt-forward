import logging
from loguru import logger
import paho.mqtt.client as mqtt
import json
import time
import ssl
import threading
from typing import Callable, Dict, Any, Optional, List
from dataclasses import dataclass
from enum import Enum
import queue


class MQTTQoS(Enum):
    """MQTT QoS级别"""
    AT_MOST_ONCE = 0
    AT_LEAST_ONCE = 1
    EXACTLY_ONCE = 2


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
    max_reconnect_attempts: int = -1  # -1表示无限重试


class MQTTMessage:
    """MQTT消息封装类"""

    def __init__(self, topic: str, payload: Any, qos: MQTTQoS = MQTTQoS.AT_MOST_ONCE):
        self.topic = topic
        self.payload = payload
        self.qos = qos
        self.timestamp = time.time()

    def to_json(self) -> str:
        """将消息转换为JSON字符串"""
        return json.dumps({
            'topic': self.topic,
            'payload': self.payload,
            'qos': self.qos.value,
            'timestamp': self.timestamp
        })

    @staticmethod
    def from_json(json_str: str) -> 'MQTTMessage':
        """从JSON字符串创建消息对象"""
        data = json.loads(json_str)
        return MQTTMessage(
            topic=data['topic'],
            payload=data['payload'],
            qos=MQTTQoS(data['qos'])
        )


class MQTTCallback:
    """回调处理类"""

    def __init__(self, callback: Callable, topics: List[str]):
        self.callback = callback
        self.topics = topics


class MQTTClient:
    """增强的MQTT客户端"""

    def __init__(self, config: MQTTConfig):
        self.config = config
        self.client = mqtt.Client(
            client_id=config.client_id, clean_session=config.clean_session)
        self.connected = False
        self.callbacks: Dict[str, List[MQTTCallback]] = {}
        self.reconnect_count = 0
        self.message_queue = queue.Queue()
        self.stopping = False

        logger.info(f"Initializing MQTT client with ID: {config.client_id}")
        logger.debug(f"Client configuration: {config}")

        # 设置回调
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message
        self.client.on_publish = self._on_publish
        self.client.on_subscribe = self._on_subscribe

        # 设置认证
        if config.username and config.password:
            self.client.username_pw_set(config.username, config.password)
            logger.debug("Client authentication configured")

        # 设置SSL/TLS
        if config.ssl_config:
            self.client.tls_set(**config.ssl_config)
            logger.debug("SSL/TLS configuration applied")

        # 启动消息处理线程
        self.message_processor = threading.Thread(
            target=self._process_message_queue)
        self.message_processor.daemon = True
        self.message_processor.start()
        logger.debug("Message processor thread started")

    def _on_connect(self, client, userdata, flags, rc):
        """连接回调"""
        if rc == 0:
            self.connected = True
            self.reconnect_count = 0
            logger.success(f"Connected to MQTT broker: {self.config.broker}")
            # 重新订阅所有主题
            for topic in self.callbacks.keys():
                self.client.subscribe(topic)
                logger.debug(f"Resubscribed to topic: {topic}")
        else:
            logger.error(f"Connection failed with code {rc}")

    def _on_disconnect(self, client, userdata, rc):
        """断开连接回调"""
        self.connected = False
        logger.warning(f"Disconnected from MQTT broker: {self.config.broker}")
        if not self.stopping:
            self._handle_reconnection()

    def _handle_reconnection(self):
        """处理重连"""
        while not self.connected and not self.stopping:
            if self.config.max_reconnect_attempts != -1 and self.reconnect_count >= self.config.max_reconnect_attempts:
                logger.error("Max reconnection attempts reached, giving up")
                break

            self.reconnect_count += 1
            try:
                logger.info(
                    f"Attempting to reconnect... (attempt {self.reconnect_count}/{self.config.max_reconnect_attempts if self.config.max_reconnect_attempts != -1 else 'infinite'})")
                self.client.reconnect()
                break
            except Exception as e:
                logger.error(f"Reconnection failed: {str(e)}", exc_info=True)
                time.sleep(self.config.reconnect_delay)

    def _on_message(self, client, userdata, msg):
        """消息接收回调"""
        self.message_queue.put((msg.topic, msg.payload))

    def _process_message_queue(self):
        """处理消息队列"""
        while not self.stopping:
            try:
                topic, payload = self.message_queue.get(timeout=1)
                self._handle_message(topic, payload)
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(
                    f"Error processing message: {str(e)}", exc_info=True)

    def _handle_message(self, topic: str, payload):
        """处理接收到的消息"""
        try:
            decoded_payload = payload.decode()
            logger.trace(
                f"Received message on topic {topic}: {decoded_payload}")

            # 尝试解析JSON
            try:
                payload_data = json.loads(decoded_payload)
                logger.debug(f"Successfully parsed JSON payload")
            except json.JSONDecodeError:
                payload_data = decoded_payload
                logger.debug("Using raw payload (not JSON)")

            # 调用相关的回调函数
            for topic_pattern, callbacks in self.callbacks.items():
                if mqtt.topic_matches_sub(topic_pattern, topic):
                    logger.debug(f"Topic match found: {topic_pattern}")
                    for callback in callbacks:
                        try:
                            callback.callback(topic, payload_data)
                            logger.trace(f"Callback executed successfully")
                        except Exception as e:
                            logger.error(
                                f"Callback error: {str(e)}", exc_info=True)
        except Exception as e:
            logger.error(f"Message handling error: {str(e)}", exc_info=True)

    def _on_publish(self, client, userdata, mid):
        """发布回调"""
        logger.debug(f"Message {mid} published")

    def _on_subscribe(self, client, userdata, mid, granted_qos):
        """订阅回调"""
        logger.debug(f"Subscribed with QoS {granted_qos}")

    def connect(self):
        """连接到MQTT代理"""
        try:
            self.client.connect(
                self.config.broker,
                self.config.port,
                self.config.keepalive
            )
            self.client.loop_start()
            return True
        except Exception as e:
            logger.error(f"Connection error: {e}")
            return False

    def disconnect(self):
        """断开连接"""
        self.stopping = True
        self.client.loop_stop()
        self.client.disconnect()

    def publish(self, message: MQTTMessage) -> bool:
        """发布消息"""
        try:
            if isinstance(message.payload, (dict, list)):
                payload = json.dumps(message.payload)
            else:
                payload = str(message.payload)

            result = self.client.publish(
                message.topic,
                payload,
                qos=message.qos.value
            )
            return result.rc == mqtt.MQTT_ERR_SUCCESS
        except Exception as e:
            logger.error(f"Publish error: {e}")
            return False

    def subscribe(self, topic: str, callback: Callable, qos: MQTTQoS = MQTTQoS.AT_MOST_ONCE):
        """订阅主题"""
        if topic not in self.callbacks:
            self.callbacks[topic] = []

        self.callbacks[topic].append(MQTTCallback(callback, [topic]))
        result = self.client.subscribe(topic, qos.value)
        return result[0] == mqtt.MQTT_ERR_SUCCESS

    def unsubscribe(self, topic: str):
        """取消订阅"""
        if topic in self.callbacks:
            del self.callbacks[topic]
        return self.client.unsubscribe(topic)


class MQTTManager:
    """MQTT管理器，用于管理多个MQTT客户端"""

    def __init__(self):
        self.clients: Dict[str, MQTTClient] = {}

    def add_client(self, client_id: str, config: MQTTConfig) -> MQTTClient:
        """添加新的MQTT客户端"""
        if client_id in self.clients:
            logger.warning(f"Client {client_id} already exists")
            return self.clients[client_id]

        client = MQTTClient(config)
        self.clients[client_id] = client
        return client

    def remove_client(self, client_id: str):
        """移除MQTT客户端"""
        if client_id in self.clients:
            self.clients[client_id].disconnect()
            del self.clients[client_id]

    def get_client(self, client_id: str) -> Optional[MQTTClient]:
        """获取指定的MQTT客户端"""
        return self.clients.get(client_id)

    def disconnect_all(self):
        """断开所有客户端连接"""
        for client in self.clients.values():
            client.disconnect()
        self.clients.clear()
