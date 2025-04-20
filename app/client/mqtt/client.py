from concurrent.futures import ThreadPoolExecutor
import time
import asyncio
import paho.mqtt.client as mqtt
from typing import Any, Dict, List, Optional, Callable, Set, Tuple, Union
import uuid
import cachetools
from loguru import logger

from .exceptions import (
    ConnectionError, 
    PublishError, 
    SubscriptionError, 
    ConfigurationError,
    CircuitBreakerOpenError,
    TimeoutError
)
from .constants import MQTTQoS, MQTT_CONNECTION_CODES
from .models import (
    MQTTConfig, 
    MQTTMessage, 
    MQTTSubscription, 
    MQTTBatchMessage,
    PerformanceMetrics, 
    RetryConfig
)
from .utils import (
    parse_payload, 
    measure_time, 
    async_measure_time, 
    validate_topic, 
    CircuitBreaker
)


class MQTTClient:
    """Enhanced MQTT client implementation
    
    Features:
    - Async-first API design
    - Automatic reconnection
    - Circuit breaker for fault tolerance
    - Message batching
    - Performance metrics
    - Retry mechanism
    - Message caching
    - QoS support
    - SSL/TLS support
    """

    def __init__(self, config: MQTTConfig):
        """Initialize the MQTT client with configuration
        
        Args:
            config: MQTT client configuration
        
        Raises:
            ConfigurationError: If configuration is invalid
        """
        # Validate configuration
        if not config.validate():
            raise ConfigurationError("Invalid MQTT configuration")
        
        self.config = config
        
        # Core components
        self._executor = ThreadPoolExecutor(max_workers=4)
        self._client = mqtt.Client(
            client_id=config.client_id,
            clean_session=config.clean_session
        )
        self._message_cache = cachetools.TTLCache(
            maxsize=config.message_cache_size,
            ttl=300
        )
        
        # Queues
        self._message_queue = asyncio.Queue()
        self._batch_queue = asyncio.Queue(maxsize=config.batch_size)
        self._retry_queue = asyncio.PriorityQueue()
        
        # State variables
        self._subscriptions: Dict[str, MQTTSubscription] = {}
        self._stopping = False
        self._connected = False
        self._reconnect_count = 0
        self._last_message_id = 0
        self._pending_messages: Dict[str, MQTTMessage] = {}
        self._message_callbacks: Dict[str, Callable] = {}
        
        # Locks and synchronization
        self._connection_lock = asyncio.Lock()
        self._publish_lock = asyncio.Lock()
        self._subscription_lock = asyncio.Lock()
        
        # Performance metrics
        self._metrics = PerformanceMetrics()
        
        # Circuit breaker
        self._circuit_breaker = CircuitBreaker(
            failure_threshold=config.circuit_breaker_threshold,
            reset_timeout=config.circuit_breaker_reset_timeout
        )
        
        # Background tasks
        self._tasks = []
        
        # Configure the client
        self._setup_callbacks()
        self._configure_auth_and_ssl()

    async def __aenter__(self):
        """Async context manager entry"""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.disconnect()
        
    def _setup_callbacks(self):
        """Set up MQTT client callbacks"""
        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_message = self._on_message
        self._client.on_publish = self._on_publish
        self._client.on_subscribe = self._on_subscribe
        self._client.on_unsubscribe = self._on_unsubscribe
        
    def _configure_auth_and_ssl(self):
        """Configure authentication and SSL/TLS"""
        if self.config.username and self.config.password:
            self._client.username_pw_set(
                self.config.username, self.config.password
            )
            
        if self.config.ssl_config:
            self._client.tls_set(**self.config.ssl_config)
    
    def _on_connect(self, client, userdata, flags, rc):
        """Handle connection event"""
        logger.info(f"MQTT connection result: {MQTT_CONNECTION_CODES.get(rc, f'Unknown ({rc})')}")
        if rc == 0:
            self._connected = True
            self._reconnect_count = 0
            # Resubscribe to topics on reconnect
            asyncio.create_task(self._resubscribe_to_topics())
        else:
            self._connected = False
            
    def _on_disconnect(self, client, userdata, rc):
        """Handle disconnection event"""
        self._connected = False
        if rc != 0:
            logger.warning(f"Unexpected MQTT disconnection: {rc}")
            asyncio.create_task(self._handle_reconnect())
        else:
            logger.info("MQTT client disconnected")
            
    def _on_message(self, client, userdata, msg):
        """Handle incoming message"""
        try:
            payload = parse_payload(msg.payload)
            message = MQTTMessage(
                topic=msg.topic,
                payload=payload,
                qos=MQTTQoS(msg.qos),
                retain=msg.retain
            )
            asyncio.create_task(self._process_incoming_message(message))
        except Exception as e:
            logger.error(f"Error processing incoming message: {e}")
            
    def _on_publish(self, client, userdata, mid):
        """Handle publish acknowledgment"""
        asyncio.create_task(self._handle_publish_ack(mid))
            
    def _on_subscribe(self, client, userdata, mid, granted_qos):
        """Handle subscription acknowledgment"""
        logger.debug(f"Subscription confirmed, QoS: {granted_qos}")
            
    def _on_unsubscribe(self, client, userdata, mid):
        """Handle unsubscribe acknowledgment"""
        logger.debug("Unsubscribe confirmed")
    
    async def _process_incoming_message(self, message: MQTTMessage):
        """Process an incoming message"""
        # Add to message queue
        await self._message_queue.put(message)
        
        # Call topic-specific callbacks
        for topic, subscription in self._subscriptions.items():
            if self._topic_matches(message.topic, topic) and subscription.callback:
                try:
                    if asyncio.iscoroutinefunction(subscription.callback):
                        await subscription.callback(message)
                    else:
                        await asyncio.get_event_loop().run_in_executor(
                            self._executor,
                            subscription.callback,
                            message
                        )
                except Exception as e:
                    logger.error(f"Error in message callback: {e}")
    
    async def _handle_publish_ack(self, mid):
        """Handle publish acknowledgment"""
        # Find and remove from pending messages
        for msg_id, message in list(self._pending_messages.items()):
            if str(mid) == str(msg_id):
                del self._pending_messages[msg_id]
                
                # Call message-specific callback if exists
                if msg_id in self._message_callbacks:
                    callback = self._message_callbacks.pop(msg_id)
                    try:
                        if asyncio.iscoroutinefunction(callback):
                            await callback(True, message)
                        else:
                            await asyncio.get_event_loop().run_in_executor(
                                self._executor,
                                callback,
                                True,
                                message
                            )
                    except Exception as e:
                        logger.error(f"Error in publish callback: {e}")
                break
    
    async def _handle_reconnect(self):
        """Handle reconnection logic"""
        if self._stopping:
            return
            
        await asyncio.sleep(self.config.reconnect_delay)
        
        max_attempts = self.config.max_reconnect_attempts
        if max_attempts < 0 or self._reconnect_count < max_attempts:
            self._reconnect_count += 1
            logger.info(f"Attempting to reconnect (attempt {self._reconnect_count})")
            
            try:
                await self.connect()
            except Exception as e:
                logger.error(f"Reconnection failed: {e}")
    
    @staticmethod
    def _topic_matches(actual_topic: str, subscription_topic: str) -> bool:
        """Check if an actual topic matches a subscription topic pattern
        
        Args:
            actual_topic: The actual topic of the message
            subscription_topic: The subscription topic pattern
            
        Returns:
            bool: True if the topic matches the subscription
        """
        # Handle wildcards and multi-level wildcards
        if subscription_topic == '#':
            return True
            
        actual_parts = actual_topic.split('/')
        subscription_parts = subscription_topic.split('/')
        
        # Multi-level wildcard at the end
        if subscription_parts[-1] == '#':
            return actual_parts[:len(subscription_parts)-1] == subscription_parts[:-1]
        
        if len(actual_parts) != len(subscription_parts):
            return False
            
        for i, part in enumerate(subscription_parts):
            if part != '+' and part != actual_parts[i]:
                return False
                
        return True
    
    async def _resubscribe_to_topics(self):
        """Resubscribe to all topics after reconnection"""
        async with self._subscription_lock:
            for topic, subscription in self._subscriptions.items():
                try:
                    result = await asyncio.get_event_loop().run_in_executor(
                        self._executor,
                        self._client.subscribe,
                        topic,
                        subscription.qos.value
                    )
                    
                    if result[0] != mqtt.MQTT_ERR_SUCCESS:
                        logger.error(f"Failed to resubscribe to {topic}")
                except Exception as e:
                    logger.error(f"Error resubscribing to {topic}: {e}")
    
    async def _start_background_tasks(self):
        """Start background tasks"""
        if self._tasks:
            for task in self._tasks:
                if not task.done():
                    task.cancel()
                    
        self._tasks = [
            asyncio.create_task(self._heartbeat_check()),
            asyncio.create_task(self._process_retry_queue()),
            asyncio.create_task(self._process_batch_queue())
        ]
    
    async def _heartbeat_check(self):
        """Check connection health periodically"""
        while not self._stopping:
            try:
                if self._connected:
                    await asyncio.get_event_loop().run_in_executor(
                        self._executor,
                        self._client.ping
                    )
                await asyncio.sleep(self.config.heartbeat_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Heartbeat error: {e}")
    
    async def _process_retry_queue(self):
        """Process message retry queue"""
        while not self._stopping:
            try:
                priority, message = await self._retry_queue.get()
                
                if message.retry_count < self.config.retry_config.max_retries:
                    message.retry_count += 1
                    backoff = self.config.retry_config.retry_interval * (
                        self.config.retry_config.retry_backoff ** message.retry_count
                    )
                    await asyncio.sleep(backoff)
                    await self._publish_single(message)
                else:
                    logger.error(f"Message {message.message_id} failed after max retries")
                    # Call callback with failure
                    if message.message_id in self._message_callbacks:
                        callback = self._message_callbacks.pop(message.message_id)
                        try:
                            if asyncio.iscoroutinefunction(callback):
                                await callback(False, message)
                            else:
                                await asyncio.get_event_loop().run_in_executor(
                                    self._executor,
                                    callback,
                                    False,
                                    message
                                )
                        except Exception as e:
                            logger.error(f"Error in retry callback: {e}")
                            
                self._retry_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Retry processor error: {e}")
                await asyncio.sleep(1)
    
    async def _process_batch_queue(self):
        """Process message batch queue"""
        while not self._stopping:
            try:
                batch = []
                while len(batch) < self.config.batch_size:
                    try:
                        message = await asyncio.wait_for(
                            self._batch_queue.get(),
                            timeout=0.1
                        )
                        batch.append(message)
                    except asyncio.TimeoutError:
                        break
                
                if batch:
                    await self._publish_batch(batch)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Batch processor error: {e}")
                await asyncio.sleep(1)
    
    async def _publish_batch(self, messages: List[MQTTMessage]) -> bool:
        """Publish a batch of messages
        
        Args:
            messages: List of MQTT messages to publish
            
        Returns:
            bool: True if all messages were published successfully
        """
        tasks = []
        for message in messages:
            tasks.append(self._publish_single(message))
            
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return all(isinstance(r, bool) and r for r in results)
    
    @async_measure_time
    async def _publish_single(self, message: MQTTMessage) -> bool:
        """Publish a single message
        
        Args:
            message: MQTT message to publish
            
        Returns:
            bool: True if message was published successfully
        """
        if not self._connected:
            await self._retry_queue.put((time.time(), message))
            return False
            
        try:
            # Use circuit breaker
            return await self._circuit_breaker.call(self._do_publish, message)
        except CircuitBreakerOpenError:
            await self._retry_queue.put((time.time(), message))
            return False
        except Exception as e:
            logger.error(f"Error publishing message: {e}")
            await self._retry_queue.put((time.time(), message))
            return False
    
    async def _do_publish(self, message: MQTTMessage) -> bool:
        """Actual message publishing implementation
        
        Args:
            message: MQTT message to publish
            
        Returns:
            bool: True if message was published successfully
        """
        message_size = len(str(message.payload).encode('utf-8'))
        start_time = time.time()
        
        try:
            result = await asyncio.get_event_loop().run_in_executor(
                self._executor,
                self._client.publish,
                message.topic,
                message.payload,
                message.qos.value,
                message.retain
            )
            
            success = result[0] == mqtt.MQTT_ERR_SUCCESS
            process_time = time.time() - start_time
            
            await self._metrics.update_async(success, message_size, process_time)
            
            if success:
                self._message_cache[message.message_id] = message
                self._pending_messages[str(result[1])] = message
            
            return success
        except Exception as e:
            process_time = time.time() - start_time
            await self._metrics.update_async(False, message_size, process_time)
            raise e
    
    def _connect_sync(self):
        """Synchronous connection method for the thread executor
        
        Raises:
            ConnectionError: If connection fails
        """
        try:
            result = self._client.connect(
                self.config.broker,
                self.config.port,
                keepalive=self.config.keepalive
            )
            
            if result != mqtt.MQTT_ERR_SUCCESS:
                raise ConnectionError(f"Connection failed with code {result}")
                
            self._client.loop_start()
        except Exception as e:
            raise ConnectionError(f"Connection failed: {str(e)}")
    
    # Public API methods
    
    @async_measure_time
    async def connect(self) -> bool:
        """Connect to the MQTT broker
        
        Returns:
            bool: True if connection was successful
            
        Raises:
            ConnectionError: If connection fails
        """
        async with self._connection_lock:
            if self._connected:
                return True
                
            try:
                await asyncio.get_event_loop().run_in_executor(
                    self._executor,
                    self._connect_sync
                )
                
                # Wait for connection to be established
                for _ in range(10):
                    if self._connected:
                        break
                    await asyncio.sleep(0.5)
                    
                if not self._connected:
                    raise ConnectionError("Connection timed out")
                    
                await self._start_background_tasks()
                return True
            except Exception as e:
                logger.error(f"Connection failed: {e}")
                raise ConnectionError(str(e))
    
    @async_measure_time
    async def disconnect(self) -> None:
        """Disconnect from the MQTT broker"""
        self._stopping = True
        
        # Cancel background tasks
        for task in self._tasks:
            if not task.done():
                task.cancel()
        
        # Wait for tasks to complete
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
            
        if self._connected:
            await asyncio.get_event_loop().run_in_executor(
                self._executor,
                self._client.disconnect
            )
            
        # Stop the network loop
        await asyncio.get_event_loop().run_in_executor(
            self._executor,
            self._client.loop_stop
        )
        
        self._connected = False
        self._stopping = False
    
    @async_measure_time
    async def publish(self, message: MQTTMessage) -> bool:
        """Publish an MQTT message
        
        Args:
            message: Message to publish
            
        Returns:
            bool: True if message was published successfully
            
        Raises:
            ConnectionError: If client is not connected
            PublishError: If publish operation fails
        """
        if not self._connected:
            raise ConnectionError("Client not connected")
            
        if not validate_topic(message.topic):
            raise PublishError(f"Invalid topic: {message.topic}")
            
        try:
            return await self._publish_single(message)
        except Exception as e:
            logger.error(f"Publish error: {e}")
            raise PublishError(str(e))
    
    async def publish_with_callback(self, message: MQTTMessage, callback: Callable) -> str:
        """Publish a message with a callback for completion
        
        Args:
            message: Message to publish
            callback: Function to call when publish completes (success, message)
            
        Returns:
            str: Message ID
            
        Raises:
            ConnectionError: If client is not connected
            PublishError: If publish operation fails
        """
        message.message_id = str(uuid.uuid4())
        self._message_callbacks[message.message_id] = callback
        
        success = await self.publish(message)
        if not success:
            del self._message_callbacks[message.message_id]
            raise PublishError(f"Failed to publish message {message.message_id}")
            
        return message.message_id
    
    async def publish_batch(self, messages: List[MQTTMessage]) -> bool:
        """Publish multiple messages as a batch
        
        Args:
            messages: List of messages to publish
            
        Returns:
            bool: True if all messages were published successfully
        """
        if not self._connected:
            raise ConnectionError("Client not connected")
            
        return await self._publish_batch(messages)
    
    async def publish_async(self, message: MQTTMessage) -> bool:
        """Queue a message for async publishing
        
        Args:
            message: Message to publish
            
        Returns:
            bool: True if message was queued successfully
        """
        if not validate_topic(message.topic):
            raise PublishError(f"Invalid topic: {message.topic}")
            
        try:
            await self._batch_queue.put(message)
            return True
        except Exception as e:
            logger.error(f"Error queuing message: {e}")
            return False
    
    @async_measure_time
    async def subscribe(self, topic: str, qos: MQTTQoS = MQTTQoS.AT_MOST_ONCE, 
                       callback: Callable = None) -> bool:
        """Subscribe to an MQTT topic
        
        Args:
            topic: Topic to subscribe to
            qos: Quality of Service level
            callback: Optional callback for messages on this topic
            
        Returns:
            bool: True if subscription was successful
            
        Raises:
            ConnectionError: If client is not connected
            SubscriptionError: If subscription fails
        """
        if not self._connected:
            raise ConnectionError("Client not connected")
            
        if not validate_topic(topic):
            raise SubscriptionError(f"Invalid topic: {topic}")
            
        async with self._subscription_lock:
            try:
                result = await asyncio.get_event_loop().run_in_executor(
                    self._executor,
                    self._client.subscribe,
                    topic,
                    qos.value
                )
                
                if result[0] != mqtt.MQTT_ERR_SUCCESS:
                    raise SubscriptionError(f"Subscribe failed with result code {result[0]}")
                    
                self._subscriptions[topic] = MQTTSubscription(
                    topic=topic,
                    qos=qos,
                    callback=callback
                )
                
                return True
            except Exception as e:
                logger.error(f"Subscribe error: {e}")
                raise SubscriptionError(str(e))
    
    @async_measure_time
    async def unsubscribe(self, topic: str) -> bool:
        """Unsubscribe from an MQTT topic
        
        Args:
            topic: Topic to unsubscribe from
            
        Returns:
            bool: True if unsubscribe was successful
            
        Raises:
            ConnectionError: If client is not connected
            SubscriptionError: If unsubscribe fails
        """
        if not self._connected:
            raise ConnectionError("Client not connected")
            
        async with self._subscription_lock:
            try:
                result = await asyncio.get_event_loop().run_in_executor(
                    self._executor,
                    self._client.unsubscribe,
                    topic
                )
                
                if result[0] != mqtt.MQTT_ERR_SUCCESS:
                    raise SubscriptionError(f"Unsubscribe failed with result code {result[0]}")
                    
                if topic in self._subscriptions:
                    del self._subscriptions[topic]
                    
                return True
            except Exception as e:
                logger.error(f"Unsubscribe error: {e}")
                raise SubscriptionError(str(e))
    
    async def receive(self, timeout: float = None) -> Optional[MQTTMessage]:
        """Receive a message from the queue
        
        Args:
            timeout: Maximum time to wait for a message (None = wait forever)
            
        Returns:
            Optional[MQTTMessage]: Received message or None if timeout
            
        Raises:
            TimeoutError: If timeout is reached
        """
        try:
            if timeout is not None:
                return await asyncio.wait_for(self._message_queue.get(), timeout)
            else:
                return await self._message_queue.get()
        except asyncio.TimeoutError:
            raise TimeoutError("Receive operation timed out")
        except Exception as e:
            logger.error(f"Receive error: {e}")
            return None
    
    def get_metrics(self) -> Dict:
        """Get performance metrics
        
        Returns:
            Dict: Performance metrics
        """
        current_time = time.time()
        elapsed_time = current_time - self._metrics.last_reset
        
        return {
            'publish_rate': self._metrics.publish_count / max(1, elapsed_time),
            'success_rate': self._metrics.success_rate,
            'average_message_size': self._metrics.average_message_size,
            'average_processing_time': self._metrics.message_processing_time / max(1, self._metrics.publish_count),
            'retry_rate': self._metrics.retry_count / max(1, self._metrics.publish_count),
            'connection_status': 'connected' if self._connected else 'disconnected',
            'reconnect_count': self._reconnect_count,
            'message_queue_size': self._message_queue.qsize(),
            'retry_queue_size': self._retry_queue.qsize(),
            'batch_queue_size': self._batch_queue.qsize(),
            'active_subscriptions': len(self._subscriptions)
        }
    
    def reset_metrics(self) -> None:
        """Reset performance metrics"""
        self._metrics.reset()
        
    @property
    def is_connected(self) -> bool:
        """Check if client is connected
        
        Returns:
            bool: True if connected
        """
        return self._connected
        
    @property
    def subscriptions(self) -> List[str]:
        """Get list of active subscriptions
        
        Returns:
            List[str]: List of subscription topics
        """
        return list(self._subscriptions.keys())
