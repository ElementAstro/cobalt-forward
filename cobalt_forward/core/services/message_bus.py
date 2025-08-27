"""
Message bus implementation for reliable message delivery.

This module provides a message bus implementation with support for
request-response patterns, message transformation, and delivery guarantees.
"""

import asyncio
import logging
import time
import uuid
from collections import defaultdict
from typing import Any, Callable, Dict, List, Optional

from ..interfaces.messaging import IMessageBus, IMessageTransformer, IMessageFilter
from ..interfaces.lifecycle import IComponent
from ..domain.messages import Message, MessageType

logger = logging.getLogger(__name__)


class MessageSubscription:
    """Represents a message subscription."""

    def __init__(self, subscription_id: str, topic_pattern: str, handler: Callable[[Message], Any]) -> None:
        self.subscription_id = subscription_id
        self.topic_pattern = topic_pattern
        self.handler = handler
        self.created_at = time.time()
        self.message_count = 0
        self.last_message: Optional[float] = None
        self.error_count = 0


class MessageBus(IComponent, IMessageBus):
    """
    Message bus implementation with transformation and filtering support.

    Provides reliable message delivery, request-response patterns,
    and pluggable message processing pipeline.
    """

    def __init__(self) -> None:
        self._subscriptions: Dict[str,
                                  List[MessageSubscription]] = defaultdict(list)
        self._wildcard_subscriptions: List[MessageSubscription] = []
        self._transformers: List[IMessageTransformer] = []
        self._filters: List[IMessageFilter] = []
        self._pending_requests: Dict[str, asyncio.Future[Any]] = {}
        self._running = False

        # Metrics
        self._metrics = {
            'messages_sent': 0,
            'messages_delivered': 0,
            'messages_failed': 0,
            'requests_sent': 0,
            'responses_received': 0,
            'subscriptions_count': 0
        }

    @property
    def name(self) -> str:
        """Get component name."""
        return "MessageBus"

    @property
    def version(self) -> str:
        """Get component version."""
        return "1.0.0"

    async def start(self) -> None:
        """Start the message bus."""
        if self._running:
            return

        logger.info("Starting message bus")
        self._running = True
        logger.info("Message bus started successfully")

    async def stop(self) -> None:
        """Stop the message bus."""
        if not self._running:
            return

        logger.info("Stopping message bus...")

        self._running = False

        # Cancel pending requests
        for future in self._pending_requests.values():
            if not future.done():
                future.cancel()

        self._pending_requests.clear()
        self._subscriptions.clear()
        self._wildcard_subscriptions.clear()

        logger.info("Message bus stopped")

    async def configure(self, config: Dict[str, Any]) -> None:
        """Configure the message bus."""
        # Configuration options can be added here
        pass

    async def check_health(self) -> Dict[str, Any]:
        """Check message bus health."""
        return {
            'healthy': True,
            'status': 'running' if self._running else 'stopped',
            'details': {
                'subscriptions_count': sum(len(subs) for subs in self._subscriptions.values()) + len(self._wildcard_subscriptions),
                'pending_requests': len(self._pending_requests),
                'transformers_count': len(self._transformers),
                'filters_count': len(self._filters),
                'messages_sent': self._metrics['messages_sent'],
                'messages_delivered': self._metrics['messages_delivered'],
                'messages_failed': self._metrics['messages_failed']
            }
        }

    async def send(self, message: Message) -> None:
        """Send a message through the bus."""
        if not self._running:
            raise RuntimeError("Message bus is not running")

        try:
            # Apply filters
            if not self._should_process_message(message):
                logger.debug(f"Message filtered out: {message.topic}")
                return

            # Apply transformations
            transformed_message = await self._transform_message(message)

            # Handle response messages
            if (transformed_message.message_type == MessageType.RESPONSE and
                    transformed_message.correlation_id in self._pending_requests):

                future = self._pending_requests.pop(
                    transformed_message.correlation_id)
                if not future.done():
                    future.set_result(transformed_message)
                    self._metrics['responses_received'] += 1
                return

            # Deliver to subscribers
            await self._deliver_message(transformed_message)

            self._metrics['messages_sent'] += 1

        except Exception as e:
            self._metrics['messages_failed'] += 1
            logger.error(f"Failed to send message: {e}")
            raise

    async def request(self, message: Message, timeout: float = 30.0) -> Message:
        """Send a request message and wait for response."""
        if not self._running:
            raise RuntimeError("Message bus is not running")

        # Set up request tracking
        correlation_id = message.correlation_id or str(uuid.uuid4())
        message = message.with_correlation_id(correlation_id)

        # Create future for response
        response_future: asyncio.Future[Any] = asyncio.Future()
        self._pending_requests[correlation_id] = response_future

        try:
            # Send request
            await self.send(message)
            self._metrics['requests_sent'] += 1

            # Wait for response
            response = await asyncio.wait_for(response_future, timeout=timeout)
            return response  # type: ignore[no-any-return]

        except asyncio.TimeoutError:
            # Clean up on timeout
            self._pending_requests.pop(correlation_id, None)
            raise TimeoutError(f"Request timeout after {timeout} seconds")

        except Exception as e:
            # Clean up on error
            self._pending_requests.pop(correlation_id, None)
            raise

    async def subscribe(self, topic: str, handler: Callable[[Message], Any]) -> str:
        """Subscribe to messages on a topic."""
        subscription_id = str(uuid.uuid4())
        subscription = MessageSubscription(
            subscription_id=subscription_id,
            topic_pattern=topic,
            handler=handler
        )

        # Add to appropriate subscription list
        if '*' in topic or '?' in topic:
            self._wildcard_subscriptions.append(subscription)
        else:
            self._subscriptions[topic].append(subscription)

        self._metrics['subscriptions_count'] += 1

        logger.debug(
            f"Added subscription for topic '{topic}' (ID: {subscription_id})")
        return subscription_id

    async def unsubscribe(self, subscription_id: str) -> bool:
        """Unsubscribe using subscription ID."""
        # Search in regular subscriptions
        for topic, subscriptions in self._subscriptions.items():
            for i, subscription in enumerate(subscriptions):
                if subscription.subscription_id == subscription_id:
                    subscriptions.pop(i)
                    self._metrics['subscriptions_count'] -= 1
                    logger.debug(
                        f"Removed subscription {subscription_id} for topic '{topic}'")
                    return True

        # Search in wildcard subscriptions
        for i, subscription in enumerate(self._wildcard_subscriptions):
            if subscription.subscription_id == subscription_id:
                self._wildcard_subscriptions.pop(i)
                self._metrics['subscriptions_count'] -= 1
                logger.debug(
                    f"Removed wildcard subscription {subscription_id}")
                return True

        return False

    async def get_metrics(self) -> Dict[str, Any]:
        """Get message bus metrics."""
        return self._metrics.copy()

    def add_transformer(self, transformer: IMessageTransformer) -> None:
        """Add a message transformer to the processing pipeline."""
        self._transformers.append(transformer)
        logger.debug(
            f"Added message transformer: {transformer.__class__.__name__}")

    def add_filter(self, filter: IMessageFilter) -> None:
        """Add a message filter to the processing pipeline."""
        self._filters.append(filter)
        logger.debug(f"Added message filter: {filter.__class__.__name__}")

    async def _deliver_message(self, message: Message) -> None:
        """Deliver message to all matching subscribers."""
        matching_subscriptions = []

        # Exact match subscriptions
        if message.topic in self._subscriptions:
            matching_subscriptions.extend(self._subscriptions[message.topic])

        # Wildcard subscriptions
        for subscription in self._wildcard_subscriptions:
            if self._matches_pattern(message.topic, subscription.topic_pattern):
                matching_subscriptions.append(subscription)

        # Deliver to all matching subscribers
        for subscription in matching_subscriptions:
            try:
                if asyncio.iscoroutinefunction(subscription.handler):
                    await subscription.handler(message)
                else:
                    subscription.handler(message)

                subscription.message_count += 1
                subscription.last_message = time.time()
                self._metrics['messages_delivered'] += 1

            except Exception as e:
                subscription.error_count += 1
                logger.error(f"Handler error for message {message.topic}: {e}")
                self._metrics['messages_failed'] += 1

    def _should_process_message(self, message: Message) -> bool:
        """Check if message should be processed based on filters."""
        for filter in self._filters:
            if not filter.should_process(message):
                return False
        return True

    async def _transform_message(self, message: Message) -> Message:
        """Apply all transformers to the message."""
        transformed = message

        for transformer in self._transformers:
            if transformer.can_transform(transformed):
                transformed = await transformer.transform(transformed)

        return transformed

    def _matches_pattern(self, topic: str, pattern: str) -> bool:
        """Check if topic matches a pattern with wildcards."""
        import fnmatch
        return fnmatch.fnmatch(topic, pattern)
