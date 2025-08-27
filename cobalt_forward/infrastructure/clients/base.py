"""
Base client implementation for the Cobalt Forward application.

This module provides the base client class that all protocol-specific
clients inherit from.
"""

import asyncio
import logging
import time
from abc import ABC
from typing import Any, Dict, List, Optional, Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

from ...core.interfaces.clients import IBaseClient, ClientStatus
from ...core.interfaces.lifecycle import IComponent
from ...core.interfaces.messaging import IEventBus

logger = logging.getLogger(__name__)


@dataclass
class ClientMetrics:
    """Client performance metrics."""
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    total_response_time: float = 0.0
    min_response_time: float = float('inf')
    max_response_time: float = 0.0
    last_request_time: Optional[float] = None
    connection_count: int = 0
    disconnection_count: int = 0
    error_count: int = 0
    last_error: Optional[str] = None

    @property
    def average_response_time(self) -> float:
        """Calculate average response time."""
        if self.total_requests == 0:
            return 0.0
        return self.total_response_time / self.total_requests

    @property
    def success_rate(self) -> float:
        """Calculate success rate percentage."""
        if self.total_requests == 0:
            return 100.0
        return (self.successful_requests / self.total_requests) * 100.0

    def record_request(self, success: bool, response_time: float) -> None:
        """Record a request result."""
        self.total_requests += 1
        self.total_response_time += response_time
        self.last_request_time = time.time()

        if success:
            self.successful_requests += 1
        else:
            self.failed_requests += 1

        # Update min/max response times
        if response_time < self.min_response_time:
            self.min_response_time = response_time
        if response_time > self.max_response_time:
            self.max_response_time = response_time

    def record_error(self, error: str) -> None:
        """Record an error."""
        self.error_count += 1
        self.last_error = error


@dataclass
class ClientConfig:
    """Base client configuration."""
    host: str
    port: int
    timeout: float = 30.0
    retry_attempts: int = 3
    retry_delay: float = 1.0
    max_connections: int = 10
    keepalive: bool = True
    keepalive_interval: float = 60.0
    metadata: Dict[str, Any] = field(default_factory=dict)


class BaseClient(IBaseClient, IComponent, ABC):
    """
    Base client implementation.

    Provides common functionality for all protocol-specific clients
    including connection management, metrics, and error handling.
    """

    def __init__(
        self,
        config: ClientConfig,
        event_bus: Optional[IEventBus] = None,
        name: Optional[str] = None
    ):
        """
        Initialize base client.

        Args:
            config: Client configuration
            event_bus: Event bus for publishing events
            name: Client name for identification
        """
        self._config = config
        self._event_bus = event_bus
        self._name = name or self.__class__.__name__

        self._status = ClientStatus.DISCONNECTED
        self._metrics = ClientMetrics()
        self._callbacks: Dict[str, List[Callable[..., Any]]] = {}
        self._executor = ThreadPoolExecutor(max_workers=4)
        self._lock = asyncio.Lock()
        self._running = False

        # Connection management
        self._connection: Optional[Any] = None
        self._keepalive_task: Optional[asyncio.Task[None]] = None
        self._last_activity = time.time()

    async def start(self) -> None:
        """Start the client service."""
        if self._running:
            return

        self._running = True
        logger.info(f"Client {self._name} started")

        if self._event_bus:
            await self._event_bus.publish("client.started", {
                "client_name": self._name,
                "client_type": self.__class__.__name__
            })

    async def stop(self) -> None:
        """Stop the client service."""
        if not self._running:
            return

        self._running = False

        # Disconnect if connected
        if self.is_connected():
            await self.disconnect()

        # Stop keepalive task
        if self._keepalive_task and not self._keepalive_task.done():
            self._keepalive_task.cancel()
            try:
                await self._keepalive_task
            except asyncio.CancelledError:
                pass

        # Shutdown executor
        self._executor.shutdown(wait=False)

        logger.info(f"Client {self._name} stopped")

        if self._event_bus:
            await self._event_bus.publish("client.stopped", {
                "client_name": self._name
            })

    async def health_check(self) -> Dict[str, Any]:
        """Perform health check."""
        return {
            "healthy": self._running and self.is_connected(),
            "status": self._status.value,
            "metrics": {
                "total_requests": self._metrics.total_requests,
                "success_rate": self._metrics.success_rate,
                "average_response_time": self._metrics.average_response_time,
                "connection_count": self._metrics.connection_count,
                "error_count": self._metrics.error_count
            },
            "details": {
                "running": self._running,
                "connected": self.is_connected(),
                "host": self._config.host,
                "port": self._config.port,
                "last_activity": self._last_activity,
                "last_error": self._metrics.last_error
            }
        }

    def is_connected(self) -> bool:
        """Check if client is connected."""
        return self._status == ClientStatus.CONNECTED and self._connection is not None

    def get_status(self) -> ClientStatus:
        """Get current client status."""
        return self._status

    def get_metrics(self) -> ClientMetrics:
        """Get client metrics."""
        return self._metrics

    def add_callback(self, event: str, callback: Callable[..., Any]) -> None:
        """Add event callback."""
        if event not in self._callbacks:
            self._callbacks[event] = []
        self._callbacks[event].append(callback)

    def remove_callback(self, event: str, callback: Callable[..., Any]) -> None:
        """Remove event callback."""
        if event in self._callbacks:
            try:
                self._callbacks[event].remove(callback)
            except ValueError:
                logger.warning(f"Callback not found for event {event}")

    async def execute_with_retry(
        self,
        operation: Callable[..., Any],
        *args: Any,
        **kwargs: Any
    ) -> Any:
        """Execute operation with retry logic."""
        last_exception: Optional[Exception] = None

        for attempt in range(self._config.retry_attempts):
            try:
                start_time = time.time()

                if asyncio.iscoroutinefunction(operation):
                    result = await operation(*args, **kwargs)
                else:
                    result = await asyncio.get_event_loop().run_in_executor(
                        self._executor, operation, *args, **kwargs
                    )

                # Record successful request
                response_time = time.time() - start_time
                self._metrics.record_request(True, response_time)
                self._last_activity = time.time()

                return result

            except Exception as e:
                last_exception = e
                response_time = time.time() - start_time
                self._metrics.record_request(False, response_time)
                self._metrics.record_error(str(e))

                logger.warning(
                    f"Operation failed (attempt {attempt + 1}/{self._config.retry_attempts}): {e}")

                if attempt < self._config.retry_attempts - 1:
                    await asyncio.sleep(self._config.retry_delay * (attempt + 1))

        # All attempts failed
        if last_exception:
            raise last_exception
        else:
            raise RuntimeError("Operation failed after all retry attempts")

    async def _trigger_callback(self, event: str, *args: Any, **kwargs: Any) -> None:
        """Trigger event callbacks."""
        if event in self._callbacks:
            for callback in self._callbacks[event]:
                try:
                    if asyncio.iscoroutinefunction(callback):
                        await callback(*args, **kwargs)
                    else:
                        callback(*args, **kwargs)
                except Exception as e:
                    logger.error(f"Callback error for event {event}: {e}")

    async def _start_keepalive(self) -> None:
        """Start keepalive task."""
        if not self._config.keepalive or self._keepalive_task:
            return

        self._keepalive_task = asyncio.create_task(self._keepalive_worker())

    async def _stop_keepalive(self) -> None:
        """Stop keepalive task."""
        if self._keepalive_task and not self._keepalive_task.done():
            self._keepalive_task.cancel()
            try:
                await self._keepalive_task
            except asyncio.CancelledError:
                pass
            self._keepalive_task = None

    async def _keepalive_worker(self) -> None:
        """Keepalive worker task."""
        while self._running and self.is_connected():
            try:
                await asyncio.sleep(self._config.keepalive_interval)

                # Check if we need to send keepalive
                time_since_activity = time.time() - self._last_activity
                if time_since_activity >= self._config.keepalive_interval:
                    await self._send_keepalive()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Keepalive error: {e}")
                break

    async def _send_keepalive(self) -> None:
        """Send keepalive message (to be implemented by subclasses)."""
        self._last_activity = time.time()

    def _update_status(self, status: ClientStatus) -> None:
        """Update client status."""
        old_status = self._status
        self._status = status

        if old_status != status:
            logger.debug(
                f"Client {self._name} status changed: {old_status.value} -> {status.value}")

            # Update metrics
            if status == ClientStatus.CONNECTED:
                self._metrics.connection_count += 1
            elif status == ClientStatus.DISCONNECTED and old_status == ClientStatus.CONNECTED:
                self._metrics.disconnection_count += 1
