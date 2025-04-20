class MQTTError(Exception):
    """Base exception class for MQTT errors"""
    def __init__(self, message: str = None, code: int = None):
        self.message = message or "MQTT operation failed"
        self.code = code
        super().__init__(self.message)


class ConnectionError(MQTTError):
    """Connection related errors"""
    def __init__(self, message: str = None, code: int = None):
        super().__init__(
            message or "Failed to establish MQTT connection",
            code
        )


class SubscriptionError(MQTTError):
    """Subscription related errors"""
    def __init__(self, message: str = None, code: int = None):
        super().__init__(
            message or "Failed to subscribe to MQTT topic",
            code
        )


class PublishError(MQTTError):
    """Publishing related errors"""
    def __init__(self, message: str = None, code: int = None):
        super().__init__(
            message or "Failed to publish MQTT message",
            code
        )


class ConfigurationError(MQTTError):
    """Configuration related errors"""
    def __init__(self, message: str = None, code: int = None):
        super().__init__(
            message or "Invalid MQTT configuration",
            code
        )


class MessageValidationError(MQTTError):
    """Message validation errors"""
    def __init__(self, message: str = None, code: int = None):
        super().__init__(
            message or "Invalid MQTT message format",
            code
        )


class CircuitBreakerOpenError(ConnectionError):
    """Circuit breaker is open"""
    def __init__(self, message: str = None, reset_time: float = None):
        super().__init__(
            message or "Circuit breaker is open, requests are rejected",
            None
        )
        self.reset_time = reset_time


class TimeoutError(MQTTError):
    """Operation timeout errors"""
    def __init__(self, message: str = None, operation: str = None):
        super().__init__(
            message or f"MQTT operation timed out: {operation or 'unknown'}",
            None
        )
        self.operation = operation
