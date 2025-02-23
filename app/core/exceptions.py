from app.utils.error_handler import BaseCustomException, ErrorLevel

class MessageBusException(BaseCustomException):
    """消息总线基础异常"""
    pass

class MessageDeliveryException(MessageBusException):
    """消息投递异常"""
    def __init__(self, message: str, retry_count: int = 0):
        super().__init__(message, "MSG_DELIVERY_ERROR", ErrorLevel.ERROR)
        self.retry_count = retry_count

class MessageProcessingException(MessageBusException):
    """消息处理异常"""
    pass

class SubscriptionException(MessageBusException):
    """订阅相关异常"""
    pass

class ConnectionException(MessageBusException):
    """连接相关异常"""
    def __init__(self, message: str):
        super().__init__(message, "CONNECTION_ERROR", ErrorLevel.CRITICAL)
