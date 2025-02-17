from typing import Any, Optional
from enum import Enum

class ErrorCode(Enum):
    """错误码枚举"""
    UNKNOWN_ERROR = 10000
    CONFIG_ERROR = 10001
    PLUGIN_ERROR = 10002
    NETWORK_ERROR = 10003
    PROTOCOL_ERROR = 10004
    AUTHENTICATION_ERROR = 10005
    PERMISSION_DENIED = 10006
    RESOURCE_NOT_FOUND = 10007
    INVALID_OPERATION = 10008
    SERVICE_UNAVAILABLE = 10009

class BaseError(Exception):
    """基础异常类"""
    def __init__(
        self,
        code: ErrorCode,
        message: str,
        details: Optional[Any] = None
    ):
        self.code = code
        self.message = message
        self.details = details
        super().__init__(message)

class ConfigError(BaseError):
    """配置相关错误"""
    def __init__(self, message: str, details: Any = None):
        super().__init__(ErrorCode.CONFIG_ERROR, message, details)

class PluginError(BaseError):
    """插件相关错误"""
    def __init__(self, message: str, details: Any = None):
        super().__init__(ErrorCode.PLUGIN_ERROR, message, details)

class NetworkError(BaseError):
    """网络相关错误"""
    def __init__(self, message: str, details: Any = None):
        super().__init__(ErrorCode.NETWORK_ERROR, message, details)

class ProtocolError(BaseError):
    """协议相关错误"""
    def __init__(self, message: str, details: Any = None):
        super().__init__(ErrorCode.PROTOCOL_ERROR, message, details)

class AuthenticationError(BaseError):
    """认证相关错误"""
    def __init__(self, message: str, details: Any = None):
        super().__init__(ErrorCode.AUTHENTICATION_ERROR, message, details)
