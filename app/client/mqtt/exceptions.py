class MQTTError(Exception):
    """MQTT基础异常类"""
    pass

class ConnectionError(MQTTError):
    """连接相关错误"""
    pass

class SubscriptionError(MQTTError):
    """订阅相关错误"""
    pass

class PublishError(MQTTError):
    """发布相关错误"""
    pass

class ConfigurationError(MQTTError):
    """配置相关错误"""
    pass
