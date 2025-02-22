from enum import Enum

class MQTTQoS(Enum):
    """MQTT QoS级别"""
    AT_MOST_ONCE = 0
    AT_LEAST_ONCE = 1
    EXACTLY_ONCE = 2

# MQTT 状态码
MQTT_CONNECTION_CODES = {
    0: "Connection successful",
    1: "Connection refused - incorrect protocol version",
    2: "Connection refused - invalid client identifier",
    3: "Connection refused - server unavailable",
    4: "Connection refused - bad username or password",
    5: "Connection refused - not authorized"
}

# 默认配置
DEFAULT_KEEPALIVE = 60
DEFAULT_RECONNECT_DELAY = 5
DEFAULT_QOS = MQTTQoS.AT_MOST_ONCE
DEFAULT_CLEAN_SESSION = True
