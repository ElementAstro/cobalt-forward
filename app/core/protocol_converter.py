from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
import struct


class ProtocolConverter(ABC):
    @abstractmethod
    async def to_wire_format(self, data: Any) -> bytes:
        pass

    @abstractmethod
    async def from_wire_format(self, data: bytes) -> Any:
        pass


class ModbusConverter(ProtocolConverter):
    async def to_wire_format(self, data: Dict[str, Any]) -> bytes:
        # Modbus协议转换实现
        function_code = data.get('function_code', 0x03)
        address = data.get('address', 0)
        value = data.get('value', 0)
        return struct.pack('>BBH', function_code, address, value)

    async def from_wire_format(self, data: bytes) -> Dict[str, Any]:
        function_code, address, value = struct.unpack('>BBH', data[:4])
        return {
            'function_code': function_code,
            'address': address,
            'value': value
        }


class MQTTConverter(ProtocolConverter):
    async def to_wire_format(self, data: Dict[str, Any]) -> bytes:
        # MQTT协议转换实现
        topic = data.get('topic', '').encode()
        payload = data.get('payload', '').encode()
        return struct.pack(f'>H{len(topic)}sH{len(payload)}s',
                           len(topic), topic, len(payload), payload)

    async def from_wire_format(self, data: bytes) -> Dict[str, Any]:
        topic_len = struct.unpack('>H', data[:2])[0]
        topic = data[2:2+topic_len].decode()
        payload_len = struct.unpack('>H', data[2+topic_len:4+topic_len])[0]
        payload = data[4+topic_len:4+topic_len+payload_len].decode()
        return {
            'topic': topic,
            'payload': payload
        }
