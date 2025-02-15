from abc import ABC, abstractmethod
from datetime import datetime
import zlib
import json
from typing import Any, Dict
from message_bus import Message


class MessageTransform(ABC):
    @abstractmethod
    async def transform(self, message: Message) -> Message:
        pass


class CompressionTransform(MessageTransform):
    async def transform(self, message: Message) -> Message:
        if isinstance(message.data, (str, bytes)):
            message.data = zlib.compress(message.data if isinstance(
                message.data, bytes) else message.data.encode())
            message.metadata['compressed'] = True
        return message


class EncryptionTransform(MessageTransform):
    def __init__(self, key: bytes):
        self.key = key

    async def transform(self, message: Message) -> Message:
        # 实现加密逻辑
        return message


class MessageValidator(MessageTransform):
    async def transform(self, message: Message) -> Message:
        if not message.topic or not message.data:
            raise ValueError("Invalid message format")
        return message


class MessageEnricher(MessageTransform):
    async def transform(self, message: Message) -> Message:
        if not message.metadata:
            message.metadata = {}
        message.metadata.update({
            'timestamp': datetime.now().isoformat(),
            'version': '1.0'
        })
        return message
