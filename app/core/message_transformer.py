from abc import ABC, abstractmethod
from datetime import datetime
import zlib
import json
from typing import Any, Dict
from app.core.message_bus import Message
from collections import defaultdict
import time
import logging

logger = logging.getLogger(__name__)

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


class MessageTransformer:
    def __init__(self):
        self._message_bus = None
        self._metrics = defaultdict(int)

    async def set_message_bus(self, message_bus):
        self._message_bus = message_bus

    async def transform(self, message: Any) -> Any:
        """增强的消息转换"""
        start_time = time.time()
        
        # 应用所有转换器
        result = message
        for transformer in self.transformers:
            try:
                result = await transformer(result)
                self._metrics[transformer.__name__] += 1
            except Exception as e:
                logger.error(f"转换器 {transformer.__name__} 执行失败: {e}")
                
        # 记录性能指标
        processing_time = time.time() - start_time
        self._metrics['total_transforms'] += 1
        self._metrics['avg_transform_time'] = (
            (self._metrics['avg_transform_time'] * (self._metrics['total_transforms'] - 1) + 
             processing_time) / self._metrics['total_transforms']
        )
        
        return result

    @property 
    def metrics(self):
        """获取转换器指标"""
        return dict(self._metrics)
