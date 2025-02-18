from typing import Any, Callable, List, Optional
from dataclasses import dataclass
from app.core.message_bus import Message, MessageType
import json
import re


@dataclass
class MessageFilter:
    topic_pattern: str
    condition: Optional[Callable[[Message], bool]] = None

    def matches(self, message: Message) -> bool:
        if not re.match(self.topic_pattern, message.topic):
            return False
        return True if self.condition is None else self.condition(message)


class MessageTransformer:
    def __init__(self):
        self.filters: List[MessageFilter] = []
        self.transformers: List[Callable[[Message], Message]] = []

    def add_filter(self, filter_: MessageFilter):
        self.filters.append(filter_)

    def add_transformer(self, transformer: Callable[[Message], Message]):
        self.transformers.append(transformer)

    def process(self, message: Message) -> Optional[Message]:
        # Check if message passes all filters
        if not all(f.matches(message) for f in self.filters):
            return None

        # Apply all transformers
        result = message
        for transformer in self.transformers:
            result = transformer(result)
        return result

# 预定义的转换器


def json_payload_transformer(message: Message) -> Message:
    """将消息payload转换为JSON格式"""
    if isinstance(message.data, (dict, list)):
        message.data = json.dumps(message.data)
    return message


def compress_payload_transformer(message: Message) -> Message:
    """压缩大型消息payload"""
    if isinstance(message.data, str) and len(message.data) > 1000:
        message.metadata['compressed'] = True
        # 这里可以添加实际的压缩逻辑
    return message
