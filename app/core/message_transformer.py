from abc import ABC, abstractmethod
from datetime import datetime
import zlib
import json
from typing import Any, Dict, List, Callable, Optional, Union
from app.core.message_bus import Message
from collections import defaultdict
import time
import logging
import asyncio
from functools import wraps
import traceback

logger = logging.getLogger(__name__)

class MessageTransform(ABC):
    """消息转换器基类，定义了转换接口"""
    
    @abstractmethod
    async def transform(self, message: Message) -> Message:
        """转换消息的抽象方法"""
        pass
    
    @property
    def name(self) -> str:
        """获取转换器名称"""
        return self.__class__.__name__
    
    def __str__(self) -> str:
        return self.name


def transform_error_handler(func):
    """转换器错误处理装饰器"""
    @wraps(func)
    async def wrapper(self, message: Message) -> Message:
        try:
            return await func(self, message)
        except Exception as e:
            logger.error(f"转换器 {self.name} 执行失败: {e}")
            logger.debug(traceback.format_exc())
            # 记录错误信息到消息元数据
            if message.metadata is None:
                message.metadata = {}
            if 'transform_errors' not in message.metadata:
                message.metadata['transform_errors'] = []
            message.metadata['transform_errors'].append({
                'transformer': self.name,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            })
            return message
    return wrapper


class CompressionTransform(MessageTransform):
    """压缩转换器，用于压缩消息数据"""
    
    def __init__(self, threshold: int = 1024, compression_level: int = 6):
        """
        初始化压缩转换器
        
        Args:
            threshold: 触发压缩的最小数据大小(字节)
            compression_level: 压缩级别(1-9)，9为最高压缩率
        """
        self.threshold = threshold
        self.compression_level = compression_level
    
    @transform_error_handler
    async def transform(self, message: Message) -> Message:
        """压缩消息数据"""
        if isinstance(message.data, (str, bytes)):
            data = message.data if isinstance(message.data, bytes) else message.data.encode()
            # 仅压缩超过阈值的数据
            if len(data) > self.threshold:
                message.data = zlib.compress(data, self.compression_level)
                if message.metadata is None:
                    message.metadata = {}
                message.metadata['compressed'] = True
                message.metadata['original_size'] = len(data)
                message.metadata['compressed_size'] = len(message.data)
        return message


class DecompressionTransform(MessageTransform):
    """解压转换器，用于解压缩消息数据"""
    
    @transform_error_handler
    async def transform(self, message: Message) -> Message:
        """解压消息数据"""
        if isinstance(message.data, bytes) and message.metadata and message.metadata.get('compressed'):
            message.data = zlib.decompress(message.data)
            message.metadata['compressed'] = False
        return message


class EncryptionTransform(MessageTransform):
    """加密转换器"""
    
    def __init__(self, key: bytes):
        self.key = key
    
    @transform_error_handler
    async def transform(self, message: Message) -> Message:
        # 这里可以实现实际的加密逻辑
        if message.metadata is None:
            message.metadata = {}
        message.metadata['encrypted'] = True
        return message


class MessageValidator(MessageTransform):
    """消息验证转换器"""
    
    @transform_error_handler
    async def transform(self, message: Message) -> Message:
        if not message.topic:
            raise ValueError("消息缺少必要的主题")
        if message.data is None:
            raise ValueError("消息缺少必要的数据")
        return message


class MessageEnricher(MessageTransform):
    """消息富集转换器，用于添加元数据"""
    
    def __init__(self, enrichments: Dict[str, Any] = None):
        """
        初始化消息富集器
        
        Args:
            enrichments: 要添加的固定元数据
        """
        self.enrichments = enrichments or {}
    
    @transform_error_handler
    async def transform(self, message: Message) -> Message:
        if message.metadata is None:
            message.metadata = {}
        
        # 添加基础元数据
        message.metadata.update({
            'timestamp': datetime.now().isoformat(),
            'version': '1.0'
        })
        
        # 添加自定义元数据
        message.metadata.update(self.enrichments)
        return message


class JsonTransform(MessageTransform):
    """JSON转换器，用于序列化/反序列化JSON数据"""
    
    def __init__(self, mode: str = 'serialize'):
        """
        初始化JSON转换器
        
        Args:
            mode: 'serialize' 或 'deserialize'
        """
        if mode not in ['serialize', 'deserialize']:
            raise ValueError("模式必须是 'serialize' 或 'deserialize'")
        self.mode = mode
    
    @transform_error_handler
    async def transform(self, message: Message) -> Message:
        if self.mode == 'serialize' and isinstance(message.data, (dict, list)):
            message.data = json.dumps(message.data)
            if message.metadata is None:
                message.metadata = {}
            message.metadata['content_type'] = 'application/json'
        elif self.mode == 'deserialize' and isinstance(message.data, str):
            try:
                message.data = json.loads(message.data)
                if message.metadata is None:
                    message.metadata = {}
                message.metadata['content_type'] = 'application/python'
            except json.JSONDecodeError:
                # 不是有效的JSON，保持原样
                pass
        return message


class MessageTransformer:
    """消息转换器，用于管理和应用多个转换器"""
    
    def __init__(self):
        """初始化消息转换器"""
        self._message_bus = None
        self._transformers: List[MessageTransform] = []
        self._metrics = defaultdict(int)
        self._processing_times = defaultdict(list)
        self._max_metrics_history = 100
    
    async def set_message_bus(self, message_bus):
        """设置消息总线"""
        self._message_bus = message_bus
    
    def add_transformer(self, transformer: MessageTransform) -> 'MessageTransformer':
        """添加转换器到转换链"""
        self._transformers.append(transformer)
        return self
    
    def clear_transformers(self) -> 'MessageTransformer':
        """清除所有转换器"""
        self._transformers.clear()
        return self
    
    async def transform(self, message: Any) -> Any:
        """应用所有转换器处理消息"""
        start_time = time.time()
        
        # 应用所有转换器
        result = message
        for transformer in self._transformers:
            transform_start = time.time()
            try:
                result = await transformer.transform(result)
                self._metrics[transformer.name] += 1
                
                # 记录转换器处理时间
                elapsed = time.time() - transform_start
                self._processing_times[transformer.name].append(elapsed)
                if len(self._processing_times[transformer.name]) > self._max_metrics_history:
                    self._processing_times[transformer.name].pop(0)
                
            except Exception as e:
                logger.error(f"转换器 {transformer.name} 执行失败: {e}")
                logger.debug(traceback.format_exc())
                self._metrics[f"{transformer.name}_errors"] += 1
        
        # 记录总体性能指标
        processing_time = time.time() - start_time
        self._metrics['total_transforms'] += 1
        self._metrics['avg_transform_time'] = (
            (self._metrics.get('avg_transform_time', 0) * (self._metrics['total_transforms'] - 1) + 
             processing_time) / self._metrics['total_transforms']
        )
        
        return result
    
    @property
    def metrics(self) -> Dict[str, Any]:
        """获取转换器指标"""
        metrics = dict(self._metrics)
        
        # 添加每个转换器的平均处理时间
        for name, times in self._processing_times.items():
            if times:
                metrics[f"{name}_avg_time"] = sum(times) / len(times)
        
        return metrics
    
    @property
    def transformers(self) -> List[MessageTransform]:
        """获取所有转换器"""
        return self._transformers
    
    def create_filter_chain(self, topic_pattern: str = None, 
                           condition: Callable[[Message], bool] = None) -> 'TransformFilterChain':
        """创建一个过滤转换链"""
        return TransformFilterChain(self, topic_pattern, condition)


class TransformFilterChain:
    """转换过滤链，用于条件性地应用转换器"""
    
    def __init__(self, transformer: MessageTransformer, 
                topic_pattern: str = None,
                condition: Callable[[Message], bool] = None):
        self.transformer = transformer
        self.topic_pattern = topic_pattern
        self.condition = condition
        self._chain_transformers: List[MessageTransform] = []
    
    def add_transformer(self, transformer: MessageTransform) -> 'TransformFilterChain':
        """添加转换器到当前过滤链"""
        self._chain_transformers.append(transformer)
        return self
    
    def should_apply(self, message: Message) -> bool:
        """检查是否应该应用转换链到此消息"""
        if self.topic_pattern and not message.topic.startswith(self.topic_pattern):
            return False
        
        if self.condition is not None:
            return self.condition(message)
            
        return True
    
    async def transform(self, message: Message) -> Message:
        """应用转换链到消息"""
        if not self.should_apply(message):
            return message
            
        result = message
        for transformer in self._chain_transformers:
            try:
                result = await transformer.transform(result)
            except Exception as e:
                logger.error(f"过滤链转换器 {transformer.__class__.__name__} 执行失败: {e}")
        
        return result
