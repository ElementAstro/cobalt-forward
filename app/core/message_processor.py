from typing import Any, Callable, List, Optional, Dict, Set
from dataclasses import dataclass, field
from app.core.message_bus import Message, MessageType
import json
import re
import asyncio
import time
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

@dataclass
class MessageFilter:
    """消息过滤器，基于主题和条件函数"""
    topic_pattern: str
    condition: Optional[Callable[[Message], bool]] = None
    name: str = field(default_factory=lambda: f"filter_{id(object())}")

    def matches(self, message: Message) -> bool:
        """检查消息是否匹配过滤器"""
        if not re.match(self.topic_pattern, message.topic):
            return False
        return True if self.condition is None else self.condition(message)


class MessageProcessor:
    """
    消息处理器，用于根据过滤器应用变换并处理消息
    """
    def __init__(self):
        self.filters: List[MessageFilter] = []
        self.transformers: Dict[str, List[Callable[[Message], Message]]] = {}
        self.global_transformers: List[Callable[[Message], Message]] = []
        self.metrics: Dict[str, Any] = {
            'processed': 0,
            'filtered_out': 0,
            'errors': 0,
            'processing_times': [],
        }
        self._max_metrics_history = 100
        self._filter_metrics: Dict[str, int] = {}
        self._transformer_metrics: Dict[str, int] = {}
        self._transformer_times: Dict[str, List[float]] = {}
        self._async_mode = True
        self._processing_semaphore = asyncio.Semaphore(10)  # 限制并发处理数量

    def add_filter(self, filter_: MessageFilter) -> None:
        """添加过滤器"""
        self.filters.append(filter_)
        self._filter_metrics[filter_.name] = 0

    def add_transformer(self, transformer: Callable[[Message], Message], filter_name: Optional[str] = None) -> None:
        """
        添加消息转换器
        
        Args:
            transformer: 转换器函数
            filter_name: 关联的过滤器名称，None表示全局转换器
        """
        transformer_name = getattr(transformer, '__name__', f"transformer_{id(transformer)}")
        
        if filter_name is None:
            self.global_transformers.append(transformer)
        else:
            if filter_name not in self.transformers:
                self.transformers[filter_name] = []
            self.transformers[filter_name].append(transformer)
        
        self._transformer_metrics[transformer_name] = 0
        self._transformer_times[transformer_name] = []

    def set_async_mode(self, enabled: bool) -> None:
        """设置是否使用异步模式处理消息"""
        self._async_mode = enabled

    async def process(self, message: Message) -> Optional[Message]:
        """处理消息，应用过滤器和转换器"""
        start_time = time.time()
        
        # 首先应用全局转换器
        try:
            for transformer in self.global_transformers:
                t_start = time.time()
                transformer_name = getattr(transformer, '__name__', str(transformer))
                
                if asyncio.iscoroutinefunction(transformer):
                    message = await transformer(message)
                else:
                    message = transformer(message)
                
                self._transformer_metrics[transformer_name] = self._transformer_metrics.get(transformer_name, 0) + 1
                
                # 记录转换器处理时间
                elapsed = time.time() - t_start
                if transformer_name not in self._transformer_times:
                    self._transformer_times[transformer_name] = []
                self._transformer_times[transformer_name].append(elapsed)
                if len(self._transformer_times[transformer_name]) > self._max_metrics_history:
                    self._transformer_times[transformer_name].pop(0)
        except Exception as e:
            logger.error(f"全局转换器处理失败: {e}")
            self.metrics['errors'] += 1
            return None

        # 检查消息是否匹配任何过滤器
        matching_filters = []
        for filter_ in self.filters:
            if filter_.matches(message):
                matching_filters.append(filter_)
                self._filter_metrics[filter_.name] = self._filter_metrics.get(filter_.name, 0) + 1

        if not matching_filters:
            self.metrics['filtered_out'] += 1
            return None

        # 对于匹配的每个过滤器，应用相应的转换器
        result = message
        async with self._processing_semaphore:
            for filter_ in matching_filters:
                if filter_.name in self.transformers:
                    for transformer in self.transformers[filter_.name]:
                        try:
                            t_start = time.time()
                            transformer_name = getattr(transformer, '__name__', str(transformer))
                            
                            if asyncio.iscoroutinefunction(transformer) and self._async_mode:
                                result = await transformer(result)
                            else:
                                result = transformer(result)
                            
                            self._transformer_metrics[transformer_name] = self._transformer_metrics.get(transformer_name, 0) + 1
                            
                            # 记录转换器处理时间
                            elapsed = time.time() - t_start
                            if transformer_name not in self._transformer_times:
                                self._transformer_times[transformer_name] = []
                            self._transformer_times[transformer_name].append(elapsed)
                            if len(self._transformer_times[transformer_name]) > self._max_metrics_history:
                                self._transformer_times[transformer_name].pop(0)
                                
                        except Exception as e:
                            logger.error(f"转换器处理失败: {e}")
                            self.metrics['errors'] += 1

        # 更新指标
        self.metrics['processed'] += 1
        processing_time = time.time() - start_time
        self.metrics['processing_times'].append(processing_time)
        if len(self.metrics['processing_times']) > self._max_metrics_history:
            self.metrics['processing_times'].pop(0)

        return result

    def get_detailed_metrics(self) -> Dict[str, Any]:
        """获取详细的处理器指标"""
        metrics = self.metrics.copy()
        
        # 添加平均处理时间
        if self.metrics['processing_times']:
            metrics['avg_processing_time'] = sum(self.metrics['processing_times']) / len(self.metrics['processing_times'])
        else:
            metrics['avg_processing_time'] = 0
            
        # 添加过滤器指标
        metrics['filters'] = self._filter_metrics.copy()
        
        # 添加转换器指标
        metrics['transformers'] = {}
        for name, count in self._transformer_metrics.items():
            metrics['transformers'][name] = {
                'count': count
            }
            if name in self._transformer_times and self._transformer_times[name]:
                metrics['transformers'][name]['avg_time'] = sum(self._transformer_times[name]) / len(self._transformer_times[name])
        
        return metrics


# 预定义的转换器函数
async def json_payload_transformer(message: Message) -> Message:
    """将消息payload转换为JSON格式"""
    if isinstance(message.data, (dict, list)):
        message.data = json.dumps(message.data)
        if message.metadata is None:
            message.metadata = {}
        message.metadata['content_type'] = 'application/json'
    return message


async def compress_payload_transformer(message: Message) -> Message:
    """压缩大型消息payload"""
    if isinstance(message.data, str) and len(message.data) > 1000:
        import zlib
        encoded_data = message.data.encode('utf-8')
        message.data = zlib.compress(encoded_data)
        if message.metadata is None:
            message.metadata = {}
        message.metadata['compressed'] = True
        message.metadata['original_size'] = len(encoded_data)
        message.metadata['compression_ratio'] = len(encoded_data) / len(message.data)
    return message
