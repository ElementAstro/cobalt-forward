from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Callable, Optional, Union
from app.core.message_bus import Message
import logging
from collections import defaultdict
from functools import wraps
import zlib
import json
import asyncio
import traceback

logger = logging.getLogger(__name__)

class MessageTransform(ABC):
    """消息转换器基类，定义了转换接口"""
    
    @abstractmethod
    async def transform(self, data: Any) -> Any:
        """
        转换数据
        
        Args:
            data: 要转换的数据
            
        Returns:
            转换后的数据
        """
        pass
    
    @property
    def name(self) -> str:
        """获取转换器名称"""
        return self.__class__.__name__
    
    def __str__(self) -> str:
        return f"{self.name} Transformer"


def transform_error_handler(func):
    """转换器错误处理装饰器"""
    @wraps(func)
    async def wrapper(self, data: Any) -> Any:
        try:
            if asyncio.iscoroutinefunction(func):
                return await func(self, data)
            return func(self, data)
        except Exception as e:
            logger.error(f"转换失败 ({self.name}): {e}")
            logger.debug(traceback.format_exc())
            # 返回原始数据，确保不会阻断消息流
            return data
    return wrapper


class CompressionTransform(MessageTransform):
    """压缩转换器，用于压缩消息数据"""
    
    def __init__(self, threshold: int = 1024, compression_level: int = 6):
        """
        初始化压缩转换器
        
        Args:
            threshold: 压缩阈值，大于此值的数据才会被压缩
            compression_level: 压缩级别 (0-9)，级别越高压缩率越高但速度越慢
        """
        self.threshold = threshold
        self.compression_level = compression_level
        
    @transform_error_handler
    async def transform(self, data: Any) -> Any:
        """压缩数据"""
        if isinstance(data, str) and len(data) > self.threshold:
            # 字符串数据压缩
            compressed = zlib.compress(data.encode('utf-8'), self.compression_level)
            return {
                "_compressed": True,
                "data": compressed,
                "original_size": len(data),
                "compression_ratio": len(data) / len(compressed)
            }
        elif isinstance(data, dict) and not data.get("_compressed"):
            # 字典形式的JSON数据，转换为字符串再压缩
            json_str = json.dumps(data)
            if len(json_str) > self.threshold:
                compressed = zlib.compress(json_str.encode('utf-8'), self.compression_level)
                return {
                    "_compressed": True,
                    "data": compressed,
                    "original_size": len(json_str),
                    "compression_ratio": len(json_str) / len(compressed)
                }
        return data


class DecompressionTransform(MessageTransform):
    """解压转换器，用于解压缩消息数据"""
    
    @transform_error_handler
    async def transform(self, data: Any) -> Any:
        """解压数据"""
        if isinstance(data, dict) and data.get("_compressed"):
            try:
                # 解压缩数据
                decompressed = zlib.decompress(data["data"]).decode('utf-8')
                # 尝试解析JSON
                try:
                    return json.loads(decompressed)
                except json.JSONDecodeError:
                    # 不是JSON，返回原始解压后的字符串
                    return decompressed
            except Exception as e:
                logger.error(f"解压失败: {e}")
                # 解压失败，返回原始数据
                return data
        return data


class EncryptionTransform(MessageTransform):
    """加密转换器，用于加密消息数据"""
    
    def __init__(self, encryption_key: str, encrypt_fields: List[str] = None):
        """
        初始化加密转换器
        
        Args:
            encryption_key: 加密密钥
            encrypt_fields: 需要加密的字段列表，None表示加密整个数据
        """
        self.encryption_key = encryption_key
        self.encrypt_fields = encrypt_fields
    
    @transform_error_handler
    async def transform(self, data: Any) -> Any:
        """加密数据 - 实际实现中使用合适的加密算法"""
        # 这里只做简单示例，实际项目中应使用如Fernet等安全加密算法
        if self.encrypt_fields and isinstance(data, dict):
            # 只加密指定字段
            result = data.copy()
            for field in self.encrypt_fields:
                if field in result:
                    # 在这里替换为实际加密逻辑
                    result[field] = f"ENCRYPTED:{result[field]}"
            return result
        else:
            # 加密整个数据
            # 在这里替换为实际加密逻辑
            return f"ENCRYPTED:{data}"
        
        
class MessageValidator(MessageTransform):
    """消息验证器，用于验证消息格式"""
    
    def __init__(self, schema: Dict = None, validator_func: Callable = None):
        """
        初始化消息验证器
        
        Args:
            schema: JSON Schema用于验证
            validator_func: 自定义验证函数
        """
        self.schema = schema
        self.validator_func = validator_func
    
    @transform_error_handler
    async def transform(self, data: Any) -> Any:
        """验证数据"""
        if self.validator_func:
            # 使用自定义验证函数
            if not self.validator_func(data):
                logger.warning(f"消息验证失败: {data}")
                # 可以选择返回None或抛出异常
                return None
        
        if self.schema and isinstance(data, dict):
            # 使用jsonschema验证数据
            try:
                import jsonschema
                jsonschema.validate(instance=data, schema=self.schema)
            except jsonschema.exceptions.ValidationError as e:
                logger.warning(f"Schema验证失败: {e}")
                return None
                
        return data


class MessageEnricher(MessageTransform):
    """消息增强器，用于添加额外信息"""
    
    def __init__(self, enrich_func: Callable = None, static_fields: Dict = None):
        """
        初始化消息增强器
        
        Args:
            enrich_func: 自定义增强函数
            static_fields: 静态字段，将添加到所有消息
        """
        self.enrich_func = enrich_func
        self.static_fields = static_fields or {}
    
    @transform_error_handler
    async def transform(self, data: Any) -> Any:
        """增强数据"""
        if isinstance(data, dict):
            # 复制数据，避免修改原始对象
            result = data.copy()
            
            # 添加静态字段
            for key, value in self.static_fields.items():
                if key not in result:
                    result[key] = value
            
            # 应用自定义增强函数
            if self.enrich_func:
                enriched = self.enrich_func(result)
                if enriched is not None:
                    result = enriched
            
            # 添加时间戳（如果没有）
            if "timestamp" not in result:
                result["timestamp"] = datetime.now().isoformat()
                
            return result
        
        return data


class JsonTransform(MessageTransform):
    """JSON转换器，用于序列化/反序列化JSON"""
    
    def __init__(self, serialize: bool = True):
        """
        初始化JSON转换器
        
        Args:
            serialize: True表示序列化，False表示反序列化
        """
        self.serialize = serialize
    
    @transform_error_handler
    async def transform(self, data: Any) -> Any:
        """转换数据"""
        if self.serialize:
            # 序列化为JSON字符串
            if isinstance(data, (dict, list)):
                return json.dumps(data)
            return data
        else:
            # 反序列化JSON字符串
            if isinstance(data, str):
                try:
                    return json.loads(data)
                except json.JSONDecodeError:
                    logger.warning(f"JSON解析失败: {data}")
            return data


class MessageTransformer:
    """消息转换管理器，协调多个转换器的应用"""
    
    def __init__(self):
        self.transformers: List[MessageTransform] = []
        self.topic_transformers: Dict[str, List[MessageTransform]] = defaultdict(list)
        self._message_bus = None
        self._metrics = {
            'transform_count': 0,
            'error_count': 0,
            'average_time': 0,
            'transform_times': []
        }
        self._max_history = 100
    
    async def set_message_bus(self, message_bus):
        """设置消息总线引用"""
        self._message_bus = message_bus
        logger.info("消息总线已与转换器管理器集成")
    
    def add_transformer(self, transformer: MessageTransform, topic_pattern: str = None) -> None:
        """
        添加转换器
        
        Args:
            transformer: 转换器实例
            topic_pattern: 主题模式，None表示应用于所有消息
        """
        if topic_pattern:
            self.topic_transformers[topic_pattern].append(transformer)
        else:
            self.transformers.append(transformer)
        logger.info(f"添加转换器: {transformer.name}, 主题: {topic_pattern or '全局'}")
    
    async def transform(self, message: Message) -> Message:
        """
        应用所有匹配的转换器处理消息
        
        Args:
            message: 要处理的消息
            
        Returns:
            处理后的消息
        """
        start_time = asyncio.get_event_loop().time()
        
        try:
            # 应用全局转换器
            for transformer in self.transformers:
                try:
                    message.data = await transformer.transform(message.data)
                    if message.data is None:
                        # 转换失败，终止处理
                        logger.warning(f"转换器 {transformer.name} 返回None，终止处理")
                        return None
                except Exception as e:
                    logger.error(f"转换器 {transformer.name} 处理失败: {e}")
                    self._metrics['error_count'] += 1
            
            # 应用特定主题的转换器
            for pattern, transformers in self.topic_transformers.items():
                if self._match_topic(message.topic, pattern):
                    for transformer in transformers:
                        try:
                            message.data = await transformer.transform(message.data)
                            if message.data is None:
                                logger.warning(f"主题转换器 {transformer.name} 返回None，终止处理")
                                return None
                        except Exception as e:
                            logger.error(f"主题转换器 {transformer.name} 处理失败: {e}")
                            self._metrics['error_count'] += 1
            
            # 更新指标
            self._metrics['transform_count'] += 1
            transform_time = asyncio.get_event_loop().time() - start_time
            self._metrics['transform_times'].append(transform_time)
            
            if len(self._metrics['transform_times']) > self._max_history:
                self._metrics['transform_times'].pop(0)
            
            self._metrics['average_time'] = sum(self._metrics['transform_times']) / len(self._metrics['transform_times'])
            
            return message
            
        except Exception as e:
            logger.error(f"转换消息时出错: {e}")
            self._metrics['error_count'] += 1
            # 返回原始消息，确保不会阻断消息流
            return message
    
    def _match_topic(self, topic: str, pattern: str) -> bool:
        """
        检查主题是否匹配模式
        
        Args:
            topic: 消息主题
            pattern: 主题模式，支持通配符 *
            
        Returns:
            是否匹配
        """
        # 转换通配符模式为正则表达式
        if '*' in pattern:
            import re
            regex_pattern = pattern.replace('.', '\\.').replace('*', '.*')
            return bool(re.match(f"^{regex_pattern}$", topic))
        return topic == pattern
    
    def get_metrics(self) -> Dict[str, Any]:
        """获取转换器指标"""
        return self._metrics.copy()
    
    def get_transformers(self) -> List[str]:
        """获取所有转换器名称"""
        transformers = [t.name for t in self.transformers]
        for pattern, topic_transformers in self.topic_transformers.items():
            for t in topic_transformers:
                transformers.append(f"{t.name} ({pattern})")
        return transformers


class TransformFilterChain:
    """转换过滤链，用于链式处理消息"""
    
    def __init__(self):
        self.transforms: List[MessageTransform] = []
        
    def add_transform(self, transform: MessageTransform) -> 'TransformFilterChain':
        """添加转换器到链"""
        self.transforms.append(transform)
        return self
    
    async def process(self, data: Any) -> Any:
        """处理数据，按顺序应用所有转换器"""
        result = data
        for transform in self.transforms:
            result = await transform.transform(result)
            if result is None:
                # 有转换器返回None，中断链
                break
        return result
