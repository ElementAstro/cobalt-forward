from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
import struct
import json
import logging
import asyncio
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


class ProtocolConverter(ABC):
    """
    协议转换器抽象基类，用于在不同协议格式之间转换数据
    
    这个类提供了在不同协议格式之间转换数据的通用接口，
    具体的转换逻辑由子类实现。
    """
    
    @abstractmethod
    async def to_wire_format(self, data: Dict[str, Any]) -> Any:
        """
        将内部数据格式转换为线路格式（用于发送）
        
        Args:
            data: 内部格式数据
            
        Returns:
            转换后的线路格式数据
        """
        pass
    
    @abstractmethod
    async def from_wire_format(self, data: Any) -> Dict[str, Any]:
        """
        将线路格式转换为内部数据格式（用于接收）
        
        Args:
            data: 线路格式数据
            
        Returns:
            转换后的内部格式数据
        """
        pass


class JsonProtocolConverter(ProtocolConverter):
    """JSON协议转换器，处理JSON格式数据"""
    
    async def to_wire_format(self, data: Dict[str, Any]) -> str:
        """
        将Python字典转换为JSON字符串
        
        Args:
            data: Python字典数据
            
        Returns:
            转换后的JSON字符串
        """
        return json.dumps(data)
    
    async def from_wire_format(self, data: str) -> Dict[str, Any]:
        """
        将JSON字符串转换为Python字典
        
        Args:
            data: JSON字符串
            
        Returns:
            转换后的Python字典
        """
        if isinstance(data, bytes):
            data = data.decode('utf-8')
        return json.loads(data)


class BinaryProtocolConverter(ProtocolConverter):
    """二进制协议转换器，处理二进制格式数据"""
    
    def __init__(self, encoding: str = 'utf-8'):
        """
        初始化二进制协议转换器
        
        Args:
            encoding: 字符编码
        """
        self.encoding = encoding
    
    async def to_wire_format(self, data: Dict[str, Any]) -> bytes:
        """
        将Python字典转换为二进制数据
        
        Args:
            data: Python字典数据
            
        Returns:
            转换后的二进制数据
        """
        # 先转换为JSON字符串，再编码为二进制
        json_str = json.dumps(data)
        return json_str.encode(self.encoding)
    
    async def from_wire_format(self, data: bytes) -> Dict[str, Any]:
        """
        将二进制数据转换为Python字典
        
        Args:
            data: 二进制数据
            
        Returns:
            转换后的Python字典
        """
        # 先解码为字符串，再解析为JSON
        if isinstance(data, str):
            return json.loads(data)
        return json.loads(data.decode(self.encoding))


@dataclass
class ProtocolSpecification:
    """协议规范，定义了协议的格式和参数"""
    name: str
    version: str
    format: str  # 'json', 'binary', 'xml', etc.
    encoding: Optional[str] = None
    schema: Optional[Dict[str, Any]] = None
    extensions: Dict[str, Any] = field(default_factory=dict)


class ProtocolAdapterFactory:
    """协议适配器工厂，创建和管理协议转换器"""
    
    def __init__(self):
        """初始化协议适配器工厂"""
        self._converters: Dict[str, ProtocolConverter] = {}
        self._specs: Dict[str, ProtocolSpecification] = {}
    
    def register_protocol(self, spec: ProtocolSpecification) -> None:
        """
        注册协议规范
        
        Args:
            spec: 协议规范
        """
        self._specs[spec.name] = spec
        
        # 创建并注册对应的转换器
        if spec.format.lower() == 'json':
            self._converters[spec.name] = JsonProtocolConverter()
        elif spec.format.lower() == 'binary':
            self._converters[spec.name] = BinaryProtocolConverter(encoding=spec.encoding or 'utf-8')
        
        logger.info(f"注册协议: {spec.name}, 版本: {spec.version}, 格式: {spec.format}")
    
    def register_converter(self, protocol_name: str, converter: ProtocolConverter) -> None:
        """
        注册自定义协议转换器
        
        Args:
            protocol_name: 协议名称
            converter: 协议转换器
        """
        self._converters[protocol_name] = converter
        logger.info(f"注册自定义协议转换器: {protocol_name}")
    
    def get_converter(self, protocol_name: str) -> Optional[ProtocolConverter]:
        """
        获取协议转换器
        
        Args:
            protocol_name: 协议名称
            
        Returns:
            协议转换器，如不存在返回None
        """
        return self._converters.get(protocol_name)
    
    def get_specification(self, protocol_name: str) -> Optional[ProtocolSpecification]:
        """
        获取协议规范
        
        Args:
            protocol_name: 协议名称
            
        Returns:
            协议规范，如不存在返回None
        """
        return self._specs.get(protocol_name)
    
    def list_protocols(self) -> Dict[str, str]:
        """
        列出所有注册的协议
        
        Returns:
            协议名称到格式的映射
        """
        return {name: spec.format for name, spec in self._specs.items()}


class MultiProtocolConverter:
    """多协议转换器，支持在多种协议格式之间转换"""
    
    def __init__(self, factory: ProtocolAdapterFactory):
        """
        初始化多协议转换器
        
        Args:
            factory: 协议适配器工厂
        """
        self._factory = factory
        self._default_protocol = None
    
    def set_default_protocol(self, protocol_name: str) -> None:
        """
        设置默认协议
        
        Args:
            protocol_name: 协议名称
        """
        if protocol_name not in self._factory.list_protocols():
            raise ValueError(f"未知协议: {protocol_name}")
        
        self._default_protocol = protocol_name
        logger.info(f"设置默认协议: {protocol_name}")
    
    async def convert(self, data: Any, from_protocol: str, to_protocol: str) -> Any:
        """
        在两种协议格式之间转换数据
        
        Args:
            data: 源数据
            from_protocol: 源协议名称
            to_protocol: 目标协议名称
            
        Returns:
            转换后的数据
        """
        # 获取源协议和目标协议的转换器
        from_converter = self._factory.get_converter(from_protocol)
        to_converter = self._factory.get_converter(to_protocol)
        
        if not from_converter:
            raise ValueError(f"未知源协议: {from_protocol}")
        if not to_converter:
            raise ValueError(f"未知目标协议: {to_protocol}")
        
        # 先转换为内部格式，再转换为目标格式
        internal_data = await from_converter.from_wire_format(data)
        result = await to_converter.to_wire_format(internal_data)
        
        return result
    
    async def to_protocol(self, data: Dict[str, Any], protocol: str) -> Any:
        """
        将内部数据转换为指定协议格式
        
        Args:
            data: 内部格式数据
            protocol: 目标协议名称
            
        Returns:
            转换后的数据
        """
        converter = self._factory.get_converter(protocol)
        if not converter:
            raise ValueError(f"未知协议: {protocol}")
        
        return await converter.to_wire_format(data)
    
    async def from_protocol(self, data: Any, protocol: str) -> Dict[str, Any]:
        """
        将协议格式数据转换为内部格式
        
        Args:
            data: 协议格式数据
            protocol: 源协议名称
            
        Returns:
            转换后的内部格式数据
        """
        converter = self._factory.get_converter(protocol)
        if not converter:
            raise ValueError(f"未知协议: {protocol}")
        
        return await converter.from_wire_format(data)
