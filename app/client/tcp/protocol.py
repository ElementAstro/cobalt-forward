import asyncio
import struct
from typing import Optional, Union, Dict, Tuple, Any, List
import zlib
from loguru import logger
import numpy as np
from concurrent.futures import ThreadPoolExecutor
import functools
import time
import json
from enum import IntEnum


class PacketType(IntEnum):
    """数据包类型"""
    DATA = 0       # 普通数据
    HEARTBEAT = 1  # 心跳包
    CONTROL = 2    # 控制命令
    ERROR = 3      # 错误信息
    ACK = 4        # 确认包


class PacketProtocol:
    """数据包协议处理类

    数据包格式:
    +--------+--------+--------+----------+----------------+
    | 魔数   | 类型   | 压缩标志| 数据长度 | 数据体         |
    | 2字节  | 1字节  | 1字节   | 4字节    | 变长           |
    +--------+--------+--------+----------+----------------+

    魔数: 固定为 0xCB55 (CoBalt 55)
    类型: 数据包类型，见 PacketType
    压缩标志: 0=不压缩, 1=zlib压缩
    数据长度: 数据体长度，不包括头部
    数据体: 实际数据，JSON或二进制
    """

    MAGIC = 0xCB55  # 魔数
    HEADER_SIZE = 8  # 头部大小

    def __init__(self, compression_threshold: int = 1024, compression_level: int = 6):
        """初始化协议处理器

        Args:
            compression_threshold: 启用压缩的阈值，大于此值才会压缩
            compression_level: 压缩级别 (0-9)
        """
        self.compression_threshold = compression_threshold
        self.compression_level = compression_level

    def pack(self, data: Union[bytes, dict], packet_type: PacketType = PacketType.DATA) -> bytes:
        """打包数据为二进制格式

        Args:
            data: 要发送的数据，可以是字节或字典
            packet_type: 数据包类型

        Returns:
            打包后的二进制数据
        """
        # 如果是字典，转换为JSON
        if isinstance(data, dict):
            data = json.dumps(data).encode('utf-8')

        # 决定是否压缩
        compressed = 0
        if len(data) >= self.compression_threshold:
            compressed_data = zlib.compress(data, self.compression_level)
            # 只有当压缩后确实变小时才使用压缩数据
            if len(compressed_data) < len(data):
                data = compressed_data
                compressed = 1

        # 构建数据包头
        header = struct.pack('>HBBl', self.MAGIC,
                             packet_type, compressed, len(data))

        # 返回完整数据包
        return header + data

    def unpack(self, data: bytes) -> Tuple[PacketType, bytes, int]:
        """解包二进制数据

        Args:
            data: 接收到的二进制数据

        Returns:
            元组 (数据包类型, 数据体, 头部大小)

        Raises:
            ValueError: 数据格式错误
        """
        if len(data) < self.HEADER_SIZE:
            raise ValueError(
                f"Data too short: {len(data)} bytes, expected at least {self.HEADER_SIZE}")

        # 解析头部
        magic, ptype, compressed, length = struct.unpack(
            '>HBBl', data[:self.HEADER_SIZE])

        # 验证魔数
        if magic != self.MAGIC:
            raise ValueError(
                f"Invalid magic number: 0x{magic:04X}, expected 0x{self.MAGIC:04X}")

        # 检查数据长度
        if len(data) < self.HEADER_SIZE + length:
            raise ValueError(
                f"Data incomplete: got {len(data)}, expected {self.HEADER_SIZE + length}")

        # 提取数据体
        body = data[self.HEADER_SIZE:self.HEADER_SIZE + length]

        # 如果数据被压缩，解压缩
        if compressed:
            body = zlib.decompress(body)

        return PacketType(ptype), body, self.HEADER_SIZE + length

    def try_parse_json(self, data: bytes) -> Union[Dict[str, Any], bytes]:
        """尝试将数据解析为JSON，失败则返回原始数据"""
        try:
            return json.loads(data.decode('utf-8'))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return data

    def create_heartbeat(self) -> bytes:
        """创建心跳包"""
        timestamp = time.time()
        return self.pack({'timestamp': timestamp}, PacketType.HEARTBEAT)

    def create_ack(self, message_id: Optional[str] = None) -> bytes:
        """创建确认包"""
        data = {'timestamp': time.time()}
        if message_id:
            data['message_id'] = message_id
        return self.pack(data, PacketType.ACK)

    def create_error(self, error_code: int, error_message: str) -> bytes:
        """创建错误包"""
        return self.pack({
            'error_code': error_code,
            'error_message': error_message,
            'timestamp': time.time()
        }, PacketType.ERROR)
