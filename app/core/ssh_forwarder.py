from dataclasses import dataclass, field
from typing import AsyncGenerator, Dict, Optional, Set, List, Any, Tuple, Union, Callable
from datetime import datetime
import asyncio
import asyncssh
import logging
import time
import uuid
import socket
import os
import json
from enum import Enum, auto
from pathlib import Path

from app.core.base import BaseComponent
from app.utils.error_handler import error_boundary

logger = logging.getLogger(__name__)


class ForwardType(Enum):
    """转发类型枚举"""
    LOCAL = auto()          # 本地端口转发
    REMOTE = auto()         # 远程端口转发
    DYNAMIC = auto()        # 动态端口转发 (SOCKS)
    X11 = auto()            # X11转发


class TunnelStatus(Enum):
    """隧道状态枚举"""
    CONNECTING = auto()     # 正在连接
    CONNECTED = auto()      # 已连接
    DISCONNECTED = auto()   # 已断开
    FAILED = auto()         # 连接失败
    RECONNECTING = auto()   # 正在重连


@dataclass
class ForwardConfig:
    """转发配置"""
    forward_type: ForwardType
    listen_host: str
    listen_port: int
    dest_host: Optional[str] = None
    dest_port: Optional[int] = None
    bind_addr: str = "127.0.0.1"
    allow_remote_hosts: bool = False
    description: Optional[str] = None


@dataclass
class TunnelInfo:
    """SSH隧道信息"""
    tunnel_id: str
    host: str
    port: int
    username: str
    forwards: List[ForwardConfig] = field(default_factory=list)
    status: TunnelStatus = TunnelStatus.DISCONNECTED
    created_at: float = field(default_factory=time.time)
    last_connected: Optional[float] = None
    last_disconnected: Optional[float] = None
    connection_count: int = 0
    error_count: int = 0
    last_error: Optional[str] = None
    session: Optional[Any] = None
    connection: Optional[Any] = None
    listener_tasks: Dict[str, asyncio.Task] = field(default_factory=dict)
    reconnect_attempt: int = 0
    client_version: str = "Cobalt_SSH_Forwarder_1.0"
    use_compression: bool = True
    keepalive_interval: int = 60
    metadata: Dict[str, Any] = field(default_factory=dict)


class SSHForwarder(BaseComponent):
    """
    SSH转发器，用于创建和管理SSH隧道及端口转发
    
    功能：
    1. 支持创建和管理多个SSH隧道连接
    2. 提供本地、远程和动态端口转发功能
    3. 支持X11转发
    4. 自动重连和连接保活
    5. 支持基于密码和密钥的认证
    """
    
    def __init__(self, known_hosts_path: Optional[str] = None,
                 keys_path: Optional[str] = None,
                 reconnect_interval: int = 5,
                 max_reconnect_attempts: int = 10,
                 name: str = "ssh_forwarder"):
        """
        初始化SSH转发器
        
        Args:
            known_hosts_path: 已知主机文件路径
            keys_path: SSH密钥目录路径
            reconnect_interval: 重连间隔（秒）
            max_reconnect_attempts: 最大重连尝试次数
            name: 组件名称
        """
        super().__init__(name=name)
        self._tunnels: Dict[str, TunnelInfo] = {}
        self._active_tunnels: Set[str] = set()
        self._reconnect_tasks: Dict[str, asyncio.Task] = {}
        self._known_hosts_path = known_hosts_path or os.path.expanduser("~/.ssh/known_hosts")
        self._keys_path = keys_path or os.path.expanduser("~/.ssh")
        self._reconnect_interval = reconnect_interval
        self._max_reconnect_attempts = max_reconnect_attempts
        self._message_bus = None
        self._event_listeners: Dict[str, List[Callable]] = {}
        self._closed = False
    
    async def _start_impl(self) -> None:
        """启动SSH转发器"""
        self._closed = False
        # 确保SSH密钥目录存在
        os.makedirs(os.path.dirname(self._known_hosts_path), exist_ok=True)
        os.makedirs(self._keys_path, exist_ok=True)
        logger.info(f"SSH转发器启动，已知主机文件: {self._known_hosts_path}")
    
    async def _stop_impl(self) -> None:
        """停止SSH转发器"""
        self._closed = True
        
        # 关闭所有活跃隧道
        for tunnel_id in list(self._active_tunnels):
            await self.close_tunnel(tunnel_id)
        
        # 取消所有重连任务
        for task in self._reconnect_tasks.values():
            task.cancel()
        
        if self._reconnect_tasks:
            await asyncio.gather(*self._reconnect_tasks.values(), return_exceptions=True)
        
        logger.info("SSH转发器已停止")
    
    @error_boundary()
    async def create_tunnel(self, host: str, port: int, username: str,
                          password: Optional[str] = None,
                          key_file: Optional[str] = None,
                          key_passphrase: Optional[str] = None,
                          forwards: Optional[List[ForwardConfig]] = None,
                          auto_connect: bool = True,
                          client_keys: Optional[List[str]] = None,
                          keepalive_interval: int = 60,
                          metadata: Optional[Dict[str, Any]] = None) -> str:
        """
        创建SSH隧道
        
        Args:
            host: SSH服务器主机名或IP
            port: SSH服务器端口
            username: 用户名
            password: 密码（可选）
            key_file: 私钥文件路径（可选）
            key_passphrase: 私钥密码（可选）
            forwards: 转发配置列表
            auto_connect: 是否自动连接
            client_keys: 客户端密钥列表
            keepalive_interval: 保活间隔（秒）
            metadata: 元数据
            
        Returns:
            隧道ID
        """
        # 生成隧道ID
        tunnel_id = str(uuid.uuid4())
        
        # 处理客户端密钥
        if client_keys is None and key_file:
            client_keys = [key_file]
        
        # 创建隧道信息
        tunnel_info = TunnelInfo(
            tunnel_id=tunnel_id,
            host=host,
            port=port,
            username=username,
            forwards=forwards or [],
            keepalive_interval=keepalive_interval,
            metadata=metadata or {}
        )
        
        # 存储认证信息到元数据（注意：实际应用中应加密存储这些敏感信息）
        if password:
            tunnel_info.metadata["password"] = password
        if key_file:
            tunnel_info.metadata["key_file"] = key_file
        if key_passphrase:
            tunnel_info.metadata["key_passphrase"] = key_passphrase
        if client_keys:
            tunnel_info.metadata["client_keys"] = client_keys
        
        # 存储隧道信息
        self._tunnels[tunnel_id] = tunnel_info
        
        # 触发隧道创建事件
        await self._trigger_event("tunnel_created", tunnel_id, tunnel_info)
        
        logger.info(f"创建SSH隧道: {tunnel_id}, 主机: {host}:{port}, 用户: {username}")
        
        # 如果指定自动连接，则立即连接隧道
        if auto_connect:
            asyncio.create_task(self.connect_tunnel(tunnel_id))
        
        return tunnel_id
    
    @error_boundary()
    async def connect_tunnel(self, tunnel_id: str) -> bool:
        """
        连接SSH隧道
        
        Args:
            tunnel_id: 隧道ID
            
        Returns:
            是否成功连接
        """
        if tunnel_id not in self._tunnels:
            logger.warning(f"隧道不存在: {tunnel_id}")
            return False
        
        tunnel_info = self._tunnels[tunnel_id]
        
        # 如果隧道已经连接，则直接返回
        if tunnel_info.status == TunnelStatus.CONNECTED:
            logger.info(f"隧道已经连接: {tunnel_id}")
            return True
        
        # 更新隧道状态
        tunnel_info.status = TunnelStatus.CONNECTING
        
        # 触发隧道连接开始事件
        await self._trigger_event("tunnel_connecting", tunnel_id, tunnel_info)
        
        try:
            # 准备SSH客户端选项
            options = {
                "known_hosts": self._known_hosts_path,
                "client_version": tunnel_info.client_version,
                "compression": tunnel_info.use_compression,
                "keepalive_interval": tunnel_info.keepalive_interval
            }
            
            # 添加认证选项
            if "password" in tunnel_info.metadata:
                options["password"] = tunnel_info.metadata["password"]
            
            if "client_keys" in tunnel_info.metadata:
                options["client_keys"] = tunnel_info.metadata["client_keys"]
                
            if "key_passphrase" in tunnel_info.metadata:
                options["passphrase"] = tunnel_info.metadata["key_passphrase"]
            
            # 连接到SSH服务器
            tunnel_info.connection = await asyncssh.connect(
                host=tunnel_info.host,
                port=tunnel_info.port,
                username=tunnel_info.username,
                **options
            )
            
            # 创建SSH会话
            tunnel_info.session = await tunnel_info.connection.create_session()
            
            # 更新隧道状态和连接统计
            tunnel_info.status = TunnelStatus.CONNECTED
            tunnel_info.last_connected = time.time()
            tunnel_info.connection_count += 1
            tunnel_info.reconnect_attempt = 0
            
            # 添加到活跃隧道集合
            self._active_tunnels.add(tunnel_id)
            
            # 设置连接关闭回调
            tunnel_info.connection.add_subsystem_handler(self._on_connection_close, tunnel_id)
            
            # 触发隧道连接成功事件
            await self._trigger_event("tunnel_connected", tunnel_id, tunnel_info)
            
            logger.info(f"SSH隧道连接成功: {tunnel_id}, 主机: {tunnel_info.host}:{tunnel_info.port}")
            
            # 启动所有转发
            await self._start_all_forwards(tunnel_id)
            
            return True
            
        except (OSError, asyncssh.Error) as e:
            # 更新隧道状态和错误信息
            tunnel_info.status = TunnelStatus.FAILED
            tunnel_info.last_error = str(e)
            tunnel_info.error_count += 1
            
            # 触发隧道连接失败事件
            await self._trigger_event("tunnel_failed", tunnel_id, tunnel_info, error=str(e))
            
            logger.error(f"SSH隧道连接失败: {tunnel_id}, 错误: {e}")
            
            # 尝试重连
            if tunnel_info.reconnect_attempt < self._max_reconnect_attempts:
                await self._schedule_reconnect(tunnel_id)
            
            return False
    
    async def _on_connection_close(self, tunnel_id: str) -> None:
        """
        处理连接关闭事件
        
        Args:
            tunnel_id: 隧道ID
        """
        if tunnel_id not in self._tunnels:
            return
        
        tunnel_info = self._tunnels[tunnel_id]
        
        # 更新隧道状态
        prev_status = tunnel_info.status
        tunnel_info.status = TunnelStatus.DISCONNECTED
        tunnel_info.last_disconnected = time.time()
        
        # 清理会话和连接
        tunnel_info.session = None
        tunnel_info.connection = None
        
        # 取消所有监听任务
        for task in tunnel_info.listener_tasks.values():
            task.cancel()
        
        # 从活跃隧道集合中移除
        if tunnel_id in self._active_tunnels:
            self._active_tunnels.remove(tunnel_id)
        
        # 触发隧道断开事件
        await self._trigger_event("tunnel_disconnected", tunnel_id, tunnel_info)
        
        logger.info(f"SSH隧道断开连接: {tunnel_id}, 之前状态: {prev_status.name}")
        
        # 如果不是手动关闭，尝试重连
        if not self._closed and prev_status == TunnelStatus.CONNECTED:
            await self._schedule_reconnect(tunnel_id)
    
    async def _schedule_reconnect(self, tunnel_id: str) -> None:
        """
        调度隧道重连
        
        Args:
            tunnel_id: 隧道ID
        """
        if tunnel_id not in self._tunnels:
            return
        
        tunnel_info = self._tunnels[tunnel_id]
        
        # 更新重连次数和状态
        tunnel_info.reconnect_attempt += 1
        tunnel_info.status = TunnelStatus.RECONNECTING
        
        # 计算重连延迟（指数退避）
        delay = min(self._reconnect_interval * (2 ** (tunnel_info.reconnect_attempt - 1)), 300)
        
        logger.info(f"调度SSH隧道重连: {tunnel_id}, 尝试次数: {tunnel_info.reconnect_attempt}, 延迟: {delay}秒")
        
        # 触发隧道重连事件
        await self._trigger_event("tunnel_reconnecting", tunnel_id, tunnel_info, attempt=tunnel_info.reconnect_attempt, delay=delay)
        
        # 创建重连任务
        reconnect_task = asyncio.create_task(self._delayed_reconnect(tunnel_id, delay))
        self._reconnect_tasks[tunnel_id] = reconnect_task
    
    async def _delayed_reconnect(self, tunnel_id: str, delay: float) -> None:
        """
        延迟重连
        
        Args:
            tunnel_id: 隧道ID
            delay: 延迟秒数
        """
        try:
            # 等待指定时间
            await asyncio.sleep(delay)
            
            # 尝试重新连接
            await self.connect_tunnel(tunnel_id)
            
        except asyncio.CancelledError:
            logger.info(f"SSH隧道重连任务取消: {tunnel_id}")
            
        except Exception as e:
            logger.error(f"SSH隧道重连失败: {tunnel_id}, 错误: {e}")
            
        finally:
            # 从重连任务字典中移除
            if tunnel_id in self._reconnect_tasks:
                del self._reconnect_tasks[tunnel_id]
    
    async def _start_all_forwards(self, tunnel_id: str) -> None:
        """
        启动所有转发
        
        Args:
            tunnel_id: 隧道ID
        """
        if tunnel_id not in self._tunnels:
            return
        
        tunnel_info = self._tunnels[tunnel_id]
        
        # 如果隧道未连接，则不能启动转发
        if tunnel_info.status != TunnelStatus.CONNECTED:
            logger.warning(f"SSH隧道未连接，无法启动转发: {tunnel_id}")
            return
        
        # 启动每个转发配置
        for i, forward in enumerate(tunnel_info.forwards):
            try:
                await self._start_forward(tunnel_id, i)
            except Exception as e:
                logger.error(f"启动转发失败: {tunnel_id}, 索引: {i}, 错误: {e}")
    
    @error_boundary()
    async def _start_forward(self, tunnel_id: str, forward_index: int) -> None:
        """
        启动单个转发
        
        Args:
            tunnel_id: 隧道ID
            forward_index: 转发配置索引
        """
        if tunnel_id not in self._tunnels:
            logger.warning(f"隧道不存在: {tunnel_id}")
            return
        
        tunnel_info = self._tunnels[tunnel_id]
        
        if forward_index >= len(tunnel_info.forwards):
            logger.warning(f"转发索引超出范围: {tunnel_id}, 索引: {forward_index}")
            return
        
        forward = tunnel_info.forwards[forward_index]
        
        # 根据转发类型启动不同的转发
        try:
            if forward.forward_type == ForwardType.LOCAL:
                # 本地端口转发
                listener = await tunnel_info.connection.forward_local_port(
                    forward.listen_host,
                    forward.listen_port,
                    forward.dest_host,
                    forward.dest_port
                )
                
                logger.info(f"启动本地端口转发: {forward.listen_host}:{forward.listen_port} -> {forward.dest_host}:{forward.dest_port}")
                
                # 触发转发创建事件
                await self._trigger_event("forward_created", tunnel_id, forward_type="local",
                                       listen=f"{forward.listen_host}:{forward.listen_port}",
                                       destination=f"{forward.dest_host}:{forward.dest_port}")
                
                # 创建监听任务（保持监听器活跃）
                task_id = f"local_{forward_index}"
                tunnel_info.listener_tasks[task_id] = asyncio.create_task(self._maintain_listener(tunnel_id, task_id, listener))
                
            elif forward.forward_type == ForwardType.REMOTE:
                # 远程端口转发
                listener = await tunnel_info.connection.forward_remote_port(
                    forward.listen_host,
                    forward.listen_port,
                    forward.dest_host,
                    forward.dest_port
                )
                
                logger.info(f"启动远程端口转发: {forward.listen_host}:{forward.listen_port} -> {forward.dest_host}:{forward.dest_port}")
                
                # 触发转发创建事件
                await self._trigger_event("forward_created", tunnel_id, forward_type="remote",
                                       listen=f"{forward.listen_host}:{forward.listen_port}",
                                       destination=f"{forward.dest_host}:{forward.dest_port}")
                
                # 创建监听任务
                task_id = f"remote_{forward_index}"
                tunnel_info.listener_tasks[task_id] = asyncio.create_task(self._maintain_listener(tunnel_id, task_id, listener))
                
            elif forward.forward_type == ForwardType.DYNAMIC:
                # 动态端口转发 (SOCKS)
                listener = await tunnel_info.connection.forward_socks(
                    forward.listen_host,
                    forward.listen_port
                )
                
                logger.info(f"启动动态端口转发: {forward.listen_host}:{forward.listen_port}")
                
                # 触发转发创建事件
                await self._trigger_event("forward_created", tunnel_id, forward_type="dynamic",
                                       listen=f"{forward.listen_host}:{forward.listen_port}")
                
                # 创建监听任务
                task_id = f"dynamic_{forward_index}"
                tunnel_info.listener_tasks[task_id] = asyncio.create_task(self._maintain_listener(tunnel_id, task_id, listener))
                
            elif forward.forward_type == ForwardType.X11:
                # X11转发
                # 注意：X11转发需要在创建会话时设置，这里简化处理
                await tunnel_info.connection.forward_x11()
                
                logger.info(f"启动X11转发")
                
                # 触发转发创建事件
                await self._trigger_event("forward_created", tunnel_id, forward_type="x11")
                
                # X11转发不需要额外的监听任务
                
        except (OSError, asyncssh.Error) as e:
            logger.error(f"启动转发失败: {tunnel_id}, 索引: {forward_index}, 错误: {e}")
            # 触发转发错误事件
            await self._trigger_event("forward_error", tunnel_id, forward_index=forward_index, error=str(e))
    
    async def _maintain_listener(self, tunnel_id: str, task_id: str, listener: Any) -> None:
        """
        维护监听器任务
        
        Args:
            tunnel_id: 隧道ID
            task_id: 任务ID
            listener: 监听器对象
        """
        try:
            # 保持监听器运行
            while not self._closed and tunnel_id in self._tunnels:
                tunnel_info = self._tunnels[tunnel_id]
                
                if tunnel_info.status != TunnelStatus.CONNECTED:
                    # 如果隧道断开，停止监听
                    break
                
                # 简单的睡眠循环，实际应用中可能需要更复杂的逻辑
                await asyncio.sleep(5)
                
        except asyncio.CancelledError:
            # 任务被取消
            logger.debug(f"监听器任务取消: {tunnel_id}, {task_id}")
            
        except Exception as e:
            logger.error(f"监听器任务错误: {tunnel_id}, {task_id}, 错误: {e}")
            
        finally:
            # 关闭监听器
            if listener:
                listener.close()
                
            # 从任务字典中移除
            if tunnel_id in self._tunnels and task_id in self._tunnels[tunnel_id].listener_tasks:
                del self._tunnels[tunnel_id].listener_tasks[task_id]
    
    @error_boundary()
    async def add_forward(self, tunnel_id: str, forward: ForwardConfig) -> int:
        """
        向隧道添加转发配置
        
        Args:
            tunnel_id: 隧道ID
            forward: 转发配置
            
        Returns:
            转发配置索引
        """
        if tunnel_id not in self._tunnels:
            logger.warning(f"隧道不存在: {tunnel_id}")
            return -1
        
        tunnel_info = self._tunnels[tunnel_id]
        
        # 添加转发配置
        tunnel_info.forwards.append(forward)
        forward_index = len(tunnel_info.forwards) - 1
        
        logger.info(f"添加转发配置: {tunnel_id}, 类型: {forward.forward_type.name}, 索引: {forward_index}")
        
        # 如果隧道已连接，立即启动转发
        if tunnel_info.status == TunnelStatus.CONNECTED:
            await self._start_forward(tunnel_id, forward_index)
        
        return forward_index
    
    @error_boundary()
    async def remove_forward(self, tunnel_id: str, forward_index: int) -> bool:
        """
        从隧道移除转发配置
        
        Args:
            tunnel_id: 隧道ID
            forward_index: 转发配置索引
            
        Returns:
            是否成功移除
        """
        if tunnel_id not in self._tunnels:
            logger.warning(f"隧道不存在: {tunnel_id}")
            return False
        
        tunnel_info = self._tunnels[tunnel_id]
        
        if forward_index >= len(tunnel_info.forwards):
            logger.warning(f"转发索引超出范围: {tunnel_id}, 索引: {forward_index}")
            return False
        
        # 获取转发类型
        forward_type = tunnel_info.forwards[forward_index].forward_type
        
        # 生成关联的任务ID
        task_ids = [f"{forward_type.name.lower()}_{forward_index}"]
        
        # 取消关联的任务
        for task_id in task_ids:
            if task_id in tunnel_info.listener_tasks:
                tunnel_info.listener_tasks[task_id].cancel()
        
        # 移除转发配置
        removed_forward = tunnel_info.forwards.pop(forward_index)
        
        logger.info(f"移除转发配置: {tunnel_id}, 类型: {forward_type.name}, 索引: {forward_index}")
        
        # 触发转发移除事件
        await self._trigger_event("forward_removed", tunnel_id, forward_type=forward_type.name.lower(), forward_index=forward_index)
        
        return True
    
    @error_boundary()
    async def close_tunnel(self, tunnel_id: str) -> bool:
        """
        关闭SSH隧道
        
        Args:
            tunnel_id: 隧道ID
            
        Returns:
            是否成功关闭
        """
        if tunnel_id not in self._tunnels:
            logger.warning(f"隧道不存在: {tunnel_id}")
            return False
        
        tunnel_info = self._tunnels[tunnel_id]
        
        if tunnel_info.status == TunnelStatus.DISCONNECTED:
            logger.warning(f"隧道已经断开: {tunnel_id}")
            return True
        
        # 取消所有监听任务
        for task in tunnel_info.listener_tasks.values():
            task.cancel()
        
        tunnel_info.listener_tasks.clear()
        
        # 取消重连任务
        if tunnel_id in self._reconnect_tasks:
            self._reconnect_tasks[tunnel_id].cancel()
            del self._reconnect_tasks[tunnel_id]
        
        # 关闭连接
        if tunnel_info.connection:
            tunnel_info.connection.close()
            
        # 等待连接完全关闭
        if tunnel_info.connection:
            await tunnel_info.connection.wait_closed()
        
        # 更新隧道状态
        tunnel_info.status = TunnelStatus.DISCONNECTED
        tunnel_info.last_disconnected = time.time()
        tunnel_info.session = None
        tunnel_info.connection = None
        
        # 从活跃隧道集合中移除
        if tunnel_id in self._active_tunnels:
            self._active_tunnels.remove(tunnel_id)
        
        # 触发隧道关闭事件
        await self._trigger_event("tunnel_closed", tunnel_id, tunnel_info)
        
        logger.info(f"SSH隧道关闭: {tunnel_id}")
        
        return True
    
    @error_boundary()
    async def remove_tunnel(self, tunnel_id: str) -> bool:
        """
        移除SSH隧道
        
        Args:
            tunnel_id: 隧道ID
            
        Returns:
            是否成功移除
        """
        # 先关闭隧道
        await self.close_tunnel(tunnel_id)
        
        # 从隧道字典中移除
        if tunnel_id in self._tunnels:
            del self._tunnels[tunnel_id]
            
            # 触发隧道移除事件
            await self._trigger_event("tunnel_removed", tunnel_id)
            
            logger.info(f"SSH隧道移除: {tunnel_id}")
            
            return True
        
        return False
    
    async def get_tunnel_info(self, tunnel_id: str) -> Optional[TunnelInfo]:
        """
        获取隧道信息
        
        Args:
            tunnel_id: 隧道ID
            
        Returns:
            隧道信息，不存在则返回None
        """
        return self._tunnels.get(tunnel_id)
    
    async def list_tunnels(self, active_only: bool = False) -> List[TunnelInfo]:
        """
        列出所有隧道
        
        Args:
            active_only: 是否只列出活跃的隧道
            
        Returns:
            隧道信息列表
        """
        if active_only:
            return [tunnel for tunnel_id, tunnel in self._tunnels.items() if tunnel_id in self._active_tunnels]
        return list(self._tunnels.values())
    
    async def get_tunnel_status(self, tunnel_id: str) -> Optional[TunnelStatus]:
        """
        获取隧道状态
        
        Args:
            tunnel_id: 隧道ID
            
        Returns:
            隧道状态，不存在则返回None
        """
        if tunnel_id not in self._tunnels:
            return None
        return self._tunnels[tunnel_id].status
    
    @error_boundary()
    async def execute_command(self, tunnel_id: str, command: str,
                            timeout: float = 30.0, get_pty: bool = False) -> Dict[str, Any]:
        """
        通过SSH隧道执行命令
        
        Args:
            tunnel_id: 隧道ID
            command: 要执行的命令
            timeout: 超时时间（秒）
            get_pty: 是否请求伪终端
            
        Returns:
            包含命令执行结果的字典
        """
        if tunnel_id not in self._tunnels:
            logger.warning(f"隧道不存在: {tunnel_id}")
            return {"success": False, "error": "Tunnel not found"}
        
        tunnel_info = self._tunnels[tunnel_id]
        
        if tunnel_info.status != TunnelStatus.CONNECTED:
            logger.warning(f"SSH隧道未连接: {tunnel_id}")
            return {"success": False, "error": "Tunnel not connected"}
        
        try:
            # 执行命令
            result = await asyncio.wait_for(
                tunnel_info.connection.run(command, check=False, get_pty=get_pty),
                timeout=timeout
            )
            
            # 构建结果字典
            command_result = {
                "success": result.exit_status == 0,
                "exit_status": result.exit_status,
                "stdout": result.stdout,
                "stderr": result.stderr,
                "command": command
            }
            
            logger.debug(f"命令执行结果: {tunnel_id}, 状态: {result.exit_status}, 命令: {command}")
            
            return command_result
            
        except asyncio.TimeoutError:
            logger.error(f"命令执行超时: {tunnel_id}, 命令: {command}")
            return {"success": False, "error": f"Command timed out after {timeout} seconds", "command": command}
            
        except Exception as e:
            logger.error(f"命令执行失败: {tunnel_id}, 错误: {e}, 命令: {command}")
            return {"success": False, "error": str(e), "command": command}
    
    @error_boundary()
    async def upload_file(self, tunnel_id: str, local_path: str,
                        remote_path: str, callback: Optional[Callable] = None) -> Dict[str, Any]:
        """
        通过SSH隧道上传文件
        
        Args:
            tunnel_id: 隧道ID
            local_path: 本地文件路径
            remote_path: 远程文件路径
            callback: 进度回调函数
            
        Returns:
            包含上传结果的字典
        """
        if tunnel_id not in self._tunnels:
            logger.warning(f"隧道不存在: {tunnel_id}")
            return {"success": False, "error": "Tunnel not found"}
        
        tunnel_info = self._tunnels[tunnel_id]
        
        if tunnel_info.status != TunnelStatus.CONNECTED:
            logger.warning(f"SSH隧道未连接: {tunnel_id}")
            return {"success": False, "error": "Tunnel not connected"}
        
        try:
            # 检查本地文件是否存在
            if not os.path.exists(local_path):
                return {"success": False, "error": "Local file not found", "local_path": local_path}
            
            # 获取文件大小
            file_size = os.path.getsize(local_path)
            
            start_time = time.time()
            
            # 上传文件
            await tunnel_info.connection.run(f"mkdir -p {os.path.dirname(remote_path)}")
            
            # 使用SFTP上传
            async with tunnel_info.connection.start_sftp_client() as sftp:
                await sftp.put(
                    local_path,
                    remote_path,
                    progress_handler=callback
                )
            
            end_time = time.time()
            duration = end_time - start_time
            speed = file_size / duration if duration > 0 else 0
            
            # 构建结果字典
            result = {
                "success": True,
                "local_path": local_path,
                "remote_path": remote_path,
                "file_size": file_size,
                "duration": duration,
                "speed": speed
            }
            
            logger.info(f"文件上传成功: {tunnel_id}, 本地: {local_path}, 远程: {remote_path}, 大小: {file_size}字节")
            
            return result
            
        except Exception as e:
            logger.error(f"文件上传失败: {tunnel_id}, 错误: {e}, 本地: {local_path}, 远程: {remote_path}")
            return {"success": False, "error": str(e), "local_path": local_path, "remote_path": remote_path}
    
    @error_boundary()
    async def download_file(self, tunnel_id: str, remote_path: str,
                          local_path: str, callback: Optional[Callable] = None) -> Dict[str, Any]:
        """
        通过SSH隧道下载文件
        
        Args:
            tunnel_id: 隧道ID
            remote_path: 远程文件路径
            local_path: 本地文件路径
            callback: 进度回调函数
            
        Returns:
            包含下载结果的字典
        """
        if tunnel_id not in self._tunnels:
            logger.warning(f"隧道不存在: {tunnel_id}")
            return {"success": False, "error": "Tunnel not found"}
        
        tunnel_info = self._tunnels[tunnel_id]
        
        if tunnel_info.status != TunnelStatus.CONNECTED:
            logger.warning(f"SSH隧道未连接: {tunnel_id}")
            return {"success": False, "error": "Tunnel not connected"}
        
        try:
            start_time = time.time()
            
            # 确保本地目录存在
            os.makedirs(os.path.dirname(os.path.abspath(local_path)), exist_ok=True)
            
            # 使用SFTP下载
            async with tunnel_info.connection.start_sftp_client() as sftp:
                # 检查远程文件是否存在
                try:
                    file_attrs = await sftp.stat(remote_path)
                    file_size = file_attrs.size
                except asyncssh.SFTPNoSuchFile:
                    return {"success": False, "error": "Remote file not found", "remote_path": remote_path}
                
                # 下载文件
                await sftp.get(
                    remote_path,
                    local_path,
                    progress_handler=callback
                )
            
            end_time = time.time()
            duration = end_time - start_time
            speed = file_size / duration if duration > 0 else 0
            
            # 构建结果字典
            result = {
                "success": True,
                "remote_path": remote_path,
                "local_path": local_path,
                "file_size": file_size,
                "duration": duration,
                "speed": speed
            }
            
            logger.info(f"文件下载成功: {tunnel_id}, 远程: {remote_path}, 本地: {local_path}, 大小: {file_size}字节")
            
            return result
            
        except Exception as e:
            logger.error(f"文件下载失败: {tunnel_id}, 错误: {e}, 远程: {remote_path}, 本地: {local_path}")
            return {"success": False, "error": str(e), "remote_path": remote_path, "local_path": local_path}
    
    def add_event_listener(self, event_type: str, listener: Callable) -> None:
        """
        添加事件监听器
        
        Args:
            event_type: 事件类型
            listener: 监听器函数
        """
        if event_type not in self._event_listeners:
            self._event_listeners[event_type] = []
        
        self._event_listeners[event_type].append(listener)
        logger.debug(f"添加SSH事件监听器: {event_type}")
    
    async def _trigger_event(self, event_type: str, tunnel_id: str, 
                           tunnel_info: TunnelInfo = None, **kwargs) -> None:
        """触发事件"""
        # 调用注册的监听器
        for listener in self._event_listeners.get(event_type, []):
            try:
                if asyncio.iscoroutinefunction(listener):
                    await listener(tunnel_id, tunnel_info=tunnel_info, **kwargs)
                else:
                    listener(tunnel_id, tunnel_info=tunnel_info, **kwargs)
            except Exception as e:
                logger.error(f"执行SSH事件监听器失败: {e}")
        
        # 发布到消息总线
        if self._message_bus:
            try:
                event_data = {
                    "tunnel_id": tunnel_id,
                    "timestamp": time.time()
                }
                
                if tunnel_info:
                    event_data.update({
                        "host": tunnel_info.host,
                        "port": tunnel_info.port,
                        "username": tunnel_info.username,
                        "status": tunnel_info.status.name,
                        "connection_count": tunnel_info.connection_count,
                        "error_count": tunnel_info.error_count,
                        "forwards_count": len(tunnel_info.forwards)
                    })
                
                event_data.update(kwargs)
                
                await self._message_bus.publish(f"ssh.{event_type}", event_data)
            except Exception as e:
                logger.error(f"发布SSH事件到消息总线失败: {e}")
    
    async def set_message_bus(self, message_bus) -> None:
        """设置消息总线引用"""
        self._message_bus = message_bus
        logger.info("消息总线已与SSH转发器集成")
    
    async def handle_message(self, topic: str, data: Any) -> None:
        """处理接收到的消息"""
        if topic.startswith("ssh.command."):
            command = topic.split(".")[-1]
            
            if command == "create_tunnel":
                # 创建隧道
                if isinstance(data, dict) and "host" in data and "port" in data and "username" in data:
                    try:
                        tunnel_id = await self.create_tunnel(
                            host=data["host"],
                            port=data["port"],
                            username=data["username"],
                            password=data.get("password"),
                            key_file=data.get("key_file"),
                            key_passphrase=data.get("key_passphrase"),
                            forwards=data.get("forwards"),
                            auto_connect=data.get("auto_connect", True),
                            metadata=data.get("metadata")
                        )
                        
                        # 发送响应
                        if self._message_bus:
                            await self._message_bus.publish("ssh.response.tunnel_created", {
                                "tunnel_id": tunnel_id,
                                "success": True
                            })
                    except Exception as e:
                        logger.error(f"创建SSH隧道失败: {e}")
                        
                        # 发送错误响应
                        if self._message_bus:
                            await self._message_bus.publish("ssh.response.tunnel_created", {
                                "success": False,
                                "error": str(e)
                            })
            
            elif command == "connect_tunnel":
                # 连接隧道
                if isinstance(data, dict) and "tunnel_id" in data:
                    tunnel_id = data["tunnel_id"]
                    
                    success = await self.connect_tunnel(tunnel_id)
                    
                    # 发送响应
                    if self._message_bus:
                        await self._message_bus.publish("ssh.response.tunnel_connected", {
                            "tunnel_id": tunnel_id,
                            "success": success
                        })
            
            elif command == "close_tunnel":
                # 关闭隧道
                if isinstance(data, dict) and "tunnel_id" in data:
                    tunnel_id = data["tunnel_id"]
                    
                    success = await self.close_tunnel(tunnel_id)
                    
                    # 发送响应
                    if self._message_bus:
                        await self._message_bus.publish("ssh.response.tunnel_closed", {
                            "tunnel_id": tunnel_id,
                            "success": success
                        })
            
            elif command == "execute":
                # 执行命令
                if isinstance(data, dict) and "tunnel_id" in data and "command" in data:
                    tunnel_id = data["tunnel_id"]
                    command_str = data["command"]
                    timeout = data.get("timeout", 30.0)
                    get_pty = data.get("get_pty", False)
                    
                    result = await self.execute_command(tunnel_id, command_str, timeout, get_pty)
                    
                    # 发送响应
                    if self._message_bus:
                        await self._message_bus.publish("ssh.response.execute", {
                            "tunnel_id": tunnel_id,
                            "request_id": data.get("request_id"),
                            "result": result
                        })
    
    def get_metrics(self) -> Dict[str, Any]:
        """获取SSH转发器指标"""
        metrics = super().metrics
        
        # 计算隧道统计信息
        total_tunnels = len(self._tunnels)
        active_tunnels = len(self._active_tunnels)
        total_forwards = sum(len(t.forwards) for t in self._tunnels.values())
        total_errors = sum(t.error_count for t in self._tunnels.values())
        
        # 按状态统计隧道数量
        tunnels_by_status = {}
        for status in TunnelStatus:
            tunnels_by_status[status.name] = len([
                t for t in self._tunnels.values() if t.status == status
            ])
        
        # 按类型统计转发数量
        forwards_by_type = {}
        for forward_type in ForwardType:
            count = 0
            for tunnel in self._tunnels.values():
                count += len([f for f in tunnel.forwards if f.forward_type == forward_type])
            forwards_by_type[forward_type.name] = count
        
        metrics.update({
            'total_tunnels': total_tunnels,
            'active_tunnels': active_tunnels,
            'total_forwards': total_forwards,
            'total_errors': total_errors,
            'tunnels_by_status': tunnels_by_status,
            'forwards_by_type': forwards_by_type,
            'reconnecting_tunnels': len(self._reconnect_tasks)
        })
        
        return metrics
