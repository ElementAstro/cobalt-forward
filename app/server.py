from app.routers import plugin
from app.routers import websocket, commands, system
from dataclasses import asdict
import ssl
import time
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Body, Query
from fastapi.middleware.cors import CORSMiddleware
import asyncio
from typing import Any, Dict, Set, Optional, List
import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime
import uuid
from app.commands import BulkOperationCommand, DataQueryCommand, DeviceControlCommand, SystemConfigCommand
from app.core.event_bus import Event, EventBus, EventPriority
from app.core.message_transformer import CompressionTransform, MessageEnricher, MessageValidator
from app.core.middleware import ErrorHandlerMiddleware, PerformanceMiddleware, SecurityMiddleware
from app.plugin.plugin_system import PluginManager
from app.core.protocol_converter import MQTTConverter, ModbusConverter
from app.utils.performance import PerformanceMonitor
from logger_config import setup_logging
from fastapi.responses import JSONResponse
from aiocache import Cache
from app.config.config_manager import ConfigManager

# Import required components
from app.client.tcp.client import ClientConfig
from app.core.message_bus import MessageBusEnabledTCPClient, MessageBus, Message, MessageType
from app.core.command_dispatcher import CommandTransmitter, UserCommand, UserCommandHandler, ScheduledCommand, BatchCommand
from app.core.message_processor import MessageTransformer, MessageFilter, json_payload_transformer
from app.client.ssh.client import SSHClient
from app.client.ssh.config import SSHConfig

# 在文件开头添加导入
from app.client.ftp.client import EnhancedFTPServer
from app.client.ftp.config import FTPConfig
from app.client.ftp.exception import FTPError

# 设置日志
logger = setup_logging()


class EnhancedForwarderConfig:
    """Enhanced configuration with message bus support"""
    tcp_host: str
    tcp_port: int
    websocket_host: str
    websocket_port: int
    tcp_timeout: float = 30.0
    tcp_retry_attempts: int = 3
    enable_command_handling: bool = True
    command_timeout: float = 30.0
    mqtt_enabled: bool = False
    mqtt_host: str = "localhost"
    mqtt_port: int = 1883
    mqtt_username: str = None
    mqtt_password: str = None
    mqtt_topics: List[str] = []
    websocket_enabled: bool = True
    websocket_ssl: bool = False
    websocket_cert: str = None
    websocket_key: str = None
    # 添加SSH配置项
    ssh_enabled: bool = False
    ssh_host: str = "localhost"
    ssh_port: int = 22
    ssh_username: str = None
    ssh_password: str = None
    ssh_key_path: str = None
    ssh_known_hosts: str = None
    ssh_pool_size: int = 5
    # 添加FTP配置项
    ftp_enabled: bool = False
    ftp_host: str = "localhost"
    ftp_port: int = 21
    ftp_user: str = None
    ftp_password: str = None
    ftp_root_dir: str = "/tmp/ftp"
    ftp_passive_ports_start: int = 60000
    ftp_passive_ports_end: int = 65535


class EnhancedConnectionManager:
    """Enhanced connection manager with message bus integration"""

    def __init__(self, message_bus: MessageBus, command_transmitter: CommandTransmitter):
        self.active_connections: Set[WebSocket] = set()
        self._logger = logging.getLogger(__name__)
        self._message_bus = message_bus
        self._command_transmitter = command_transmitter
        self._subscription_queues: Dict[WebSocket, asyncio.Queue] = {}

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.add(websocket)

        # Create message subscription for this connection
        queue = await self._message_bus.subscribe("websocket.broadcast")
        self._subscription_queues[websocket] = queue

        # Start message forwarding task
        asyncio.create_task(self._forward_messages(websocket, queue))

    async def disconnect(self, websocket: WebSocket):
        if websocket in self._subscription_queues:
            queue = self._subscription_queues.pop(websocket)
            await self._message_bus.unsubscribe("websocket.broadcast", queue)
        self.active_connections.remove(websocket)

    async def _forward_messages(self, websocket: WebSocket, queue: asyncio.Queue):
        """Forward messages from message bus to WebSocket"""
        try:
            while True:
                message: Message = await queue.get()
                await websocket.send_json({
                    "type": message.type.value,
                    "data": message.data
                })
        except Exception as e:
            self._logger.error(f"Message forwarding error: {str(e)}")
            await self.disconnect(websocket)

    async def handle_client_message(self, websocket: WebSocket, message_data: str):
        """Handle incoming client messages with command support"""
        try:
            data = json.loads(message_data)

            if data.get("type") == "command":
                # Handle as command
                command = UserCommand(data.get("payload", {}))
                result = await self._command_transmitter.send(command)
                await websocket.send_json({
                    "type": "command_result",
                    "status": result.status.value,
                    "data": result.result if result.result else None,
                    "error": str(result.error) if result.error else None
                })
            else:
                # Handle as regular message
                await self._message_bus.publish(
                    "tcp.outbound",
                    data,
                    MessageType.EVENT
                )
        except Exception as e:
            await websocket.send_json({
                "type": "error",
                "message": str(e)
            })


class EnhancedTCPWebSocketForwarder:
    """Enhanced forwarder with message bus and command support"""

    def __init__(self, config: EnhancedForwarderConfig):
        self.config = config
        self.tcp_client: Optional[MessageBusEnabledTCPClient] = None
        self._logger = logging.getLogger(__name__)
        self.command_transmitter: Optional[CommandTransmitter] = None
        self.message_transformer = MessageTransformer()
        self.scheduled_tasks: Dict[str, asyncio.Task] = {}
        self.performance_monitor = PerformanceMonitor()
        self.cache = Cache(Cache.MEMORY)
        self._config_subscriber = None
        self.mqtt_client = None
        self.websocket_client = None
        self.protocol_handlers = {}
        self.event_bus = EventBus()
        self.plugin_manager = PluginManager()
        self.ssh_client = None
        self.ssh_pool = None
        self.ftp_server = None

    async def initialize(self):
        """Initialize components with enhanced monitoring"""
        # Initialize TCP client with message bus
        tcp_config = ClientConfig(
            host=self.config.tcp_host,
            port=self.config.tcp_port,
            timeout=self.config.tcp_timeout,
            retry_attempts=self.config.tcp_retry_attempts
        )

        self.tcp_client = MessageBusEnabledTCPClient(tcp_config)
        await self.tcp_client.connect()

        # Initialize command transmitter
        self.command_transmitter = CommandTransmitter(
            self.tcp_client.message_bus)
        await self.command_transmitter.register_handler(UserCommand, UserCommandHandler())

        # Register command handlers
        from commands import (
            DeviceControlCommand, DeviceControlHandler,
            DataQueryCommand, DataQueryHandler,
            SystemConfigCommand, SystemConfigHandler,
            BulkOperationCommand, BulkOperationHandler
        )

        await self.command_transmitter.register_handler(
            DeviceControlCommand, DeviceControlHandler())
        await self.command_transmitter.register_handler(
            DataQueryCommand, DataQueryHandler())
        await self.command_transmitter.register_handler(
            SystemConfigCommand, SystemConfigHandler())
        await self.command_transmitter.register_handler(
            BulkOperationCommand, BulkOperationHandler())

        logger.info("Command handlers registered successfully")

        # Initialize connection manager
        self.manager = EnhancedConnectionManager(
            self.tcp_client.message_bus,
            self.command_transmitter
        )

        # Configure message transformer
        self.message_transformer.add_filter(
            MessageFilter(topic_pattern=r"websocket\.*")
        )
        self.message_transformer.add_transformer(json_payload_transformer)

        # Start scheduler
        asyncio.create_task(self._run_scheduler())
        await self.performance_monitor.start()
        logger.info(
            "Enhanced forwarder initialized with performance monitoring")

        # 注册配置更新处理
        self._config_subscriber = self._handle_config_update
        app.state.config_manager.register_observer(self._config_subscriber)

        # 启用TCP客户端高级功能
        await self.tcp_client.enable_compression()
        await self.tcp_client.start_heartbeat(interval=30.0)
        await self.tcp_client.enable_auto_reconnect()

        # 添加协议转换器
        self.protocol_converters = {
            'modbus': ModbusConverter(),
            'mqtt': MQTTConverter()
        }

        # 添加消息转换器
        self.message_transforms = [
            CompressionTransform(),
            MessageValidator(),
            MessageEnricher()
        ]

        # 初始化MQTT客户端
        if self.config.mqtt_enabled:
            from app.client.mqtt.client import MQTTConfig, MQTTClient
            mqtt_config = MQTTConfig(
                broker=self.config.mqtt_host,
                port=self.config.mqtt_port,
                client_id=f"cobalt-forward-{uuid.uuid4().hex[:8]}",
                username=self.config.mqtt_username,
                password=self.config.mqtt_password
            )
            self.mqtt_client = MQTTClient(mqtt_config)
            self.mqtt_client.connect()

            # 订阅配置的主题
            for topic in self.config.mqtt_topics:
                self.mqtt_client.subscribe(topic, self._handle_mqtt_message)

            logger.info("MQTT client initialized and connected")

        # 初始化WebSocket客户端（如果需要外部WebSocket连接）
        if self.config.websocket_enabled:
            from app.client.websocket.client import WebSocketConfig, WebSocketClient
            ws_config = WebSocketConfig(
                uri=f"{'wss' if self.config.websocket_ssl else 'ws'}://{self.config.websocket_host}:{self.config.websocket_port}",
                ssl_context=self._create_ssl_context() if self.config.websocket_ssl else None,
                auto_reconnect=True
            )
            self.websocket_client = WebSocketClient(ws_config)
            self.websocket_client.add_callback(
                'message', self._handle_ws_message)
            await self.websocket_client.connect()
            logger.info("WebSocket client initialized and connected")

        # 初始化SSH客户端
        if self.config.ssh_enabled:
            ssh_config = SSHConfig(
                hostname=self.config.ssh_host,
                port=self.config.ssh_port,
                username=self.config.ssh_username,
                password=self.config.ssh_password,
                private_key_path=self.config.ssh_key_path,
                known_hosts=self.config.ssh_known_hosts,
                pool=True,
                pool_size=self.config.ssh_pool_size
            )
            self.ssh_client = SSHClient(ssh_config)
            try:
                self.ssh_client.connect_with_retry()
                logger.info("SSH client initialized and connected")
            except Exception as e:
                logger.error(f"SSH connection failed: {e}")

        # 注册协议处理器
        self.protocol_handlers = {
            'mqtt': self._handle_mqtt_protocol,
            'ws': self._handle_ws_protocol,
            'tcp': self._handle_tcp_protocol
        }

        # 启动事件总线
        await self.event_bus.start()
        logger.info("Event bus started")

        # 加载插件
        await self.plugin_manager.load_plugins()
        logger.info("Plugins loaded")

        # 注册事件处理器
        self.event_bus.subscribe(
            "message.received", self._handle_message_event)
        self.event_bus.subscribe("client.connected", self._handle_client_event)
        self.event_bus.subscribe(
            "error.occurred", self._handle_error_event, EventPriority.HIGH)

        # 初始化插件系统
        app.state.plugin_manager = self.plugin_manager
        app.state.event_bus = self.event_bus

        # 启动插件配置监视
        await self.plugin_manager.start_plugin_watcher()

        # 注册插件事件处理器
        for plugin in self.plugin_manager.plugins.values():
            for event_name, handlers in plugin._event_handlers.items():
                for handler in handlers:
                    self.event_bus.subscribe(event_name, handler)

        # 初始化FTP服务器
        if self.config.ftp_enabled:
            ftp_config = FTPConfig(
                host=self.config.ftp_host,
                port=self.config.ftp_port,
                root_dir=self.config.ftp_root_dir
            )
            self.ftp_server = EnhancedFTPServer(ftp_config)

            # 添加默认用户
            if self.config.ftp_user and self.config.ftp_password:
                self.ftp_server.add_user(
                    self.config.ftp_user,
                    self.config.ftp_password,
                    self.config.ftp_root_dir
                )

            # 启动FTP服务器
            import threading
            threading.Thread(
                target=self.ftp_server.start_server, daemon=True).start()
            logger.info("FTP server initialized and started")

    async def _run_scheduler(self):
        while True:
            now = datetime.now()
            # Check and execute scheduled tasks
            for task_id, task in list(self.scheduled_tasks.items()):
                if task.done():
                    del self.scheduled_tasks[task_id]
            await asyncio.sleep(1)

    async def _handle_config_update(self, new_config: Any):
        """处理配置更新"""
        logger.info("正在应用新的配置...")
        try:
            # 更新TCP客户端配置
            if self.tcp_client:
                # 重新连接TCP客户端
                await self.tcp_client.disconnect()
                self.tcp_client = MessageBusEnabledTCPClient(ClientConfig(
                    host=new_config.tcp_host,
                    port=new_config.tcp_port,
                    timeout=new_config.tcp_timeout,
                    retry_attempts=new_config.tcp_retry_attempts
                ))
                await self.tcp_client.connect()

            # 更新其他组件配置
            self.performance_monitor.update_config(new_config)
            logger.success("配置更新完成")
        except Exception as e:
            logger.error(f"配置更新失败: {e}")

    async def shutdown(self):
        """Graceful shutdown with cleanup"""
        if self._config_subscriber:
            app.state.config_manager.unregister_observer(
                self._config_subscriber)
        await self.performance_monitor.stop()
        await self.cache.clear()
        if self.tcp_client:
            await self.tcp_client.disconnect()
        if self.mqtt_client:
            self.mqtt_client.disconnect()
        if self.websocket_client:
            await self.websocket_client.close_connection()
        if self.ssh_client:
            self.ssh_client.close()
        await self.event_bus.stop()
        await self.plugin_manager.shutdown_plugins()
        if self.ftp_server and self.ftp_server.server:
            self.ftp_server.server.close_all()
        logger.info("Enhanced forwarder shutdown completed")

    async def forward_message(self, message: Dict[str, Any], protocol: str = None):
        """增强的消息转发"""
        try:
            start_time = time.perf_counter()
            # 应用消息转换
            for transform in self.message_transforms:
                message = await transform.transform(message)

            # 协议转换与发送数据
            if protocol and protocol in self.protocol_converters:
                converter = self.protocol_converters[protocol]
                wire_data = await converter.to_wire_format(message)
            else:
                wire_data = json.dumps(message).encode()

            await self.tcp_client.send_with_retry(wire_data)

            # 记录性能指标
            latency = (time.perf_counter() - start_time) * 1000  # 转换为毫秒
            self.performance_monitor.record_message(latency)
            self.performance_monitor.record_forward()

        except Exception as e:
            self.performance_monitor.record_error()
            logger.error(f"Message forward error: {e}")
            raise

    async def _handle_mqtt_message(self, topic: str, payload: Any):
        """处理来自MQTT的消息"""
        try:
            message = {
                'protocol': 'mqtt',
                'topic': topic,
                'payload': payload,
                'timestamp': time.time()
            }
            await self.forward_message(message, protocol='mqtt')
        except Exception as e:
            logger.error(f"Error handling MQTT message: {e}")

    async def _handle_ws_message(self, message: Any):
        """处理来自WebSocket的消息"""
        try:
            if isinstance(message, str):
                message = json.loads(message)
            await self.forward_message(message, protocol='ws')
        except Exception as e:
            logger.error(f"Error handling WebSocket message: {e}")

    async def _handle_tcp_protocol(self, message: Dict[str, Any]) -> bytes:
        """处理TCP协议消息"""
        return json.dumps(message).encode()

    async def _handle_mqtt_protocol(self, message: Dict[str, Any]) -> bytes:
        """处理MQTT协议消息"""
        if self.mqtt_client and 'topic' in message:
            await self.mqtt_client.publish(message['topic'], message['payload'])
        return json.dumps(message).encode()

    async def _handle_ws_protocol(self, message: Dict[str, Any]) -> bytes:
        """处理WebSocket协议消息"""
        if self.websocket_client:
            await self.websocket_client.send_message(message)
        return json.dumps(message).encode()

    def _create_ssl_context(self) -> Optional[ssl.SSLContext]:
        """创建SSL上下文"""
        if not self.config.websocket_ssl:
            return None

        import ssl
        ssl_context = ssl.create_default_context()
        if self.config.websocket_cert and self.config.websocket_key:
            ssl_context.load_cert_chain(
                self.config.websocket_cert,
                self.config.websocket_key
            )
        return ssl_context

    async def _handle_message_event(self, event: Event):
        """处理消息事件"""
        try:
            message = event.data
            protocol = message.get('protocol', 'tcp')
            await self.forward_message(message, protocol)
        except Exception as e:
            logger.error(f"Error handling message event: {e}")
            await self.event_bus.publish(Event("error.occurred", str(e)))

    async def _handle_client_event(self, event: Event):
        """处理客户端连接事件"""
        client_info = event.data
        logger.info(f"New client connected: {client_info}")

    async def _handle_error_event(self, event: Event):
        """处理错误事件"""
        error_info = event.data
        logger.error(f"Error occurred: {error_info}")

    async def execute_ssh_command(self, command: str, stream: bool = False) -> Dict[str, Any]:
        """执行SSH命令"""
        if not self.ssh_client:
            raise ValueError("SSH client not initialized")

        try:
            if stream:
                return self.ssh_client.execute_command_stream(command)
            else:
                result = await self.ssh_client.execute_command_async(command)
                return {
                    "status": "success",
                    "result": result
                }
        except Exception as e:
            logger.error(f"SSH command execution error: {e}")
            return {
                "status": "error",
                "error": str(e)
            }

    async def sync_ssh_directory(self, local_dir: str, remote_dir: str, delete: bool = False) -> Dict[str, Any]:
        """同步目录到SSH服务器"""
        if not self.ssh_client:
            raise ValueError("SSH client not initialized")

        try:
            result = self.ssh_client.sync_directory(
                local_dir, remote_dir, delete)
            return {
                "status": "success",
                "result": result
            }
        except Exception as e:
            logger.error(f"SSH directory sync error: {e}")
            return {
                "status": "error",
                "error": str(e)
            }

# Update FastAPI application setup


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 初始化插件管理器
    app.state.plugin_manager = PluginManager("plugins", "config/plugins")
    # 初始化配置管理器
    app.state.config_manager = ConfigManager()

    config = EnhancedForwarderConfig(
        tcp_host="localhost",
        tcp_port=8080,
        websocket_host="0.0.0.0",
        websocket_port=8000
    )
    app.forwarder = EnhancedTCPWebSocketForwarder(config)

    # 加载插件
    await app.state.plugin_manager.load_plugins()
    # 启动插件监控
    await app.state.plugin_manager.start_health_monitor()
    # 启动插件文件监视
    await app.state.plugin_manager.start_plugin_watcher()

    await app.forwarder.initialize()
    yield
    # 关闭插件系统
    app.state.plugin_manager.stop_health_monitor()
    app.state.plugin_manager.stop_plugin_watcher()
    await app.state.plugin_manager.shutdown_plugins()
    await app.forwarder.shutdown()

app = FastAPI(lifespan=lifespan)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 在app定义后添加路由注册

app.include_router(websocket.router)
app.include_router(commands.router)
app.include_router(system.router)
app.include_router(plugin.router)

# 移除所有原有的路由处理函数
# 保留以下基础路由


@app.get("/health")
async def health_check():
    """Server health check"""
    metrics = app.forwarder.performance_monitor.get_current_metrics()
    return {
        "status": "healthy",
        "uptime": time.time() - app.state.start_time,
        "metrics": metrics.__dict__
    }


@app.get("/metrics")
async def get_metrics():
    """获取性能指标"""
    try:
        metrics = app.forwarder.performance_monitor.get_current_metrics()
        return JSONResponse(content=metrics.__dict__)
    except Exception as e:
        logger.error(f"Error getting metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/ssh/execute")
async def execute_ssh_command(
    command: str = Body(...),
    stream: bool = Query(False)
):
    """执行SSH命令"""
    try:
        result = await app.forwarder.execute_ssh_command(command, stream)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/ssh/sync")
async def sync_ssh_directory(
    local_dir: str = Body(...),
    remote_dir: str = Body(...),
    delete: bool = Body(False)
):
    """同步目录到SSH服务器"""
    try:
        result = await app.forwarder.sync_ssh_directory(local_dir, remote_dir, delete)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# 添加FTP相关路由


@app.post("/ftp/upload")
async def upload_file(
    local_path: str = Body(...),
    remote_path: str = Body(...),
    secure: bool = Body(False)
):
    """上传文件到FTP服务器"""
    try:
        if secure:
            result = app.forwarder.ftp_server.secure_upload(
                local_path, remote_path)
        else:
            with open(local_path, 'rb') as f:
                result = app.forwarder.ftp_server.upload_file(
                    local_path, remote_path)
        return {"status": "success", "result": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/ftp/download")
async def download_file(
    remote_path: str = Body(...),
    local_path: str = Body(...),
    verify: bool = Body(False)
):
    """从FTP服务器下载文件"""
    try:
        if verify:
            result = app.forwarder.ftp_server.download_with_verification(
                remote_path, local_path)
        else:
            result = app.forwarder.ftp_server.download_file(
                remote_path, local_path)
        return {"status": "success", "result": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/ftp/sync")
async def sync_directory(
    local_dir: str = Body(...),
    remote_dir: str = Body(...),
    delete_extra: bool = Body(False)
):
    """同步目录到FTP服务器"""
    try:
        app.forwarder.ftp_server.synchronize_directories(
            local_dir, remote_dir, delete_extra)
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/ftp/batch")
async def batch_process(
    action: str = Body(...),
    file_list: List[str] = Body(...),
    target_dir: str = Body(...)
):
    """批量处理FTP操作"""
    try:
        results = app.forwarder.ftp_server.batch_process(
            action,
            file_list,
            remote_dir=target_dir if action in ["upload", "sync"] else None,
            local_dir=target_dir if action == "download" else None
        )
        return {"status": "success", "results": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/ftp/resume/{transfer_id}")
async def resume_transfer(transfer_id: str):
    """恢复中断的传输"""
    try:
        app.forwarder.ftp_server.resume_upload(transfer_id)
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=(str(e)))
