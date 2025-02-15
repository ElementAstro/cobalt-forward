from dataclasses import asdict
import time
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware
import asyncio
from typing import Any, Dict, Set, Optional, List
import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime
import uuid
from app.commands import BulkOperationCommand, DataQueryCommand, DeviceControlCommand, SystemConfigCommand
from app.message_transformer import CompressionTransform, MessageEnricher, MessageValidator
from app.protocol_converter import MQTTConverter, ModbusConverter
from logger_config import setup_logging
from performance_monitor import PerformanceMonitor
from fastapi.responses import JSONResponse
from aiocache import Cache
from config_manager import ConfigManager

# Import required components
from tcp_client import ClientConfig
from message_bus import MessageBusEnabledTCPClient, MessageBus, Message, MessageType
from command_dispatcher import CommandTransmitter, UserCommand, UserCommandHandler, ScheduledCommand, BatchCommand
from message_processor import MessageTransformer, MessageFilter, json_payload_transformer

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
        logger.info("Enhanced forwarder shutdown completed")

    async def forward_message(self, message: Dict[str, Any], protocol: str = None):
        """增强的消息转发"""
        try:
            # 应用消息转换
            for transform in self.message_transforms:
                message = await transform.transform(message)

            # 协议转换
            if protocol and protocol in self.protocol_converters:
                converter = self.protocol_converters[protocol]
                wire_data = await converter.to_wire_format(message)
            else:
                wire_data = json.dumps(message).encode()

            # 发送数据
            await self.tcp_client.send_with_retry(wire_data)
            
            # 更新性能指标
            self.performance_monitor.record_forward()

        except Exception as e:
            logger.error(f"Message forward error: {e}")
            raise

# Update FastAPI application setup


@asynccontextmanager
async def lifespan(app: FastAPI):
    config = EnhancedForwarderConfig(
        tcp_host="localhost",
        tcp_port=8080,
        websocket_host="0.0.0.0",
        websocket_port=8000
    )
    app.forwarder = EnhancedTCPWebSocketForwarder(config)
    await app.forwarder.initialize()
    yield
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


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """Enhanced WebSocket endpoint with performance monitoring"""
    start_time = time.time()
    await app.forwarder.manager.connect(websocket)
    try:
        while True:
            message = await websocket.receive_text()
            process_start = time.time()
            await app.forwarder.manager.handle_client_message(websocket, message)

            # 记录性能指标
            latency = (time.time() - process_start) * 1000  # 转换为毫秒
            app.forwarder.performance_monitor.record_message(latency)

    except WebSocketDisconnect:
        end_time = time.time()
        logger.info(
            f"WebSocket connection duration: {end_time - start_time:.2f}s")
        await app.forwarder.manager.disconnect(websocket)
    except Exception as e:
        logger.exception(f"WebSocket error: {e}")
        raise


@app.post("/schedule_command")
async def schedule_command(command_data: dict):
    try:
        command = ScheduledCommand(command_data)
        task = asyncio.create_task(
            app.forwarder.command_transmitter.send(command))
        task_id = str(uuid.uuid4())
        app.forwarder.scheduled_tasks[task_id] = task
        return {"task_id": task_id}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/batch_command")
async def batch_command(commands: List[dict]):
    try:
        command = BatchCommand(commands)
        result = await app.forwarder.command_transmitter.send(command)
        return result.result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# 添加新的API端点


@app.get("/metrics")
async def get_metrics():
    """获取性能指标"""
    try:
        metrics = app.forwarder.performance_monitor.get_current_metrics()
        return JSONResponse(content=metrics.__dict__)
    except Exception as e:
        logger.error(f"Error getting metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/cache_stats")
async def get_cache_stats():
    """获取缓存统计信息"""
    try:
        stats = await app.forwarder.cache.raw.stats()
        return JSONResponse(content=stats)
    except Exception as e:
        logger.error(f"Error getting cache stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Add new routes for configuration management


@app.post("/api/config")
async def update_config(config_updates: dict = Body(...)):
    """Update server configuration"""
    try:
        await app.state.config_manager.update_config(config_updates)
        return {"status": "success", "message": "Configuration updated"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/config")
async def get_config():
    """Get current server configuration"""
    return JSONResponse(content=asdict(app.state.config_manager.runtime_config))


@app.post("/api/reload")
async def reload_server():
    """Trigger server reload"""
    try:
        await app.state.forwarder.reload()
        return {"status": "success", "message": "Server reloaded"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Add health check endpoint


@app.get("/health")
async def health_check():
    """Server health check"""
    metrics = app.forwarder.performance_monitor.get_current_metrics()
    return {
        "status": "healthy",
        "uptime": time.time() - app.state.start_time,
        "metrics": metrics.__dict__
    }

# Add debugging endpoints


@app.get("/api/debug/connections")
async def get_connections():
    """Get current connection information"""
    return {
        "active_websockets": len(app.forwarder.manager.active_connections),
        "tcp_state": app.forwarder.tcp_client.state.value
    }


@app.post("/api/debug/simulate_error")
async def simulate_error():
    """Endpoint for testing error handling"""
    if not app.state.config_manager.runtime_config.development_mode:
        raise HTTPException(
            status_code=403, detail="Only available in development mode")
    # Simulate an error condition
    raise Exception("Simulated error for testing")

# 添加新的API端点用于命令处理


@app.post("/api/device/control")
async def device_control(command_data: Dict[str, Any]):
    """设备控制接口"""
    try:
        command = DeviceControlCommand(command_data)
        result = await app.forwarder.command_transmitter.send(command)
        return result.result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/data/query")
async def data_query(query_data: Dict[str, Any]):
    """数据查询接口"""
    try:
        command = DataQueryCommand(query_data)
        result = await app.forwarder.command_transmitter.send(command)
        return result.result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/system/config")
async def system_config(config_data: Dict[str, Any]):
    """系统配置接口"""
    try:
        command = SystemConfigCommand(config_data)
        result = await app.forwarder.command_transmitter.send(command)
        return result.result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/bulk/operation")
async def bulk_operation(operation_data: Dict[str, Any]):
    """批量操作接口"""
    try:
        command = BulkOperationCommand(operation_data)
        result = await app.forwarder.command_transmitter.send(command)
        return result.result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
