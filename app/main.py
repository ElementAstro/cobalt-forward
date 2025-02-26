import typer
import uvicorn
from pathlib import Path
import yaml
from typing import Optional
import os
from dotenv import load_dotenv
from loguru import logger
from dataclasses import dataclass
from fastapi import FastAPI
from app.core.ssh_forwarder import SSHForwarder
import time

from app.config.config_manager import ConfigManager
from server import app, EnhancedForwarderConfig
from logger_config import setup_logging
from app.routers import router
from app.core.middleware import ErrorHandlerMiddleware, PerformanceMiddleware, SecurityMiddleware

import asyncio
import sys
import logging
from typing import Dict, Any, List, Optional
import argparse
import signal
from pathlib import Path

# 确保可以导入app包
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.server import app, lifespan
from app.core.shared_services import SharedServices, ServiceType
from app.core.integration_manager import IntegrationManager, ComponentType
from app.core.event_bus import EventBus, EventPriority
from app.core.message_bus import MessageBus
from app.core.command_dispatcher import CommandDispatcher, CommandTransmitter
from app.plugin.plugin_manager import PluginManager
from app.utils.logger import setup_logging
from app.config.config_manager import ConfigManager

# 初始化日志
logger = setup_logging()

# 创建命令行应用
cli = typer.Typer()


@dataclass
class AppConfig:
    """应用配置类"""
    tcp_host: str
    tcp_port: int
    websocket_host: str
    websocket_port: int
    log_path: str
    tcp_timeout: float
    tcp_retry_attempts: int
    enable_metrics: bool
    enable_cache: bool
    development_mode: bool


def load_config(config_path: Optional[str] = None) -> AppConfig:
    """加载配置，优先级：命令行 > 环境变量 > 配置文件 > 默认值"""
    # 默认配置
    config = {
        "tcp_host": "localhost",
        "tcp_port": 8080,
        "websocket_host": "0.0.0.0",
        "websocket_port": 8000,
        "log_path": "logs",
        "tcp_timeout": 30.0,
        "tcp_retry_attempts": 3,
        "enable_metrics": True,
        "enable_cache": True,
        "development_mode": False
    }

    # 加载配置文件
    if config_path and Path(config_path).exists():
        with open(config_path, 'r') as f:
            file_config = yaml.safe_load(f)
            if file_config:
                config.update(file_config)

    # 加载环境变量
    load_dotenv()
    env_mapping = {
        "COBALT_TCP_HOST": "tcp_host",
        "COBALT_TCP_PORT": "tcp_port",
        "COBALT_WS_HOST": "websocket_host",
        "COBALT_WS_PORT": "websocket_port",
        "COBALT_LOG_PATH": "log_path",
        "COBALT_TCP_TIMEOUT": "tcp_timeout",
        "COBALT_TCP_RETRY": "tcp_retry_attempts",
        "COBALT_ENABLE_METRICS": "enable_metrics",
        "COBALT_ENABLE_CACHE": "enable_cache",
        "COBALT_DEV_MODE": "development_mode"
    }

    for env_var, config_key in env_mapping.items():
        if os.getenv(env_var):
            value = os.getenv(env_var)
            # 类型转换
            if isinstance(config[config_key], bool):
                value = value.lower() in ('true', '1', 'yes')
            elif isinstance(config[config_key], int):
                value = int(value)
            elif isinstance(config[config_key], float):
                value = float(value)
            config[config_key] = value

    return AppConfig(**config)


def create_app(config_path: str) -> FastAPI:
    """创建FastAPI应用并配置热重载"""
    config = load_config(config_path)
    app = FastAPI(title="Cobalt Forward API")

    # 添加中间件
    app.add_middleware(ErrorHandlerMiddleware)
    app.add_middleware(PerformanceMiddleware)
    app.add_middleware(SecurityMiddleware)

    # 包含路由
    app.include_router(router)

    # 初始化配置管理器
    config_manager = ConfigManager(config_path)
    app.state.config_manager = config_manager

    if config.development_mode:
        # 在开发模式下启用热重载
        config_manager.start_hot_reload()
        logger.info("已启用配置热重载支持")

    # 初始化SSH转发器
    app.ssh_forwarder = SSHForwarder()

    @app.on_event("startup")
    async def startup_event():
        # 初始化各个组件
        from app.routers.core import (
            ssh_forwarder,
            upload_manager,
            message_bus,
            event_bus
        )
        await message_bus.start()
        await event_bus.start()
        await upload_manager.start()

    @app.on_event("shutdown")
    async def shutdown_event():
        # 关闭组件
        from app.routers.core import (
            ssh_forwarder,
            upload_manager,
            message_bus,
            event_bus
        )
        await message_bus.stop()
        await event_bus.stop()
        await upload_manager.stop()
        config_manager.stop_hot_reload()
        logger.info("应用关闭，停止配置热重载")

    return app


@cli.command()
def start(
    config_file: str = typer.Option(None, "--config", "-c", help="配置文件路径"),
    tcp_host: str = typer.Option(None, "--tcp-host", help="TCP服务器主机"),
    tcp_port: int = typer.Option(None, "--tcp-port", help="TCP服务器端口"),
    ws_host: str = typer.Option(None, "--ws-host", help="WebSocket服务器主机"),
    ws_port: int = typer.Option(None, "--ws-port", help="WebSocket服务器端口"),
    log_path: str = typer.Option(None, "--log-path", help="日志路径"),
    dev_mode: bool = typer.Option(False, "--dev", help="开发模式")
):
    """启动Cobalt Forward服务"""
    # 加载配置
    config = load_config(config_file)

    # 命令行参数覆盖其他配置
    if tcp_host:
        config.tcp_host = tcp_host
    if tcp_port:
        config.tcp_port = tcp_port
    if ws_host:
        config.websocket_host = ws_host
    if ws_port:
        config.websocket_port = ws_port
    if log_path:
        config.log_path = log_path
    if dev_mode:
        config.development_mode = dev_mode

    # 设置日志
    logger = setup_logging(config.log_path)
    logger.info(f"Starting Cobalt Forward with config: {config}")

    # 更新应用配置
    app.state.config = config
    app.state.forwarder_config = EnhancedForwarderConfig(
        tcp_host=config.tcp_host,
        tcp_port=config.tcp_port,
        websocket_host=config.websocket_host,
        websocket_port=config.websocket_port,
        tcp_timeout=config.tcp_timeout,
        tcp_retry_attempts=config.tcp_retry_attempts
    )

    # 启动服务器
    if config.development_mode:
        uvicorn.run(
            "server:app",
            host=config.websocket_host,
            port=config.websocket_port,
            reload=True,
            log_level="debug"
        )
    else:
        uvicorn.run(
            app,
            host=config.websocket_host,
            port=config.websocket_port,
            log_level="info"
        )


@cli.command()
def dev(
    config_file: str = typer.Option("config.yaml", "--config", "-c"),
    reload: bool = typer.Option(True, "--reload/--no-reload"),
    port: int = typer.Option(8000, "--port", "-p")
):
    """以开发模式运行服务器"""
    config = load_config(config_file)
    config.development_mode = True

    # 配置 uvicorn 的重载选项
    uvicorn_config = {
        "app": "main:create_app('{}')".format(config_file),
        "host": config.websocket_host,
        "port": port,
        "reload": reload,
        "reload_dirs": ["app"],
        "reload_delay": 0.25,
        "log_level": "debug"
    }

    logger.info(f"正在以开发模式启动服务器，端口: {port}")
    if reload:
        logger.info("代码热重载已启用")

    uvicorn.run(**uvicorn_config)


@cli.command()
def init_config(output: str = typer.Option("config.yaml", "--output", "-o")):
    """生成默认配置文件"""
    default_config = {
        "tcp_host": "localhost",
        "tcp_port": 8080,
        "websocket_host": "0.0.0.0",
        "websocket_port": 8000,
        "log_path": "logs",
        "tcp_timeout": 30.0,
        "tcp_retry_attempts": 3,
        "enable_metrics": True,
        "enable_cache": True,
        "development_mode": False
    }

    with open(output, 'w') as f:
        yaml.dump(default_config, f, default_flow_style=False)
    print(f"Default configuration has been written to {output}")


class ApplicationContext:
    """应用上下文，管理所有核心组件"""
    
    def __init__(self, config_path: str = "config.yaml"):
        self.config_path = config_path
        self.integration_manager: Optional[IntegrationManager] = None
        self.shared_services: Optional[SharedServices] = None
        self.event_bus: Optional[EventBus] = None
        self.message_bus: Optional[MessageBus] = None
        self.command_dispatcher: Optional[CommandDispatcher] = None
        self.plugin_manager: Optional[PluginManager] = None
        self.config_manager: Optional[ConfigManager] = None
        self.running = False
        self._shutdown_event = asyncio.Event()

    async def initialize(self):
        """初始化应用上下文和所有核心组件"""
        logger.info("初始化应用上下文...")
        
        # 创建共享服务管理器
        self.shared_services = SharedServices()
        
        # 创建集成管理器
        self.integration_manager = IntegrationManager(config_path=self.config_path)
        
        # 创建并注册事件总线
        self.event_bus = EventBus()
        await self.integration_manager.register_component(
            "event_bus", 
            self.event_bus, 
            ComponentType.EVENT_BUS
        )
        
        # 创建并注册命令分发器
        self.command_dispatcher = CommandDispatcher()
        await self.integration_manager.register_component(
            "command_dispatcher", 
            self.command_dispatcher, 
            ComponentType.CUSTOM
        )
        
        # 创建并注册插件管理器
        self.plugin_manager = PluginManager()
        await self.integration_manager.register_component(
            "plugin_manager", 
            self.plugin_manager, 
            ComponentType.CUSTOM
        )
        
        # 创建并注册配置管理器
        self.config_manager = ConfigManager(self.config_path)
        await self.integration_manager.register_component(
            "config_manager", 
            self.config_manager, 
            ComponentType.CONFIG_MANAGER
        )
        
        # 注册组件到共享服务
        await self.shared_services.register_service(
            ServiceType.EVENT_BUS, "default", self.event_bus, make_default=True)
        await self.shared_services.register_service(
            ServiceType.COMMAND_DISPATCHER, "default", self.command_dispatcher, make_default=True)
        await self.shared_services.register_service(
            ServiceType.PLUGIN_MANAGER, "default", self.plugin_manager, make_default=True)
        
        # 组件之间的相互集成
        await self.event_bus.register_plugin_handlers(self.plugin_manager)
        await self.plugin_manager.set_event_bus(self.event_bus)
        await self.plugin_manager.set_command_dispatcher(self.command_dispatcher)
        await self.command_dispatcher.set_event_bus(self.event_bus)
        
        # 完成初始化
        await self.integration_manager.start()
        self.running = True
        logger.info("应用上下文初始化完成")

    async def shutdown(self):
        """优雅关闭所有组件"""
        if not self.running:
            return
            
        logger.info("关闭应用上下文...")
        self.running = False
        
        # 发布系统关闭事件
        if self.event_bus:
            await self.event_bus.publish(
                "system.shutdown", 
                {"timestamp": asyncio.get_event_loop().time()}
            )
        
        # 关闭插件管理器
        if self.plugin_manager:
            await self.plugin_manager.shutdown_plugins()
        
        # 停止所有组件
        if self.integration_manager:
            await self.integration_manager.stop()
        
        # 设置关闭事件，通知等待的任务
        self._shutdown_event.set()
        logger.info("应用上下文已关闭")
    
    async def wait_for_shutdown(self):
        """等待应用关闭"""
        await self._shutdown_event.wait()


# 全局应用上下文
app_context = None


async def start_application(config_path: str = "config.yaml"):
    """启动应用"""
    global app_context
    app_context = ApplicationContext(config_path)
    await app_context.initialize()
    
    # 注册信号处理器用于优雅关闭
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop = asyncio.get_event_loop()
        loop.add_signal_handler(sig, lambda: asyncio.create_task(shutdown_application()))
    
    # 将应用上下文绑定到FastAPI应用
    app.state.context = app_context


async def shutdown_application():
    """关闭应用"""
    global app_context
    if app_context:
        await app_context.shutdown()


def run_server(host: str = "0.0.0.0", port: int = 8000, 
               config_path: str = "config.yaml", reload: bool = False):
    """运行服务器"""
    # 确保配置文件存在
    if not os.path.isfile(config_path):
        logger.warning(f"配置文件 {config_path} 不存在，将使用默认配置")
    
    # 启动FastAPI应用
    logger.info(f"启动服务器: {host}:{port}")
    uvicorn.run(
        "app.server:app",
        host=host,
        port=port,
        reload=reload,
        log_level="info"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Cobalt Forward 服务器")
    parser.add_argument("--host", default="0.0.0.0", help="监听主机地址")
    parser.add_argument("--port", type=int, default=8000, help="监听端口")
    parser.add_argument("--config", default="config.yaml", help="配置文件路径")
    parser.add_argument("--reload", action="store_true", help="启用热重载")
    
    args = parser.parse_args()
    run_server(args.host, args.port, args.config, args.reload)


if __name__ == "__main__":
    cli()
