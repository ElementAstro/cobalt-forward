import typer
import uvicorn
from pathlib import Path
import yaml
from typing import Optional
import os
from dotenv import load_dotenv
from loguru import logger
from dataclasses import dataclass

from server import app, EnhancedForwarderConfig
from logger_config import setup_logging

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


if __name__ == "__main__":
    cli()
