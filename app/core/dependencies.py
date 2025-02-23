from fastapi import Request
from app.config.config_manager import ConfigManager

async def get_config_manager(request: Request) -> ConfigManager:
    """获取配置管理器实例"""
    return request.app.state.config_manager
