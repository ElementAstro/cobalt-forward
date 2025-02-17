from .websocket import router as websocket_router
from .commands import router as commands_router
from .system import router as system_router

__all__ = ['websocket_router', 'commands_router', 'system_router']
