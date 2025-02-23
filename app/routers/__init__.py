from fastapi import APIRouter
from .websocket import router as websocket_router
from .commands import router as commands_router
from .system import router as system_router
from .core import router as core_router

router = APIRouter()
router.include_router(core_router)

__all__ = ['websocket_router', 'commands_router', 'system_router']
