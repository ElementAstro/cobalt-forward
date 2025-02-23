from fastapi import APIRouter, HTTPException
from typing import Dict, Any
from app.core.upload_manager import UploadManager
from app.core.message_bus import MessageBus
from app.core.event_bus import EventBus

router = APIRouter(prefix="/api/core", tags=["core"])

@router.post("/upload/create")
async def create_upload(filename: str, total_size: int):
    """创建文件上传任务"""
    app = router.app
    upload_id = await app.upload_manager.create_upload(filename, total_size)
    return {"upload_id": upload_id}

@router.post("/message/publish")
async def publish_message(topic: str, data: Dict[str, Any]):
    """发布消息到消息总线"""
    app = router.app
    await app.message_bus.publish(topic, data)
    return {"status": "published"}

@router.get("/stats")
async def get_stats():
    """获取核心组件状态"""
    app = router.app
    return {
        "message_bus": app.message_bus.metrics,
        "command_stats": app.command_dispatcher.metrics,
        "upload_manager": app.upload_manager.stats
    }
